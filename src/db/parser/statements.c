#include "parser/parser.h"
#include "storage/database.h"

JQLCommand parser_parse(Database* db) {
  JQLCommand command = {0};
  memset(&command, 0, sizeof(JQLCommand));

  while (db->parser->cur->type == TOK_SC) {
    parser_consume(db->parser);
  }

  switch (db->parser->cur->type) {
    case TOK_CRT:
      command = parser_parse_create_table(db->parser, db);
      break;
    case TOK_ALT:
      command = parser_parse_alter_table(db->parser, db);
      break;
    case TOK_INS:
      command = parser_parse_insert(db->parser, db);
      break;
    case TOK_SEL: 
      command = parser_parse_select(db->parser, db);
      break;
    case TOK_UPD:
      command = parser_parse_update(db->parser, db);
      break;
    case TOK_DEL:
      command = parser_parse_delete(db->parser, db);
      break;
    case TOK_EOF:
      return command;
    default:
      REPORT_ERROR(db->parser->lexer, "SYE_UNSUPPORTED");
      return command;
  }

  if (command.is_invalid) {
    while (db->parser->cur->type != TOK_SC) {      
      parser_consume(db->parser);
    }
  } 
  
  if (db->parser->cur->type != TOK_SC && db->parser->cur->type != TOK_EOF && !command.is_invalid) {
    REPORT_ERROR(db->parser->lexer, "SYE_UE_SEMICOLON");
    command.is_invalid = true;
  }

  parser_consume(db->parser);
  return command;
}


JQLCommand parser_parse_create_table(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_CREATE;
  command.is_invalid = true;

  command.schema = calloc(1, sizeof(TableSchema));

  parser_consume(parser);

  if (parser->cur->type == TOK_NO_CONSTRAINTS) {
    command.is_unsafe = true;
    parser_consume(parser);
  }

  if (parser->cur->type != TOK_TBL) {
    REPORT_ERROR(parser->lexer, "SYE_E_TAFCR");
    return command;
  }

  parser_consume(parser);

  bool if_not_exists = false;
  if (parser->cur->type == TOK_IF) {
    parser_consume(parser);

    if (parser->cur->type != TOK_NOT) {
      REPORT_ERROR(parser->lexer, "SYE_NOT_AFIF_CT");
      return command;
    }
    parser_consume(parser);

    if (parser->cur->type != TOK_EXISTS) {
      REPORT_ERROR(parser->lexer, "SYE_NOT_AFIF_CT");
      return command;
    }
    parser_consume(parser);

    if_not_exists = true;
  }

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_TNAFTA");
    return command;
  }

  strcpy(command.schema->table_name, parser->cur->value);
  parser_consume(parser);

  int idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (!is_struct_zeroed(&db->tc[idx], sizeof(TableCatalogEntry))) {
    if (!if_not_exists) {
      LOG_ERROR("Table `%s` already exists", command.schema->table_name);
    }
    return command;
  }

  if (parser->cur->type != TOK_LP) {
    REPORT_ERROR(parser->lexer, "SYE_E_PRNAFDYNA");
    return command;
  }

  parser_consume(parser);

  command.schema->columns = calloc(MAX_COLUMNS, sizeof(ColumnDefinition));
  command.schema->column_count = 0;

  while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
    if (!parser_parse_column_definition(parser, &command)) {
      return command;
    }

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else if (parser->cur->type == TOK_RP) {
      break;
    } else {
      REPORT_ERROR(parser->lexer, "SYE_E_CPRORCOM");
      return command;
    }
  }

  if (parser->cur->type != TOK_RP) {
    REPORT_ERROR(parser->lexer, "SYE_E_CPR");
    return command;
  }

  parser_consume(parser);
  
  command.is_invalid = false;
  return command;
}


JQLCommand parser_parse_insert(Parser *parser, Database* db) {
  JQLCommand command;  
  
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_INSERT;
  command.is_invalid = true;

  parser_consume(parser); 

  if (parser->cur->type == TOK_NO_CONSTRAINTS) {
    command.is_unsafe = true;
    parser_consume(parser);
  }

  if (parser->cur->type != TOK_INTO) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_INTO");
    return command;
  }

  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  command.col_count = 0;
 
  command.schema = get_table_schema(db, parser->cur->value);

  if (!command.schema) {
    LOG_ERROR("Expected a valid table, %s doesnt exist", parser->cur->value);
    return command;
  }

  // LOG_DEBUG("schema: %s, cc: %d", command.schema->table_name, command.schema->column_count);

  parser_consume(parser); 
  command.columns = calloc(MAX_COLUMNS, sizeof(char *));

  if (parser->cur->type == TOK_LP) { 
    parser_consume(parser);  

    while (parser->cur->type == TOK_ID) {
      command.columns[command.col_count] = strdup(parser->cur->value);
      command.col_count++;


      parser_consume(parser);

      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else if (parser->cur->type == TOK_RP) {
        break;
      } else {
        REPORT_ERROR(parser->lexer, "SYE_E_INVALID_COLUMN_LIST");
        return command;
      }
    }

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
      return command;
    }

    // command.specified_order = true;

    parser_consume(parser);
  } else {
    for (int i = 0; i < command.schema->column_count; i++) {
      command.columns[i] = strdup(command.schema->columns[i].name);
    }

    command.col_count = command.schema->column_count;
  }

  if (parser->cur->type != TOK_VAL) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_VALUES");
    return command;
  }

  parser_consume(parser);

  command.values = calloc(MAX_OPERATIONS, sizeof(ExprNode*));
  command.row_count = 0;

  while (parser->cur->type == TOK_LP) {
    ExprNode** row = calloc(command.schema->column_count, sizeof(ExprNode*));

    parser_consume(parser); 

    uint8_t value_count = 0; 

    while (value_count < command.col_count) {
      int row_idx = find_column_index(command.schema, command.columns[value_count]);

      if (row_idx < 0) {
        LOG_DEBUG("Internal: Row index in table '%s' for '%s' was evaluated incorrectly",
            command.schema->table_name, command.columns[value_count]);
        return command;
      }
      
      row[row_idx] = parser_parse_expression(parser, command.schema);
      if (!row[row_idx]) return command;
      value_count++;

      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else if (parser->cur->type != TOK_RP) {
        REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUES", 
          parser->cur->value, parser->cur->type);
        return command;
      }

      if (parser->cur->type == TOK_EOF) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP_VALUES");
        return command;  
      }

    }

    if (parser->cur->type != TOK_RP) {
      LOG_ERROR("Mismatch in number of expected attributes %d and actual attributes %d, ended on token %s expected closing parentheses",
        command.col_count, value_count - 1, parser->cur->value);
      return command;
    }

    parser_consume(parser);   

    command.value_counts[command.row_count] = value_count;
    command.values[command.row_count] = row;
    command.row_count++;

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);  
    } else {
      break;
    }
    
    if (command.row_count >= MAX_OPERATIONS) {
      LOG_ERROR("Too many rows in INSERT");
      return command;
    }
  }

  if (parser->cur->type != TOK_RETURNING) {
    command.is_invalid = false;
    return command;
  }

  if (command.col_count <= 0 || command.col_count > MAX_COLUMNS) {
    LOG_ERROR("Invalid number of columns expected %d <= 0 and > %d", command.col_count, MAX_COLUMNS);
    return command;
  }

  parser_consume(parser); 
  command.returning_columns = calloc(command.col_count, sizeof(char *));
  if (!command.returning_columns) {
    REPORT_ERROR(parser->lexer, "Memory allocation failed");
    return command;
  }

  while (parser->cur->type == TOK_ID) {
    if (command.ret_col_count > command.col_count) {
      REPORT_ERROR(parser->lexer, "Too many RETURNING columns");
      return command;
    }

    command.returning_columns[command.ret_col_count] = strdup(parser->cur->value);
    command.ret_col_count += 1;
    parser_consume(parser);

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else if (parser->cur->type == TOK_SC || parser->cur->type == TOK_EOF) {
      break; 
    } else {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_RET_COLUMN_LIST");
      return command;
    }
  }


  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_select(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_SELECT;
  command.is_invalid = true;
  
  parser_consume(parser);
  
  ParserState state = parser_save_state(parser);
  
  while (parser->cur->type != TOK_FRM) {
    parser_consume(parser);
    if (parser->cur->type == TOK_EOF || parser->cur->type == TOK_SC) {
      return command;
    }
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }
  
  command.schema = calloc(1, sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);

  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);
  
  if (is_struct_zeroed(&db->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Couldn't fetch rows from table '%s', doesn't exist", command.schema->table_name);
    return command;
  }
    
  parser_restore_state(parser, state);
  
  uint8_t value_count = 0;
  
  if (parser->cur->type == TOK_MUL) {   
    value_count = db->tc[idx].schema->column_count;

    command.sel_columns = calloc(MAX_COLUMNS, sizeof(SelectColumn));  // 🛠️ fix added

    for (int j = 0; j < value_count; j++) {
      ExprNode* id_expr = malloc(sizeof(ExprNode));
      id_expr->type = EXPR_COLUMN;
      id_expr->column.index = j;
      
      command.sel_columns[j].expr = id_expr;
    }
    parser_consume(parser);
  } else {
    int column_count = 0;
    command.sel_columns = calloc(MAX_COLUMNS, sizeof(SelectColumn));
    
    while (true) {
      ExprNode* expr = parser_parse_expression(parser, db->tc[idx].schema);
      
      if (expr == NULL) {
        REPORT_ERROR(parser->lexer, "E_INVALID_COLUMN_EXPR");
        return command;
      }
      
      char* alias = NULL;
      if (parser->cur->type == TOK_AS) {
        parser_consume(parser);
        if (parser->cur->type == TOK_ID) {
          alias = strdup(parser->cur->value);
          parser_consume(parser);
        } else {
          REPORT_ERROR(parser->lexer, "E_IDEN_AF_ALIAS_KW");
          free_expr_node(expr);
          return command;
        }
      }
      
      command.sel_columns[column_count].expr = expr;
      command.sel_columns[column_count].alias = alias;
      command.sel_columns[column_count].type = -1;

      if (expr->type == EXPR_FUNCTION) {
        command.sel_columns[column_count].type = expr->fn.type;
      }
      
      column_count++;
      
      if (parser->cur->type != TOK_COM) break;
      parser_consume(parser);
    }
    value_count = column_count;
  }

  
  if (parser->cur->type != TOK_FRM) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_FROM");
    return command;
  }
  parser_consume(parser); 
  
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }
  parser_consume(parser); 
  
  command.value_counts[0] = value_count;

  parse_where_clause(parser, db, &command, idx);
  parse_order_by_clause(parser, db, &command, idx);
  parse_limit_clause(parser, &command);
  parse_offset_clause(parser, &command);

  command.is_invalid = false;
  return command;
}


JQLCommand parser_parse_update(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_UPDATE;
  command.is_invalid = true;

  parser_consume(parser); 

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  uint32_t idx = hash_fnv1a(parser->cur->value, MAX_TABLES);
  command.schema = db->tc[idx].schema;
  
  if (is_struct_zeroed(command.schema, sizeof(TableSchema))) {
    return command;
  }

  parser_consume(parser); 

  if (parser->cur->type != TOK_SET) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_SET");
    return command;
  }

  parser_consume(parser); 

  uint8_t value_count = 0;
  command.values = calloc(1, sizeof(ExprNode*));
  command.row_count = 1;

  command.update_columns = calloc(command.schema->column_count, sizeof(UpdateColumn));
  command.values[0] = calloc(command.schema->column_count, sizeof(ExprNode));

  uint8_t null_bitmap_size = (command.schema->column_count + 7) / 8;
  command.bitmap = (uint8_t*)malloc(null_bitmap_size);
  if (!command.bitmap) {
    return command;
  }
  memset(command.bitmap, 0, null_bitmap_size);

  ExprNode* expr = NULL;
  while (parser->cur->type == TOK_ID) {
    expr = parser_parse_primary(parser, command.schema);
    if (!expr || !(expr->type == EXPR_COLUMN || expr->type == EXPR_ARRAY_ACCESS)) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_EQUAL_IN_SET");
      return command;
    }

    int col_index = expr->column.index;
  
    if (parser->cur->type != TOK_EQ) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_EQUAL_IN_SET");
      return command;
    }
    parser_consume(parser); 
    
    ExprNode* value = parser_parse_expression(parser, command.schema);
    if (!value) {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUE_IN_SET");
      return command;
    }
  
    command.update_columns[value_count].index = col_index;
    command.update_columns[value_count].array_idx = expr->type == EXPR_ARRAY_ACCESS ? 
                         expr->column.array_idx : NULL;
    command.values[0][value_count] = value;

    free_expr_node(expr);
    value_count++;
  
    if (parser->cur->type == TOK_COM) {
      parser_consume(parser); 
    } else {
      break;
    }
  }

  for (int i = 0; i < value_count; ++i) {
    ExprNode* val = command.values[0][i];
  
    if (command.schema->columns[command.update_columns[i].index].is_not_null /* && val->is_null */) {
      LOG_ERROR("Column is NOT NULL but attempted to set NULL");
      return command;
    }
  }  
  
  command.value_counts[0] = value_count;

  parse_where_clause(parser, db, &command, idx);

  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_delete(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_DELETE;
  command.is_invalid = true;

  parser_consume(parser);  

  if (parser->cur->type != TOK_FRM) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_FROM");
    return command;
  }

  parser_consume(parser);  

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  command.schema = malloc(sizeof(TableSchema));
  if (!command.schema) {
    REPORT_ERROR(parser->lexer, "SYE_E_ALLOC_FAILED");
    return command;
  }

  strcpy(command.schema->table_name, parser->cur->value);
  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (is_struct_zeroed(&db->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Couldn't update row in table '%s', doesn't exist", command.schema->table_name);
    return command;
  }

  parser_consume(parser);

  parse_where_clause(parser, db, &command, idx);

  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_alter_table(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_ALTER;
  command.is_invalid = true;

  AlterTableCommand* alter_cmd = calloc(1, sizeof(AlterTableCommand));

  parser_consume(parser);

  if (parser->cur->type != TOK_TBL) {
    REPORT_ERROR(parser->lexer, "Expected TABLE keyword after ALTER");
    free(alter_cmd);
    return command;
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected table name after ALTER TABLE");
    free(alter_cmd);
    return command;
  }
  strcpy(alter_cmd->table_name, parser->cur->value);
  parser_consume(parser);

  switch (parser->cur->type) {
    case TOK_KW_ADD:
      parser_consume(parser);
      if (parser->cur->type == TOK_ID) {
        parser_consume(parser);
        if (!parse_alter_add_column(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_CNST) {
        parser_consume(parser);

        if (!parse_alter_add_constraint(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected COLUMN or CONSTRAINT after ADD");
        free(alter_cmd);
        return command;
      }
      break;
    case TOK_DRP:
        parser_consume(parser);
        if (parser->cur->type == TOK_ID) {
          parser_consume(parser);
          if (!parse_alter_drop_column(parser, alter_cmd)) {
            free(alter_cmd);
            return command;
          }
        } else if (parser->cur->type == TOK_CNST) {
          parser_consume(parser);
          if (!parse_alter_drop_constraint(parser, alter_cmd)) {
            free(alter_cmd);
            return command;
          }
        } else {
          REPORT_ERROR(parser->lexer, "Expected COLUMN or CONSTRAINT after DROP");
          free(alter_cmd);
          return command;
        }
        break;

    case TOK_RENAME:
      parser_consume(parser);
      if (parser->cur->type == TOK_ID) {
        parser_consume(parser);
        if (!parse_alter_rename_column(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_CNST) {
        parser_consume(parser);
        if (!parse_alter_rename_constraint(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_TO) {
        if (!parse_alter_rename_table(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected COLUMN, CONSTRAINT, or TO after RENAME");
        free(alter_cmd);
        return command;
      }
      break;

    case TOK_ALT:
      parser_consume(parser);

      if (parser->cur->type == TOK_KW_COL) {
        parser_consume(parser);
        if (!parse_alter_alter_column(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected COLUMN after ALTER");
        free(alter_cmd);
        return command;
      }
      break;

    case TOK_SET:
      parser_consume(parser);
      if (parser->cur->type == TOK_TABLESPACE) {
        parser_consume(parser);
        if (!parse_alter_set_tablespace(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_OWNER) {
        parser_consume(parser);
        if (!parse_alter_set_owner(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected TABLESPACE or OWNER after SET");
        free(alter_cmd);
        return command;
      }
      break;

    default:
      REPORT_ERROR(parser->lexer, "Unknown ALTER TABLE operation");
      free(alter_cmd);
      return command;
  }

  command.alter = alter_cmd;
  command.is_invalid = false;
  return command;
}

