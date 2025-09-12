#include "parser/parser.h"
#include "storage/database.h"

static const AlterHandler __alter_handlers[] = {
  {TOK_KW_ADD, TOK_ID, parse_alter_add_column},
  {TOK_KW_ADD, TOK_CNST, parse_alter_add_constraint},
  {TOK_DRP, TOK_ID, parse_alter_drop_column},
  {TOK_DRP, TOK_CNST, parse_alter_drop_constraint},
  {TOK_RENAME, TOK_ID, parse_alter_rename_column},
  {TOK_RENAME, TOK_CNST, parse_alter_rename_constraint},
  {TOK_RENAME, TOK_TO, parse_alter_rename_table},
  {TOK_ALT, TOK_KW_COL, parse_alter_alter_column},
  {TOK_SET, TOK_TABLESPACE, parse_alter_set_tablespace},
  {TOK_SET, TOK_OWNER, parse_alter_set_owner}
};

JQLCommand* parser_parse(Database* db) {
  while (db->parser->cur->type == TOK_SC) parser_consume(db->parser);

  JQLCommand* command = NULL;
  
  static const struct {
    TokenType token;
    JQLCommand* (*handler)(Parser*, Database*);
  } handlers[] = {
    {TOK_CRT, parser_parse_create_table},
    {TOK_ALT, parser_parse_alter_table},
    {TOK_INS, parser_parse_insert},
    {TOK_SEL, parser_parse_select},
    {TOK_UPD, parser_parse_update},
    {TOK_DEL, parser_parse_delete},
    {TOK_DRP, parser_parse_drop_table},
  };
  
  for (int i = 0; i < sizeof(handlers)/sizeof(handlers[0]); i++) {
    if (db->parser->cur->type == handlers[i].token) {
      command = handlers[i].handler(db->parser, db);
      break;
    }
  }
  
  if (db->parser->cur->type == TOK_EOF) return command;

  if (!command || command->is_invalid) {
    while (db->parser->cur->type != TOK_SC) parser_consume(db->parser);
  } else if (db->parser->cur->type != TOK_SC && db->parser->cur->type != TOK_EOF) {
    LOG_ERROR(">> %d: %s", db->parser->cur->type, db->parser->cur->value);
    REPORT_ERROR(db->parser->lexer, "SYE_UE_SEMICOLON");
    command->is_invalid = true;
    return command;
  }
  
  parser_consume(db->parser);
  return command;
}

JQLCommand* parser_parse_create_table(Parser* parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_CREATE);
  
  TableSchema* schema = xcalloc(1, sizeof(TableSchema));
  parser_consume(parser);

  if (parser->cur->type == TOK_CI_A) {
    command->flag_a = true;
    parser_consume(parser);
  }

  parser_expect(parser, TOK_TBL, "SYE_E_TAFCR");

  bool if_not_exists = false;
  if (parser->cur->type == TOK_IF) {
    parser_consume(parser);
    parser_expect(parser, TOK_NOT, "SYE_NOT_AFIF_CT");
    parser_expect(parser, TOK_EXISTS, "SYE_NOT_AFIF_CT");
    if_not_exists = true;
  }

  parser_expect_nc(parser, TOK_ID, "SYE_E_TNAFTA");
  xstrcpy(schema->table_name, parser->cur->value);
  parser_consume(parser);

  uint32_t idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  if (!is_struct_zeroed(&db->tc[idx], sizeof(TableCatalogEntry))) {
    if (!if_not_exists) {
      LOG_ERROR("Table `%s` already exists", schema->table_name);
    }
    return command;
  }

  parser_expect(parser, TOK_LP, "SYE_E_PRNAFDYNA");
  schema->column_count = 0;
  schema->columns = xcalloc(MAX_COLUMNS, sizeof(ColumnDefinition));

  while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
    if (!parser_parse_column_definition(parser, command)) return command;
    
    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_CPRORCOM");
      return command;
    }
  }

  parser_expect(parser, TOK_RP, "SYE_E_CPR");
  command->is_invalid = false;

  command->schema_count = 1;
  command->schemas->ptr = schema;
  command->schemas->alias[0] = '\0';

  return command;
}

JQLCommand* parser_parse_insert(Parser *parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_INSERT);
  
  parser_consume(parser);

  if (parser->cur->type == TOK_CI_A) {
    command->flag_a = true;
    parser_consume(parser);
  }

  parser_expect(parser, TOK_INTO, "SYE_E_MISSING_INTO");
  parser_expect_nc(parser, TOK_ID, "SYE_E_MISSING_TABLE_NAME");

  TableSchema* schema = get_table_schema(db, parser->cur->value);
  if (!schema) {
    LOG_ERROR("! Table '%s' doesn't exist", parser->cur->value);
    return command;
  }
  command->schemas[0].ptr = schema;

  parser_consume(parser);
  command->columns = xcalloc(MAX_COLUMNS, sizeof(char *));

  if (parser->cur->type == TOK_LP) {
    parser_consume(parser);
    
    while (parser->cur->type == TOK_ID) {
      command->columns[command->col_count++] = xstrdup(parser->cur->value);
      parser_consume(parser);
      
      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else if (parser->cur->type != TOK_RP) {
        REPORT_ERROR(parser->lexer, "SYE_E_INVALID_COLUMN_LIST");
        return command;
      }
    }
    
    parser_expect(parser, TOK_RP, "SYE_E_EXPECTED_RP");
  } else {
    for (int i = 0; i < schema->column_count; i++) {
      command->columns[i] = xstrdup(schema->columns[i].name);
    }
    command->col_count = schema->column_count;
  }

  parser_expect(parser, TOK_VAL, "SYE_E_MISSING_VALUES");

  command->values = xcalloc(MAX_OPERATIONS, sizeof(ExprNode*));

  while (parser->cur->type == TOK_LP) {
    ExprNode** row = xcalloc(schema->column_count, sizeof(ExprNode*));
    parser_consume(parser);

    for (int i = 0; i < command->col_count; i++) {
      int row_idx = find_column_index(schema, command->columns[i]);
      if (row_idx < 0) {
        LOG_DEBUG("Invalid column index for '%s'", command->columns[i]);
        return command;
      }
      
      row[row_idx] = parser_parse_expression(parser, command);
      if (!row[row_idx]) return command;

      if (i < command->col_count - 1) {
        parser_expect(parser, TOK_COM, "SYE_E_INVALID_VALUES");
      }
    }

    parser_expect(parser, TOK_RP, "SYE_E_EXPECTED_RP_VALUES");

    command->value_counts[command->row_count] = command->col_count;
    command->values[command->row_count++] = row;

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else {
      break;
    }
  }

  if (parser->cur->type == TOK_RETURNING) {
    parser_consume(parser);
    command->returning_columns = xcalloc(command->col_count, sizeof(char *));

    if (parser->cur->type == TOK_MUL) {
      command->returning_columns = command->columns;
      command->ret_col_count = command->col_count;
      parser_consume(parser);
    } else {
      while (parser->cur->type == TOK_ID) {
        command->returning_columns[command->ret_col_count++] = xstrdup(parser->cur->value);
        parser_consume(parser);

        if (parser->cur->type == TOK_COM) {
          parser_consume(parser);
        } else {
          break;
        }
      }
    }
  }

  command->is_invalid = false;
  return command;
}

JQLCommand* parser_parse_select(Parser* parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_SELECT);

  parser_consume(parser);
  ParserState state = parser_save_state(parser);

  while (parser->cur->type != TOK_FRM && parser->cur->type != TOK_EOF) {
    parser_consume(parser);
  }

  if (parser->cur->type != TOK_FRM) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_FROM");
    return command;
  }
  parser_consume(parser);

  uint16_t cap = _jql_command_schemas_capacity;
  SchemaRef* special_ref = NULL;
  int special_index = -1;

  while (true) {
    if (parser->cur->type != TOK_ID && parser->cur->type != TOK_BANG) {
      REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
      return command;
    }

    TableSchema* schema_ptr = get_validated_table(db, parser->cur->value);
    if (!schema_ptr) {
      LOG_ERROR("Table '%s' does not exist", parser->cur->value);
      return command;
    }

    command->schemas = ensure_schema_capacity(command->schemas, &cap, command->schema_count + 1);
    SchemaRef* ref = &command->schemas[command->schema_count++];
    ref->ptr = schema_ptr;

    bool is_special = true;

    if (parser->cur->type == TOK_BANG) {
      is_special = true;
      parser_consume(parser); 
      if (parser->cur->type != TOK_ID) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_TABLE_NAME_AFTER_BANG");
        return command;
      }
      set_schema_alias(ref, parser->cur->value);
      parser_consume(parser);
    } else {
      parser_consume(parser);
      if (parser->cur->type == TOK_ID) {
        set_schema_alias(ref, parser->cur->value);
        parser_consume(parser);
      } else {
        set_schema_alias(ref, schema_ptr->table_name);
      }
    }

    if (is_special) {
      if (!special_ref) {
        special_ref = ref;
        special_index = command->schema_count - 1;
      } else {
        REPORT_ERROR(parser->lexer, "Only one primary !table allowed");
        return command;
      }
    }

    LOG_DEBUG("Schemas in command (count=%d):", command->schema_count);

    for (int i = 0; i < command->schema_count; i++) {
        SchemaRef* ref = &command->schemas[i];
        LOG_DEBUG("\t[%d] table_name='%s', alias='%s', ptr=%p",
              i,
              ref->ptr ? ref->ptr->table_name : "NULL",
              ref->alias,
              (void*)ref->ptr);
    }


    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
      continue;
    }
    break;
  }

  if (special_ref && special_index != 0) {
    SchemaRef temp = command->schemas[0];
    command->schemas[0] = *special_ref;
    command->schemas[special_index] = temp;
  }

  parser_restore_state(parser, state);

  command->sel_columns = xcalloc(MAX_COLUMNS, sizeof(SelectColumn));
  int column_count = 0;
  
  if (parser->cur->type == TOK_MUL) {
    for (int i = 0; i < command->schemas[0].ptr->column_count; i++) {
      ExprNode* expr = xmalloc(sizeof(ExprNode));
      expr->type = EXPR_COLUMN;
      expr->column.index = i;
      command->sel_columns[i].expr = expr;
    }
    column_count = command->schemas[0].ptr->column_count;
    parser_consume(parser);
  } else {
    command->sel_columns = xcalloc(MAX_COLUMNS, sizeof(SelectColumn));

    while (true) {
      LOG_DEBUG("CURRENT TOKEN: %s", parser->cur->value);
      ExprNode* expr = parser_parse_expression(parser, command);
      if (!expr) {
        REPORT_ERROR(parser->lexer, "E_INVALID_COLUMN_EXPR");
        return command;
      }

      char* alias = NULL;
      if (parser->cur->type == TOK_AS) {
        parser_consume(parser);
        if (parser->cur->type != TOK_ID) {
          REPORT_ERROR(parser->lexer, "E_IDEN_AF_ALIAS_KW");
          // free_expr_node(expr);
          return command;
        }
        alias = xstrdup(parser->cur->value);
        parser_consume(parser);
      }
      
      SelectColumn* col = &command->sel_columns[column_count++];
      col->expr = expr;
      col->alias = alias;
      col->type = (expr->type == EXPR_FUNCTION) ? expr->fn.type : -1;
      
      if (parser->cur->type != TOK_COM) break;
      parser_consume(parser);
    }
  }

  command->value_counts[0] = column_count;
  
  while (parser->cur->type != TOK_WR && parser->cur->type != TOK_ODR &&
        parser->cur->type != TOK_LIM && parser->cur->type != TOK_OFF) {
    parser_consume(parser);
    if (parser->cur->type == TOK_EOF) return command; 
  }

  
  parse_where_clause(parser, db, command);
  parse_order_by_clause(parser, db, command);
  parse_limit_clause(parser, command);
  parse_offset_clause(parser, command);

  command->is_invalid = false;

  LOG_INFO("Parsed SELECT command successfully");
  return command;
}

JQLCommand* parser_parse_update(Parser* parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_UPDATE);
  
  parser_consume(parser);

  if (parser->cur->type == TOK_CI_A) {
    command->flag_a = true;
    parser_consume(parser);
  }

  parser_expect_nc(parser, TOK_ID, "SYE_E_MISSING_TABLE_NAME");
  
  uint32_t idx = hash_fnv1a(parser->cur->value, MAX_TABLES);
  TableSchema* schema = db->tc[idx].schema;
  command->schemas[0].ptr = schema;

  if (is_struct_zeroed(schema, sizeof(TableSchema))) return command;
  
  parser_consume(parser);
  parser_expect(parser, TOK_SET, "SYE_E_EXPECTED_SET");
  
  command->update_columns = xcalloc(schema->column_count, sizeof(UpdateColumn));
  command->values = xcalloc(1, sizeof(ExprNode*));
  command->values[0] = xcalloc(schema->column_count, sizeof(ExprNode));
  command->row_count = 1;
  
  uint8_t null_bitmap_size = (schema->column_count + 7) / 8;
  command->bitmap = xcalloc(1, null_bitmap_size);
  if (!command->bitmap) return command;
  
  uint8_t value_count = 0;
  
  while (parser->cur->type == TOK_ID) {
    ExprNode* expr = parser_parse_primary(parser, command);
    if (!expr || !(expr->type == EXPR_COLUMN || expr->type == EXPR_ARRAY_ACCESS)) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_EQUAL_IN_SET");
      return command;
    }
    
    parser_expect(parser, TOK_EQ, "SYE_E_EXPECTED_EQUAL_IN_SET");
    
  ExprNode* value = parser_parse_expression(parser, command);
    if (!value) {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUE_IN_SET");
      return command;
    }
    
    UpdateColumn* update_col = &command->update_columns[value_count];
    update_col->index = expr->column.index;
    update_col->array_idx = (expr->type == EXPR_ARRAY_ACCESS) ? expr->column.array_idx : NULL;
    command->values[0][value_count] = value;
    
    if (schema->columns[update_col->index].is_not_null /* && value->is_null */) {
      LOG_ERROR("Column is NOT NULL but attempted to set NULL");
      free_expr_node(expr);
      return command;
    }
    
    free_expr_node(expr);
    value_count++;
    
    if (parser->cur->type != TOK_COM) break;
    parser_consume(parser);
  }
  
  command->value_counts[0] = value_count;
  parse_where_clause(parser, db, command);
  
  command->is_invalid = false;
  return command;
}

JQLCommand* parser_parse_delete(Parser* parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_DELETE);
  
  parser_consume(parser);
  parser_expect(parser, TOK_FRM, "SYE_E_EXPECTED_FROM");
  parser_expect_nc(parser, TOK_ID, "SYE_E_MISSING_TABLE_NAME");

  TableSchema* schema = xmalloc(sizeof(TableSchema));
  command->schemas[0].ptr = schema;
  xstrcpy(schema->table_name, parser->cur->value);
  
  if (!get_validated_table(db, schema->table_name)) return command;
  
  parser_consume(parser);
  
  parse_where_clause(parser, db, command);

  command->schemas[0].ptr = schema;
  command->is_invalid = false;
  return command;
}

JQLCommand* parser_parse_alter_table(Parser* parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_ALTER);
  
  AlterTableCommand* alter_cmd = xcalloc(1, sizeof(AlterTableCommand));
  
  parser_consume(parser);
  parser_expect(parser, TOK_TBL, "Expected TABLE keyword after ALTER");
  parser_expect_nc(parser, TOK_ID, "Expected table name after ALTER TABLE");
  
  xstrcpy(alter_cmd->table_name, parser->cur->value);
  parser_consume(parser);

  TokenType primary = parser->cur->type;
  parser_consume(parser);
  TokenType secondary = parser->cur->type;

  for (int i = 0; i < sizeof(__alter_handlers)/sizeof(__alter_handlers[0]); i++) {
    if (__alter_handlers[i].primary == primary && 
        __alter_handlers[i].secondary == secondary) {
      if (secondary != TOK_TO) parser_consume(parser);
      
      if (!__alter_handlers[i].handler(parser, alter_cmd)) {
        xfree(alter_cmd);
        return command;
      }
      
      command->alter = alter_cmd;
      command->is_invalid = false;
      return command;
    }
  }

  REPORT_ERROR(parser->lexer, "Unknown ALTER TABLE operation");
  xfree(alter_cmd);
  return command;
}

JQLCommand* parser_parse_drop_table(Parser* parser, Database* db) {
  JQLCommand* command = jql_command_init(CMD_DROP);

  parser_consume(parser); 

  parser_expect(parser, TOK_TBL, "SYE_E_EXPECTED_TABLE_KEYWORD");
  parser_expect_nc(parser, TOK_ID, "SYE_E_MISSING_TABLE_NAME");

  const char* table_name = parser->cur->value;
  TableSchema* schema = get_validated_table(db, table_name);

  if (!schema) {
    LOG_ERROR("Table '%s' does not exist", table_name);
    return command;
  }

  parser_consume(parser); 

  command->schemas[0].ptr = schema;
  command->is_invalid = false;
  return command;
}
