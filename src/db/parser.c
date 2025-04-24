#include "parser.h"
#include "context.h"

Parser* parser_init(Lexer* lexer) {
  Parser* parser = malloc(sizeof(Parser));
  
  parser->lexer = lexer;
  parser->cur = NULL;

  return parser;
}

JQLCommand* jql_command_init(JQLCommandType type) {
  JQLCommand* cmd = malloc(sizeof(JQLCommand));
  if (!cmd) return NULL;

  cmd->type = type;
  cmd->schema = malloc(sizeof(TableSchema));
  if (!cmd->schema) {
    free(cmd);
    return NULL;
  }

  cmd->schema->column_count = 0;
  cmd->schema->columns = NULL;
  memset(cmd->schema->table_name, 0, MAX_IDENTIFIER_LEN);

  cmd->row_count = 0;
  cmd->values = NULL;

  cmd->constraint_count = 0;

  cmd->function_count = 0;

  memset(cmd->value_counts, 0, MAX_OPERATIONS);
  memset(cmd->conditions, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->order_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->group_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->having, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->join_table, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->join_condition, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->transaction, 0, MAX_IDENTIFIER_LEN);

  return cmd;
}

void parser_reset(Parser* parser) {
  parser->cur = lexer_next_token(parser->lexer);
}

void parser_free(Parser* parser) {
  if (!parser) {
    return;
  }

  lexer_free(parser->lexer);

  parser = NULL;
}

void jql_command_free(JQLCommand* cmd) {
  if (!cmd) return;

  free(cmd->schema->columns);
  
  free(cmd->values);

  free(cmd);
}

JQLCommand parser_parse(Context* ctx) {
  JQLCommand command = {0};
  memset(&command, 0, sizeof(JQLCommand));

  while (ctx->parser->cur->type == TOK_SC) {
    parser_consume(ctx->parser);
  }

  switch (ctx->parser->cur->type) {
    case TOK_CRT:
      command = parser_parse_create_table(ctx->parser, ctx);
      break;
    case TOK_INS:
      command = parser_parse_insert(ctx->parser, ctx);
      break;
    case TOK_SEL: 
      command = parser_parse_select(ctx->parser, ctx);
      break;
    case TOK_UPD:
      command = parser_parse_update(ctx->parser, ctx);
      break;
    case TOK_DEL:
      command = parser_parse_delete(ctx->parser, ctx);
      break;
    case TOK_EOF:
      return command;
    default:
      REPORT_ERROR(ctx->parser->lexer, "SYE_UNSUPPORTED");
      return command;
  }

  if (command.is_invalid) {
    while (ctx->parser->cur->type != TOK_SC) {      
      parser_consume(ctx->parser);
    }
  } else if (ctx->parser->cur->type != TOK_SC && ctx->parser->cur->type != TOK_EOF && !command.is_invalid) {
    REPORT_ERROR(ctx->parser->lexer, "SYE_UE_SEMICOLON");
    command.is_invalid = true;
  } else if (ctx->parser->cur->type == TOK_SC) {
    parser_consume(ctx->parser);
  }

  return command;
}


bool parser_parse_column_definition(Parser *parser, JQLCommand *command) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_CNAME");
    return false;
  }

  ColumnDefinition column;
  memset(&column, 0, sizeof(ColumnDefinition));

  strcpy(column.name, parser->cur->value);
  parser_consume(parser);

  if (!is_valid_data_type(parser)) {
    REPORT_ERROR(parser->lexer, "SYE_E_CDTYPE");
    return false;
  }

  column.type = parser->cur->type;

  if (column.type == TOK_T_SERIAL) {
    column.is_auto_increment = true;
  }

  parser_consume(parser);

  if (column.type == TOK_T_VARCHAR && parser->cur->type == TOK_LP) {
    parser_consume(parser);
    if (parser->cur->type != TOK_L_UINT || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_VARCHAR_VALUE", parser->cur->value);
      return false;
    }

    column.type_varchar = (uint8_t)atoi(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_CPR");
      return false;
    }

    parser_consume(parser);
  }

  if (column.type == TOK_T_DECIMAL && parser->cur->type == TOK_LP) {
    parser_consume(parser);
    if (parser->cur->type != TOK_L_UINT || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_PRECISION_VALUE", parser->cur->value);
      return false;
    }

    column.type_decimal_precision = (uint8_t)atoi(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_COM) {
      REPORT_ERROR(parser->lexer, "SYE_E_COMMA");
      return false;
    }

    parser_consume(parser);

    if (parser->cur->type != TOK_L_UINT || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_PRECISION_VALUE", parser->cur->value);
      return false;
    }

    column.type_decimal_scale = (uint8_t)atoi(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_CPR");
      return false;
    }

    parser_consume(parser);
  }

  while (parser->cur->type != TOK_COM && parser->cur->type != TOK_RP) {
    switch (parser->cur->type) {
      case TOK_PK:
        column.is_primary_key = true;
        column.is_unique = true;
        column.is_not_null = true;
        break;
      case TOK_UNQ:
        column.is_unique = true;
        break;
      case TOK_NOT:
        parser_consume(parser); 

        if (parser->cur->type != TOK_NL) {
          REPORT_ERROR(parser->lexer, "SYE_E_NLAFNOT");
          return false;
        }

        column.is_not_null = true;
        break;
      case TOK_DEF:
        parser_consume(parser);
        
        if (!is_valid_default(parser, column.type, parser->cur->type)) {
          REPORT_ERROR(parser->lexer, "SYE_E_DEFVAL");
          return false;
        }
    
        strcpy(column.default_value, parser->cur->value);
        column.has_default = true;
        
        break;
      case TOK_CHK:
        parser_consume(parser);
        column.has_check = true;
        memset(column.check_expr, 0, MAX_IDENTIFIER_LEN);

        size_t check_expr_len = 0;
        while (parser->cur->type != TOK_COM && parser->cur->type != TOK_RP) {
          if (parser->cur->type != TOK_RP && parser->cur->type != TOK_LP) {
            if (check_expr_len + strlen(parser->cur->value) < MAX_IDENTIFIER_LEN - 1) {
              strcat(column.check_expr, parser->cur->value);
              check_expr_len += strlen(parser->cur->value);
            }
          }
          parser_consume(parser);
        }
        break;
      case TOK_FK:
        column.is_foreign_key = true;
        parser_consume(parser);
        if (parser->cur->type != TOK_REF) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_REF");
          return false;
        }
        parser_consume(parser);
        if (parser->cur->type != TOK_ID) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_TBL");
          return false;
        }
        strcpy(column.foreign_table, parser->cur->value);
        parser_consume(parser);

        if (parser->cur->type != TOK_LP) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_LP");
          return false;
        }
        parser_consume(parser);

        if (parser->cur->type != TOK_ID) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_COL");
          return false;
        }
        strcpy(column.foreign_column, parser->cur->value);
        parser_consume(parser);

        if (parser->cur->type != TOK_RP) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_RP");
          return false;
        }
        break;
      case TOK_IDX:
        column.is_index = true;
        break;
      default:
        REPORT_ERROR(parser->lexer, "SYE_U_COLDEF", parser->cur->value);
        return false;
    }

    parser_consume(parser);
  }

  command->schema->columns[command->schema->column_count] = column;
  command->schema->column_count += 1;

  return true;
}

JQLCommand parser_parse_create_table(Parser* parser, Context* ctx) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_CREATE;
  command.is_invalid = true;

  command.schema = malloc(sizeof(TableSchema));

  parser_consume(parser);

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

  if (!is_struct_zeroed(&ctx->tc[idx], sizeof(TableCatalogEntry))) {
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

JQLCommand parser_parse_insert(Parser *parser, Context* ctx) {
  JQLCommand command;  

  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_INSERT;
  command.is_invalid = true;

  parser_consume(parser); 

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
  command.schema = malloc(sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);
  parser_consume(parser); 
  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (parser->cur->type == TOK_LP) { 
    parser_consume(parser);
    
    command.columns = calloc(MAX_COLUMNS, sizeof(char *));

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

    parser_consume(parser);
  }

  if (parser->cur->type != TOK_VAL) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_VALUES");
    return command;
  }

  parser_consume(parser);

  command.values = calloc(MAX_OPERATIONS, sizeof(ExprNode*));
  command.row_count = 0;

  bool un_spec_flag = command.col_count == 0;
  command.col_count = command.col_count == 0 ? 
    ctx->tc[idx].schema->column_count 
    : command.col_count;

  while (parser->cur->type == TOK_LP) {
    ExprNode** row = calloc(ctx->tc[idx].schema->column_count, sizeof(ExprNode*));

    parser_consume(parser); 

    uint8_t value_count = 0; 

    while (value_count < command.col_count) {

      int row_idx = un_spec_flag ? 
        value_count 
        : find_column_index(ctx->tc[idx].schema, command.columns[value_count]);
      
      row[row_idx] = parser_parse_expression(parser, ctx->tc[idx].schema);
      if (!row[row_idx]) return command;
      value_count++;
      LOG_DEBUG("value type @ idx %d is %d", value_count, row[row_idx]->type);

      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else if (parser->cur->type != TOK_RP) {
        REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUES");
        return command;
      }

      if (parser->cur->type == TOK_EOF) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP_VALUES");
        return command;  
      }

    }

    if (parser->cur->type != TOK_RP) {
      LOG_ERROR("Mismatch in number of expected attributes %d and actual attributes %d",
        command.col_count, value_count);
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

  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_select(Parser* parser, Context* ctx) {
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
  
  command.schema = malloc(sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);

  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);
  
  if (is_struct_zeroed(&ctx->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Couldn't fetch rows from table '%s', doesn't exist", command.schema->table_name);
    return command;
  }
    
  parser_restore_state(parser, state);
  
  uint8_t value_count = 0;
  
  if (parser->cur->type == TOK_MUL) {
    command.select_all = true;
    value_count++;
    parser_consume(parser);
  } else {
    int column_count = 0;
    command.sel_columns = calloc(MAX_COLUMNS, sizeof(ExprNode*));
    
    while (true) {
      ExprNode* expr = parser_parse_expression(parser, ctx->tc[idx].schema);
      
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

  parse_where_clause(parser, ctx, &command, idx);
  parse_order_by_clause(parser, ctx, &command, idx);
  parse_limit_clause(parser, &command);
  parse_offset_clause(parser, &command);

  command.is_invalid = false;
  return command;
}

void parse_where_clause(Parser* parser, Context* ctx, JQLCommand* command, uint32_t idx) {
  if (parser->cur->type == TOK_WR) {
    parser_consume(parser);
    command->has_where = true;
    command->where = malloc(sizeof(ExprNode));
    command->where = parser_parse_expression(parser, ctx->tc[idx].schema);
  }
}

void parse_limit_clause(Parser* parser, JQLCommand* command) {
  char* endptr;

  if (parser->cur->type == TOK_LIM) {
    parser_consume(parser);
    if (parser->cur->type == TOK_L_UINT) {
      command->has_limit = true;
      command->limit = (uint32_t)strtoul(parser->cur->value, &endptr, 10);
      parser_consume(parser);
    } else {
      REPORT_ERROR(parser->lexer, "E_INVALID_LIM_VALUE");
    }
  }
}

void parse_offset_clause(Parser* parser, JQLCommand* command) {
  char* endptr;

  if (parser->cur->type == TOK_OFF) {
    parser_consume(parser);
    if (parser->cur->type == TOK_L_UINT) {
      command->has_offset = true;
      command->offset = (uint32_t)strtoul(parser->cur->value, &endptr, 10);
      parser_consume(parser);
    } else {
      REPORT_ERROR(parser->lexer, "E_INVALID_OFFSET_VALUE");
    }
  }
}

void parse_order_by_clause(Parser* parser, Context* ctx, JQLCommand* command, uint32_t idx) {
  if (parser->cur->type == TOK_ODR) {
    parser_consume(parser);
    if (parser->cur->type != TOK_BY) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_BY_AFTER_ORDER");
      return;
    }

    parser_consume(parser);

    command->has_order_by = true;
    command->order_by_count = 0;
    command->order_by = calloc(ctx->tc[idx].schema->column_count, (sizeof(bool) + (2 * sizeof(uint8_t))));

    while (true) {
      ExprNode* ord_expr = parser_parse_expression(parser, ctx->tc[idx].schema);
      if (!ord_expr || ord_expr->type != EXPR_COLUMN) {
        REPORT_ERROR(parser->lexer, "E_INVALID_ORDER_EXPRESSION");
        return;
      }

      command->order_by[command->order_by_count].decend = false;
      if (parser->cur->type == TOK_ASC) {
        parser_consume(parser);
      } else if (parser->cur->type == TOK_DESC) {
        parser_consume(parser);
        command->order_by[command->order_by_count].decend = true;      
      }

      command->order_by[command->order_by_count].col = ord_expr->column_index;
      command->order_by[command->order_by_count].type = ord_expr->type;
      command->order_by_count++;

      if (parser->cur->type != TOK_COM) break;
      if (command->order_by_count > ctx->tc[idx].schema->column_count) {
        LOG_ERROR("Got more ORDER basises (%d) than existing columns (%d)", command->order_by_count, ctx->tc[idx].schema->column_count);
        return;
      }
      parser_consume(parser);
    }
  }
}

JQLCommand parser_parse_update(Parser* parser, Context* ctx) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_UPDATE;
  command.is_invalid = true;

  parser_consume(parser); 

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  command.schema = malloc(sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);
  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (is_struct_zeroed(&ctx->tc[idx].schema, sizeof(TableSchema))) {
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

  command.columns = calloc(ctx->tc[idx].schema->column_count, sizeof(char*));
  command.values[0] = calloc(ctx->tc[idx].schema->column_count, sizeof(ExprNode));

  uint8_t null_bitmap_size = (ctx->tc[idx].schema->column_count + 7) / 8;
  command.bitmap = (uint8_t*)malloc(null_bitmap_size);
  if (!command.bitmap) {
    return command;
  }
  memset(command.bitmap, 0, null_bitmap_size);

  while (parser->cur->type == TOK_ID) {
    char* column_name = strdup(parser->cur->value);
    parser_consume(parser);
  
    if (parser->cur->type != TOK_EQ) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_EQUAL_IN_SET");
      return command;
    }
    parser_consume(parser); 
    
    ExprNode* value = parser_parse_expression(parser, ctx->tc[idx].schema);
    if (!value) {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUE_IN_SET");
      free(column_name);
      return command;
    }
  
    int col_index = find_column_index(ctx->tc[idx].schema, column_name);
    if (col_index == -1) {
      REPORT_ERROR(parser->lexer, "SYE_E_UNKNOWN_COLUMN_IN_SET");
      free(column_name);
      return command;
    }
  
    command.columns[value_count] = column_name;
    command.values[0][value_count] = value;

    value_count++;
  
    if (parser->cur->type == TOK_COM) {
      parser_consume(parser); 
    } else {
      break;
    }
  }

  for (int i = 0; i < value_count; ++i) {
    char* col_name = command.columns[i];
    ExprNode* val = command.values[0][i];
  
    int col_index = find_column_index(ctx->tc[idx].schema, col_name);
    if (col_index == -1) {
      LOG_ERROR("Column '%s' not found in schema", col_name);
      return command;
    }
  
    if (ctx->tc[idx].schema->columns[col_index].is_not_null /* && val->is_null */) {
      LOG_ERROR("Column '%s' is NOT NULL but attempted to set NULL", col_name);
      return command;
    }
  }  
  
  command.value_counts[0] = value_count;

  parse_where_clause(parser, ctx, &command, idx);

  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_delete(Parser* parser, Context* ctx) {
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

  if (is_struct_zeroed(&ctx->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Couldn't update row in table '%s', doesn't exist", command.schema->table_name);
    return command;
  }

  parser_consume(parser);

  parse_where_clause(parser, ctx, &command, idx);

  command.is_invalid = false;
  return command;
}

void parser_consume(Parser* parser) {
  if (parser->cur->type == TOK_EOF) {
    return;
  }

  token_free(parser->cur);

  parser->cur = lexer_next_token(parser->lexer);
}

bool is_valid_data_type(Parser *parser) {
  if (VALID_TYPES_MASK & (1 << parser->cur->type)) {
    return true;
  }
  
  return false;
}

bool is_valid_default(Parser* parser, int column_type, int literal_type) {
  switch (column_type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
      return (literal_type == TOK_L_INT || TOK_L_UINT);

    case TOK_T_FLOAT:
    case TOK_T_DOUBLE:
    case TOK_T_DECIMAL:
      return (literal_type == TOK_L_FLOAT || literal_type == TOK_L_DOUBLE);

    case TOK_T_BOOL:
      return (literal_type == TOK_L_BOOL);

    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
    case TOK_T_TEXT:
    case TOK_T_JSON:
    case TOK_T_UUID:
      return (literal_type == TOK_L_STRING);

    case TOK_T_DATE:
    case TOK_T_TIME:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
      return (literal_type == TOK_L_STRING); 

    case TOK_T_BLOB:
      REPORT_ERROR(parser->lexer, "INV_BLOB_DEFVAL");
      return false; 
    default:
      return false;
  }
}

bool parser_parse_value(Parser* parser, ColumnValue* col_val) {
  memset(col_val, 0, sizeof(ColumnValue)); 

  switch (parser->cur->type) {
    case TOK_NL:
      col_val->is_null = true;
      break;
    case TOK_L_INT:
      col_val->type = TOK_T_INT;
      col_val->int_value = strtol(parser->cur->value, NULL, 10);
      break;
    case TOK_L_UINT:
      col_val->type = TOK_T_UINT;
      col_val->int_value = strtoul(parser->cur->value, NULL, 10);
      break;
    case TOK_L_FLOAT:
      col_val->type = TOK_T_FLOAT;
      col_val->float_value = strtof(parser->cur->value, NULL);
      break;
    case TOK_L_DOUBLE:
      col_val->type = TOK_T_DOUBLE;
      col_val->double_value = strtod(parser->cur->value, NULL);
      break;
    case TOK_L_BOOL:
      col_val->type = TOK_T_BOOL;
      col_val->bool_value = (strcmp(parser->cur->value, "true") == 0);
      break;
    case TOK_L_STRING:
      char temp_str[MAX_IDENTIFIER_LEN] = {0};
      memcpy(temp_str, parser->cur->value, MAX_IDENTIFIER_LEN);
      size_t value_len = strlen(parser->cur->value);

      DateTime dt;
      DateTime_TZ dt_tz;
      __dt temp_dt;
      bool has_time_component = false;
      bool has_date_component = false;
      bool has_timezone = false;
      
      has_timezone = (strpbrk(temp_str, "+-") != NULL);
      
      if (has_timezone && parse_to_datetime_TZ(temp_str, &dt_tz)) {
        col_val->type = TOK_T_DATETIME_TZ;
        memcpy(&col_val->datetime_tz_value, &dt_tz, sizeof(DateTime_TZ));
      } else if (parse_to_datetime(temp_str, &dt)) {
        has_time_component = (dt.hour != 0 || dt.minute != 0 || dt.second != 0);
        
        bool contains_time_separator = (strchr(temp_str, ':') != NULL);
        bool contains_date_separator = (strchr(temp_str, '-') != NULL);
        
        if (contains_date_separator && contains_time_separator) {
          col_val->type = TOK_T_DATETIME;
          memcpy(&col_val->datetime_value, &dt, sizeof(DateTime));
        } else if (contains_date_separator) {
          col_val->type = TOK_T_DATE;
          col_val->date_value = encode_date(dt.year, dt.month, dt.day);
        } else if (contains_time_separator) {
          col_val->type = TOK_T_TIME;
          col_val->time_value = encode_time(dt.hour, dt.minute, dt.second);
        } else {
          col_val->type = TOK_T_STRING;
          strncpy(col_val->str_value, parser->cur->value, MAX_IDENTIFIER_LEN - 1);
          col_val->str_value[MAX_IDENTIFIER_LEN - 1] = '\0';
        }

        if (parse_datetime(temp_str, &temp_dt)) {
          has_time_component = (temp_dt.hour != 0 || temp_dt.minute != 0 || temp_dt.second != 0);
          has_date_component = (temp_dt.year != JUGADBASE_EPOCH_YEAR || temp_dt.month != 1 || temp_dt.day != 1);
          
          if (has_date_component && has_time_component) {
            col_val->type = TOK_T_TIMESTAMP;
            col_val->timestamp_value = encode_timestamp(&temp_dt);
          } else if (has_date_component) {
            col_val->type = TOK_T_DATE;
            col_val->date_value = encode_date(temp_dt.year, temp_dt.month, temp_dt.day);
          } else if (has_time_component) {
            col_val->type = TOK_T_TIME;
            col_val->time_value = encode_time(temp_dt.hour, temp_dt.minute, temp_dt.second);
          }
        }

        char* time_start = strchr(temp_str, ' '); 
        if (time_start != NULL) {
          time_start++; 
        } else {
          break;
        }


        if (has_timezone && strchr(time_start, ':') != NULL) {
          int hours = 0, minutes = 0, seconds = 0;
          int tz_hours = 0, tz_minutes = 0;
          char tz_sign = '+';
          char* tz_part = strpbrk(time_start, "+-");
          
          if (tz_part) {
            if (sscanf(time_start, "%d:%d:%d", &hours, &minutes, &seconds) >= 2) {
              tz_sign = *tz_part;
              tz_part++;
              
              if (sscanf(tz_part, "%d:%d", &tz_hours, &tz_minutes) != 2) {
                sscanf(tz_part, "%2d%2d", &tz_hours, &tz_minutes);
              }
              
              int32_t offset = tz_hours * 60 + tz_minutes;
              if (tz_sign == '-') {
                offset = -offset;
              }
              
              col_val->type = col_val->type == TOK_T_TIMESTAMP ? TOK_T_TIMESTAMP_TZ : TOK_T_TIME_TZ;
              col_val->time_tz_value = encode_time_TZ(hours, minutes, seconds, offset);
            }
          }
        }
      } else {
        col_val->type = TOK_T_STRING;
        col_val->str_value = malloc(strlen(parser->cur->value) + 1);
        strcpy(col_val->str_value, parser->cur->value);
        col_val->str_value[strlen(col_val->str_value)] = '\0';
      }
      break;
    case TOK_L_CHAR:
      col_val->type = TOK_T_CHAR;
      col_val->str_value[0] = parser->cur->value[0];  
      col_val->str_value[1] = '\0';
      break;
    default:
      REPORT_ERROR(parser->lexer, "SYE_E_UNSUPPORTED_LITERAL_TYPE");
      return false;
  }

  parser_consume(parser);
  return true;
}

bool parser_parse_uuid_string(const char* uuid_str, uint8_t* output) {
  if (strlen(uuid_str) != 36) return false; 

  static const char hex_map[] = "0123456789abcdef";
  size_t j = 0;

  for (size_t i = 0; i < 36; i++) {
    if (uuid_str[i] == '-') continue;

    const char* p = strchr(hex_map, tolower(uuid_str[i]));
    if (!p) return false;

    output[j / 2] = (output[j / 2] << 4) | (p - hex_map);
    j++;
  }

  return j == 32;
}

ParserState parser_save_state(Parser* parser) {
  ParserState state;
  state.lexer_position = parser->lexer->i;
  state.lexer_line = parser->lexer->cl;
  state.lexer_column = parser->lexer->cc;
  state.current_token = token_clone(parser->cur);
  return state;
}

void parser_restore_state(Parser* parser, ParserState state) {
  parser->lexer->i = state.lexer_position;
  parser->lexer->cl = state.lexer_line;
  parser->lexer->cc = state.lexer_column;

  parser->lexer->c = (parser->lexer->i < parser->lexer->buf_size) ?
                     parser->lexer->buf[parser->lexer->i] : '\0';

  if (parser->cur) token_free(parser->cur);
  parser->cur = token_clone(state.current_token);

  token_free(state.current_token);
}


Token* parser_peek_ahead(Parser* parser, int offset) {
  ParserState state = parser_save_state(parser);
  
  Token* token = parser->cur;
  
  for (int i = 0; i < offset; i++) {
    token = lexer_next_token(parser->lexer);
    if (token->type == TOK_EOF) {
      break;
    }
  }
  
  Token* result = token;
  
  parser_restore_state(parser, state);
  
  return result;
}

ExprNode* parser_parse_expression(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_logical_and(parser, schema);
  
  while (parser->cur->type == TOK_OR) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    
    ExprNode* right = parser_parse_logical_and(parser, schema);
    
    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_LOGICAL_OR;
    new_node->binary.left = node;
    new_node->binary.right = right;
    new_node->binary.op = op;
    node = new_node;
  }
  
  return node;
}

ExprNode* parser_parse_logical_and(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_logical_not(parser, schema);
  
  while (parser->cur->type == TOK_AND) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    
    ExprNode* right = parser_parse_logical_not(parser, schema);
    
    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_LOGICAL_AND;
    new_node->binary.left = node;
    new_node->binary.right = right;
    new_node->binary.op = op;
    node = new_node;
  }
  
  return node;
}

ExprNode* parser_parse_logical_not(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_NOT) {
    parser_consume(parser);
    
    ExprNode* node = calloc(1, sizeof(ExprNode));
    node->type = EXPR_LOGICAL_NOT;    
    node->unary = parser_parse_logical_not(parser, schema);
    
    return node;
  }

  return parser_parse_comparison(parser, schema);
}

ExprNode* parser_parse_comparison(Parser* parser, TableSchema* schema) {
  ExprNode* left = parser_parse_arithmetic(parser, schema);

  switch (parser->cur->type) {
    case TOK_LIKE:
      return parser_parse_like(parser, schema, left);
    case TOK_BETWEEN:
      return parser_parse_between(parser, schema, left);
    case TOK_IN:
      return parser_parse_in(parser, schema, left);
    case TOK_EQ:
    case TOK_NE:
    case TOK_LT:
    case TOK_GT:
    case TOK_LE:
    case TOK_GE: {
      uint16_t op = parser->cur->type;
      parser_consume(parser);
      ExprNode* right = parser_parse_arithmetic(parser, schema);

      ExprNode* node = calloc(1, sizeof(ExprNode));
      node->type = EXPR_COMPARISON;
      node->binary.left = left;
      node->binary.right = right;
      node->binary.op = op;

      return node;
    }
    default:
      return left;
  }
}


ExprNode* parser_parse_arithmetic(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_term(parser, schema);

  while (parser->cur->type == TOK_ADD || parser->cur->type == TOK_SUB) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    ExprNode* right = parser_parse_term(parser, schema);

    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_BINARY_OP;
    new_node->binary.op = op;
    new_node->binary.left = node;
    new_node->binary.right = right;
    node = new_node;
  }

  return node;
}

ExprNode* parser_parse_term(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_unary(parser, schema);

  while (parser->cur->type == TOK_MUL || parser->cur->type == TOK_DIV || parser->cur->type == TOK_MOD) {
    uint16_t op = parser->cur->type;

    parser_consume(parser);
    ExprNode* right = parser_parse_unary(parser, schema);

    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_BINARY_OP;
    new_node->binary.op = op;
    new_node->binary.left = node;
    new_node->binary.right = right;
    node = new_node;
  }

  return node;
}

ExprNode* parser_parse_unary(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_ADD || parser->cur->type == TOK_SUB) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    
    ExprNode* operand = parser_parse_unary(parser, schema); 
    if (!operand) return NULL;

    ExprNode* node = calloc(1, sizeof(ExprNode));
    node->type = EXPR_UNARY_OP;
    node->arth_unary.op = op;
    node->arth_unary.expr = operand;
    return node;
  }

  return parser_parse_primary(parser, schema); 
}

ExprNode* parser_parse_primary(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_LP) {
    parser_consume(parser);
    ExprNode* node = parser_parse_expression(parser, schema);
    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
      return NULL;
    }
    parser_consume(parser);
    return node;
  }

  if (parser->cur->type == TOK_ID) {
    char* ident = strdup(parser->cur->value);
    parser_consume(parser);
    

    if (parser->cur->type == TOK_LP) {
      ExprNode* node = calloc(1, sizeof(ExprNode));
      node->fn.args = calloc(MAX_FN_ARGS, sizeof(ExprNode));
      node->type = EXPR_FUNCTION;
      node->fn.name = ident;
      node->fn.arg_count = 0;

      parser_consume(parser); 

      if (parser->cur->type != TOK_RP) {
        while (true) {
          if (node->fn.arg_count >= MAX_FN_ARGS) {
            REPORT_ERROR(parser->lexer, "SYE_E_TOO_MANY_FN_ARGS");
            return NULL;
          }

          ExprNode* arg = parser_parse_expression(parser, schema);
          if (!arg) return NULL;

          node->fn.args[node->fn.arg_count] = arg;
          node->fn.arg_count += 1;
          if (parser->cur->type == TOK_COM) {
            parser_consume(parser);
          } else {
            break;
          }
        }
      }

      if (parser->cur->type != TOK_RP) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
        return NULL;
      }

      parser_consume(parser);
      return node;
    }

    int col_index = find_column_index(schema, ident);
    if (col_index == -1) {
      REPORT_ERROR(parser->lexer, "SYE_E_UNKNOWN_COLUMN", ident);
      return NULL;
    }

    ExprNode* node = calloc(1, sizeof(ExprNode));
    node->type = EXPR_COLUMN;
    node->column_index = col_index;
    return node;
  }

  ColumnValue val;
  if (!parser_parse_value(parser, &val)) return NULL;

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_LITERAL;
  node->literal = val;
  return node;
}

ExprNode* parser_parse_like(Parser* parser, TableSchema* schema, ExprNode* left) {
  parser_consume(parser);

  if (parser->cur->type != TOK_L_STRING) {
    REPORT_ERROR(parser->lexer, "E_VALID_PATTERN_LIKE");
  }

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_LIKE;
  node->like.left = left;
  node->like.pattern = strdup(parser->cur->value);

  parser_consume(parser);
  return node;
}

ExprNode* parser_parse_between(Parser* parser, TableSchema* schema, ExprNode* value) {
  parser_consume(parser);

  ExprNode* lower = parser_parse_arithmetic(parser, schema);

  if (parser->cur->type != TOK_AND) {
    REPORT_ERROR(parser->lexer, "E_EXPECTED_AND_BETWEEN");
  }
  parser_consume(parser);

  ExprNode* upper = parser_parse_arithmetic(parser, schema);

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_BETWEEN;
  node->between.value = value;
  node->between.lower = lower;
  node->between.upper = upper;

  return node;
}

ExprNode* parser_parse_in(Parser* parser, TableSchema* schema, ExprNode* value) {
  parser_consume(parser);

  if (parser->cur->type != TOK_LP) {
    REPORT_ERROR(parser->lexer, "E_EXPECTED_PAREN_IN");
  }
  parser_consume(parser);

  ExprNode** values = NULL;
  size_t count = 0;

  while (1) {
    ExprNode* val = parser_parse_arithmetic(parser, schema);
    values = realloc(values, sizeof(ExprNode*) * (count + 1));
    values[count++] = val;

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else {
      break;
    }
  }

  if (parser->cur->type != TOK_RP) {
    REPORT_ERROR(parser->lexer, "E_EXPECTED_CLOSING_PAREN_IN");
  }
  parser_consume(parser);

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_IN;
  node->in.value = value;
  node->in.list = values;
  node->in.count = count;

  return node;
}

void free_expr_node(ExprNode* node) {
  if (!node) return;
  if (node->type == EXPR_BINARY_OP || node->type == EXPR_LOGICAL_AND || node->type == EXPR_LOGICAL_OR || node->type == EXPR_COMPARISON) {
    free_expr_node(node->binary.left);
    free_expr_node(node->binary.right);
  } else if (node->type == EXPR_LOGICAL_NOT) {
    free_expr_node(node->binary.right);
  } else if (node->type == EXPR_FUNCTION) {
    for (uint8_t i = 0; i < node->fn.arg_count; i++) {
      free_expr_node(node->fn.args[i]);
    }
    free(node->fn.args);
  }
  free(node);
}

int find_column_index(TableSchema* schema, const char* name) {
  for (uint8_t i = 0; i < schema->column_count; i++) {
    if (strcmp(schema->columns[i].name, name) == 0) {
      return i;
    }
  }
  return -1;
}

bool is_primary_key_column(TableSchema* schema, int column_index) {
  if (column_index < 0 || column_index >= schema->column_count) return false;
  return schema->columns[column_index].is_primary_key;
}

void print_column_value(ColumnValue* val) {
  if (val->is_null) {
    return;
  }

  printf("%s[", get_token_type(val->type));

  switch (val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      printf("%ld", val->int_value);
      break;

    case TOK_T_FLOAT:
      printf("%f", val->float_value);
      break;

    case TOK_T_DOUBLE:
      printf("%lf", val->double_value);
      break;

    case TOK_T_BOOL:
      printf(val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      printf("\"%s\"", val->str_value);
      break;

    case TOK_T_DATE: {
      int year, month, day;
      decode_date(val->date_value, &year, &month, &day);
      printf("%04d-%02d-%02d", year, month, day);
      break;
    }

    case TOK_T_TIME: {
      int hour, minute, second;
      decode_time(val->time_value, &hour, &minute, &second);
      printf("%02d:%02d:%02d", hour, minute, second);
      break;
    }

    case TOK_T_DATETIME: {
      DateTime dt = val->datetime_value;
      printf("%04d-%02d-%02dT%02d:%02d:%02d", dt.year, dt.month, dt.day,
             dt.hour, dt.minute, dt.second);
      break;
    }

    case TOK_T_TIMESTAMP: {
      __dt ts;
      decode_timestamp(val->timestamp_value, &ts);
      printf("%04d-%02d-%02dT%02d:%02d:%02d", ts.year, ts.month, ts.day,
             ts.hour, ts.minute, ts.second);
      break;
    }

    case TOK_T_TIME_TZ: {
      int hour, minute, second, offset_minutes;
      decode_time_TZ(val->time_tz_value, &hour, &minute, &second, &offset_minutes);
      int abs_offset = abs(offset_minutes);
      int offset_hours = abs_offset / 60;
      int offset_min = abs_offset % 60;
      printf("%02d:%02d:%02d%c%02d:%02d", hour, minute, second,
             (offset_minutes >= 0 ? '+' : '-'), offset_hours, offset_min);
      break;
    }

    case TOK_T_DATETIME_TZ: {
      DateTime_TZ dt = val->datetime_tz_value;
      int abs_offset = abs(dt.time_zone_offset);
      int offset_hours = abs_offset / 60;
      int offset_min = abs_offset % 60;
      printf("%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", dt.year, dt.month, dt.day,
             dt.hour, dt.minute, dt.second,
             (dt.time_zone_offset >= 0 ? '+' : '-'),
             offset_hours, offset_min);
      break;
    }

    case TOK_T_INTERVAL: {
      printf("%s", interval_to_string(&val->interval_value));
      break;
    }

    case TOK_T_TEXT: {
      if (val->is_toast) {
        printf("TOAST(%u)", val->toast_object);
      } else {
        const char* s = val->str_value;
        size_t len = strlen(s);
      
        if (len <= 8) {
          LOG_DEBUG("\"%s\"", s);
          return;
        }
      
        char preview[12]; 
        memcpy(preview, s, 8);
        strcpy(preview + 8, "...");
        LOG_DEBUG("\"%s\" (%zu chars)", preview, len - 8);
        printf("(some text)");
        return;
      }      
      break;
    }
    default:
      printf("unprintable type: %d", val->type);
      break;
  }

  printf("]");
}


char* sprintf_column_value(ColumnValue* val, char* buffer) {  
  if (val->is_null) {
    snprintf(buffer, sizeof(buffer), "nil");
    return strdup(buffer);
  }

  const char* type_str = get_token_type(val->type);
  
  switch (val->type) {
    case TOK_T_INT: case TOK_T_UINT: case TOK_T_SERIAL: 
      snprintf(buffer, sizeof(buffer), "%s[%ld]", type_str, val->int_value);
      break;

    case TOK_T_FLOAT:
      snprintf(buffer, sizeof(buffer), "%s[%f]", type_str, val->float_value);
      break;

    case TOK_T_DOUBLE:
      snprintf(buffer, sizeof(buffer), "%s[%lf]", type_str, val->double_value);
      break;

    case TOK_T_BOOL:
      snprintf(buffer, sizeof(buffer), "%s[%s]", type_str, val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      snprintf(buffer, sizeof(buffer), "%s[\"%s\"]", type_str, val->str_value);
      break;

    default:
      snprintf(buffer, sizeof(buffer), "%s[unprintable type: %d]", type_str, val->type);
      break;
  }

  return strdup(buffer);
}

bool verify_select_col(SelectColumn* col, ColumnValue* evaluated_expr) {
  if (!col->expr || !evaluated_expr || is_struct_zeroed(evaluated_expr, sizeof(ColumnValue))) {
    return false;
  }

  if (evaluated_expr->type == EXPR_COMPARISON || 
    evaluated_expr->type == EXPR_LOGICAL_AND ||
    evaluated_expr->type == EXPR_LOGICAL_NOT ||
    evaluated_expr->type == EXPR_LOGICAL_OR
  ) {
    LOG_WARN("Latest query expects literals, functions, columns and binary operations between them, not logical comparisons. Query not processed.");
    return false;
  }

  return true;
}