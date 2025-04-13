#include "parser.h"
#include "context.h"

Parser* parser_init(Lexer* lexer) {
  Parser* parser = malloc(sizeof(Parser));
  
  parser->lexer = lexer;
  parser->cur = NULL;
  parser->exec = calloc(1, sizeof(JQLCommand));

  return parser;
}

JQLCommand* jql_command_init(JQLCommandType type) {
  JQLCommand* cmd = malloc(sizeof(JQLCommand));
  if (!cmd) return NULL;

  cmd->type = type;
  cmd->schema->column_count = 0;
  cmd->schema->columns = NULL;
  cmd->value_count = 0;
  cmd->constraint_count = 0;
  cmd->function_count = 0;
  cmd->values = NULL;
  cmd->constraints = NULL;
  cmd->functions = NULL;

  memset(cmd->schema->table_name, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->conditions, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->order_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->group_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->having, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->limit, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->offset, 0, MAX_IDENTIFIER_LEN);
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
  free(cmd->constraints);
  free(cmd->functions);

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
      return parser_parse_create_table(ctx->parser);
    case TOK_INS:
      return parser_parse_insert(ctx->parser);
    case TOK_SEL: 
      return parser_parse_select(ctx->parser, ctx);
    case TOK_UPD:
      return parser_parse_update(ctx->parser, ctx);
    case TOK_DEL:
      return parser_parse_delete(ctx->parser, ctx);
    case TOK_EOF:
      return command;
    default:
      REPORT_ERROR(ctx->parser->lexer, "SYE_UNSUPPORTED");
      return command;
  }
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
    if (parser->cur->type != TOK_L_U8 || parser->cur->value[0] == '0') {
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
    if (parser->cur->type != TOK_L_U8 || parser->cur->value[0] == '0') {
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

    if (parser->cur->type != TOK_L_U8 || parser->cur->value[0] == '0') {
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

JQLCommand parser_parse_create_table(Parser *parser) {
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

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_TNAFTA");
    return command;
  }

  strcpy(command.schema->table_name, parser->cur->value);
  parser_consume(parser);

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

JQLCommand parser_parse_insert(Parser *parser) {
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

  command.schema = malloc(sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);
  parser_consume(parser); 

  // if (parser->cur->type == TOK_LP) { // TODO: Implement specified order inserts
  //   parser_consume(parser);
    
  //   command.insert.column_count = 0;
  //   command.insert.columns = calloc(MAX_COLUMNS, sizeof(char *));

  //   while (parser->cur->type == TOK_ID) {
  //     command.insert.columns[command.insert.column_count] = strdup(parser->cur->value);
  //     command.insert.column_count++;

  //     parser_consume(parser);

  //     if (parser->cur->type == TOK_COM) {
  //       parser_consume(parser); // Consume ','
  //     } else if (parser->cur->type == TOK_RP) {
  //       break;
  //     } else {
  //       REPORT_ERROR(parser->lexer, "SYE_E_INVALID_COLUMN_LIST");
  //       return command;
  //     }
  //   }

  //   if (parser->cur->type != TOK_RP) {
  //     REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
  //     return command;
  //   }

  //   parser_consume(parser);
  // }

  if (parser->cur->type != TOK_VAL) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_VALUES");
    return command;
  }

  parser_consume(parser);

  if (parser->cur->type != TOK_LP) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_LP_VALUES");
    return command;
  }

  parser_consume(parser); 

  command.values = calloc(MAX_COLUMNS, sizeof(ColumnValue));
  command.value_count = 0;

  while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
    if (!parser_parse_value(parser, &command.values[command.value_count])) {
      return command;
    }

    command.value_count++;

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser); 
    } else if (parser->cur->type == TOK_RP) {
      break;
    } else {
    REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUES");
      return command;
    }
  }

  if (parser->cur->type != TOK_RP) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP_VALUES");
    return command;
  }

  parser_consume(parser);

  if(parser->cur->type == TOK_SC) parser_consume(parser); 

  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_select(Parser* parser, Context* ctx) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_SELECT;
  command.is_invalid = true;

  parser_consume(parser); 

  command.value_count = 0;
  command.columns = calloc(MAX_COLUMNS, sizeof(char*));

  if (parser->cur->type == TOK_MUL) {
    command.columns[command.value_count] = strdup("*");
    command.value_count++;
    parser_consume(parser);
  } else {
    while (parser->cur->type == TOK_ID) {
      command.columns[command.value_count] = strdup(parser->cur->value);

      parser_consume(parser);
      command.value_count++;

      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else {
        break;
      }
    }
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

  command.schema = malloc(sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);
  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (is_struct_zeroed(&ctx->tc[idx].schema, sizeof(TableSchema))) {
    LOG_DEBUG("<<< %d", idx);
    return command;
  }

  parser_consume(parser); 

  if (parser->cur->type == TOK_WR) {
    parser_consume(parser);
    command.has_where = true;

    command.where = malloc(sizeof(ConditionNode));
    command.where = parser_parse_condition(parser, ctx->tc[idx].schema);
  }

  command.is_invalid = false;
  return command;
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
    LOG_DEBUG("<<< %d", idx);
    return command;
  }

  parser_consume(parser); 

  if (parser->cur->type != TOK_SET) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_SET");
    return command;
  }

  parser_consume(parser); 

  command.value_count = 0;
  command.columns = calloc(MAX_COLUMNS, sizeof(char*));
  command.values = calloc(MAX_COLUMNS, sizeof(ColumnValue));

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
  
    ColumnValue value = {0};
    if (!parser_parse_value(parser, &value)) {
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
  
    command.columns[command.value_count] = column_name;
    command.values[command.value_count] = value;
  
    if (value.is_null) {
      command.bitmap[col_index / 8] |= (1 << (col_index % 8));
    }

    command.value_count++;
  
    if (parser->cur->type == TOK_COM) {
      parser_consume(parser); 
    } else {
      break;
    }
  }


  for (int i = 0; i < command.value_count; ++i) {
    char* col_name = command.columns[i];
    ColumnValue* val = &command.values[i];
  
    int col_index = find_column_index(ctx->tc[idx].schema, col_name);
    if (col_index == -1) {
      LOG_ERROR("Column '%s' not found in schema", col_name);
      return command;
    }
  
    if (ctx->tc[idx].schema->columns[col_index].is_not_null && val->is_null) {
      LOG_ERROR("Column '%s' is NOT NULL but attempted to set NULL", col_name);
      return command;
    }
  }  
  

  if (parser->cur->type == TOK_WR) {
    parser_consume(parser);
    command.has_where = true;

    command.where = malloc(sizeof(ConditionNode));
    command.where = parser_parse_condition(parser, ctx->tc[idx].schema);
  }

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
    LOG_DEBUG("<<< Table not found in schema (idx=%d)", idx);
    return command;
  }

  parser_consume(parser);

  if (parser->cur->type == TOK_WR) {
    parser_consume(parser);  
    command.has_where = true;

    command.where = parser_parse_condition(parser, ctx->tc[idx].schema);
    if (!command.where) {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_WHERE_CLAUSE");
      return command;
    }
  }

  command.is_invalid = false;
  return command;
}

void parser_consume(Parser* parser) {
  if (parser->cur->type == TOK_EOF) {
    exit(0);
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
      return (literal_type == TOK_L_I8 || literal_type == TOK_L_I16 || 
              literal_type == TOK_L_I32 || literal_type == TOK_L_I64 || 
              literal_type == TOK_L_U8 || literal_type == TOK_L_U16 || 
              literal_type == TOK_L_U32 || literal_type == TOK_L_U64);

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
  memset(col_val, 0, sizeof(ColumnValue));  // Ensure clean struct

  switch (parser->cur->type) {
    case TOK_NL:
      col_val->is_null = true;
      break;
    case TOK_L_I8:
    case TOK_L_I16:
    case TOK_L_I32:
    case TOK_L_I64:
      col_val->type = parser->cur->type;
      col_val->int_value = strtol(parser->cur->value, NULL, 10);
      break;

    case TOK_L_U8:
    case TOK_L_U16:
    case TOK_L_U32:
    case TOK_L_U64: 
      col_val->type = parser->cur->type;
      col_val->int_value = strtoul(parser->cur->value, NULL, 10);
      break;

    case TOK_L_FLOAT:
      col_val->type = TOK_L_FLOAT;
      col_val->float_value = strtof(parser->cur->value, NULL);
      break;

    case TOK_L_DOUBLE:
      col_val->type = TOK_L_DOUBLE;
      col_val->double_value = strtod(parser->cur->value, NULL);
      break;

    case TOK_L_BOOL:
      col_val->type = TOK_L_BOOL;
      col_val->bool_value = (strcmp(parser->cur->value, "true") == 0);
      break;

    case TOK_L_STRING:
      col_val->type = TOK_L_STRING;
      strncpy(col_val->str_value, parser->cur->value, MAX_IDENTIFIER_LEN - 1);
      col_val->str_value[MAX_IDENTIFIER_LEN - 1] = '\0';  // Null-terminate
      break;

    case TOK_L_CHAR:
      col_val->type = TOK_L_CHAR;
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

ConditionNode* parser_parse_condition(Parser* parser, TableSchema* schema) {
  return parser_parse_logical_or(parser, schema);
}

ConditionNode* parser_parse_logical_or(Parser* parser, TableSchema* schema) {
  ConditionNode* node = parser_parse_logical_and(parser, schema);
  while (parser->cur->type == TOK_OR) {
    parser_consume(parser);  
    ConditionNode* right = parser_parse_logical_and(parser, schema);
    ConditionNode* new_node = calloc(1, sizeof(ConditionNode));
    new_node->type = CONDITION_OR;
    new_node->left = node;
    new_node->right = right;
    node = new_node;
  }
  return node;
}

ConditionNode* parser_parse_logical_and(Parser* parser, TableSchema* schema) {
  ConditionNode* node = parser_parse_logical_not(parser, schema);
  while (parser->cur->type == TOK_AND) {
    parser_consume(parser);  
    ConditionNode* right = parser_parse_logical_not(parser, schema);
    ConditionNode* new_node = calloc(1, sizeof(ConditionNode));
    new_node->type = CONDITION_AND;
    new_node->left = node;
    new_node->right = right;
    node = new_node;
  }
  return node;
}

ConditionNode* parser_parse_logical_not(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_NOT) {
    parser_consume(parser);
    ConditionNode* node = calloc(1, sizeof(ConditionNode));
    node->type = CONDITION_NOT;
    node->right = parser_parse_logical_not(parser, schema);
    return node;
  }

  if (parser->cur->type == TOK_LP) {
    parser_consume(parser);  
    ConditionNode* node = parser_parse_condition(parser, schema);
    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
      return NULL;
    }
    parser_consume(parser); 
    return node;
  }

  return parser_parse_comparison(parser, schema);
}

ConditionNode* parser_parse_comparison(Parser* parser, TableSchema* schema) {
  ConditionNode* node = calloc(1, sizeof(ConditionNode));

  parser_parse_column_or_value(parser, schema, node, true);  
  node->op = parser_parse_comparison_operator(parser);
  parser_parse_column_or_value(parser, schema, node, false);

  node->type = CONDITION_COMPARISON;
  return node;
}

void parser_parse_column_or_value(Parser* parser, TableSchema* schema, ConditionNode* node, bool left_side) {
  if (parser->cur->type == TOK_ID) {
    int col_index = find_column_index(schema, parser->cur->value);
    if (col_index == -1) {
      REPORT_ERROR(parser->lexer, "SYE_E_UNKNOWN_COLUMN", parser->cur->value);
      return;
    }

    if (left_side) {
      node->left_is_column = true;
      node->left_column_index = col_index;
    } else {
      node->right_is_column = true;
      node->right_column_index = col_index;
    }

    parser_consume(parser);
  } else {
    if (left_side) {
      node->left_is_column = false;
      parser_parse_value(parser, &node->left_value);
    } else {
      node->right_is_column = false;
      parser_parse_value(parser, &node->right_value);
    }
  }
}


ComparisonOp parser_parse_comparison_operator(Parser* parser) {
  ComparisonOp op;
  switch (parser->cur->type) {
    case TOK_EQ: op = COMP_EQ; break;
    case TOK_NE: op = COMP_NEQ; break;
    case TOK_LT: op = COMP_LT; break;
    case TOK_GT: op = COMP_GT; break;
    case TOK_LE: op = COMP_LTE; break;
    case TOK_GE: op = COMP_GTE; break;
    default:
      LOG_ERROR("Unexpected token for comparison operator: %d\n", parser->cur->type);
      op = -1;
  }
  parser_consume(parser);
  return op;
}

void free_condition_node(ConditionNode* node) {
  if (!node) return;
  if (node->left) free_condition_node(node->left);
  if (node->right) free_condition_node(node->right);
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
