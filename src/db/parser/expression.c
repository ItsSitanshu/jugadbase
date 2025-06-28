#include "parser/parser.h"
#include "storage/database.h"

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
      node->type = EXPR_FUNCTION;
      node->fn.name = ident;
      node->fn.type = get_aggregate_type(ident); 
      node->fn.args = calloc(MAX_FN_ARGS, sizeof(ExprNode*));
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

          node->fn.args[node->fn.arg_count++] = arg;

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

      parser_consume(parser); // consume ')'
      return node;
    }

    int col_index = find_column_index(schema, ident);
    if (col_index == -1) {
      REPORT_ERROR(parser->lexer, "SYE_E_UNKNOWN_COLUMN", ident);
      return NULL;
    }

    ExprNode* base = calloc(1, sizeof(ExprNode));
    base->type = EXPR_COLUMN;
    base->column.index = col_index;

    while (parser->cur->type == TOK_LB) {
      parser_consume(parser);

      ExprNode* index_expr = parser_parse_expression(parser, schema);
      if (!index_expr) return NULL;

      if (parser->cur->type != TOK_RB) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RBRACK");
        return NULL;
      }
      parser_consume(parser);

      base->type = EXPR_ARRAY_ACCESS;
      base->column.index = base->column.index;
      base->column.array_idx = index_expr;
    }

    return base;
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

bool parser_parse_value(Parser* parser, ColumnValue* col_val) {
  memset(col_val, 0, sizeof(ColumnValue)); 

  col_val->is_array = false;

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
      strncpy(temp_str, parser->cur->value, MAX_IDENTIFIER_LEN - 1);
      temp_str[MAX_IDENTIFIER_LEN - 1] = '\0'; 

      size_t value_len = strlen(parser->cur->value);

      DateTime dt;
      DateTime_TZ dt_tz;
      __dt temp_dt;
      
      bool has_time_component = false;
      bool has_date_component = false;
      bool has_timezone = false;
      
      has_timezone = (strpbrk(temp_str, "+-") != NULL);

      if (temp_str[0] == '{' && temp_str[value_len - 1] == '}') {
        char* array_contents = temp_str;  
        array_contents[value_len] = '\0';

        size_t count = 0;
        col_val->array.array_value = calloc(MAX_ARRAY_SIZE, sizeof(ColumnValue));

        uint32_t array_type = -1; 

        Lexer* tmp_lexer = lexer_init();
        Parser* tmp_parser = parser_init(tmp_lexer);
        lexer_set_buffer(tmp_parser->lexer, strdup(array_contents));
        tmp_parser->cur = lexer_next_token(tmp_parser->lexer);
        parser_consume(tmp_parser);

        if (tmp_parser->cur->type == TOK_RBR) {
          parser_consume(tmp_parser);
          array_type = TOK_NL;
        } 

        while (tmp_parser->cur->type != TOK_EOF
          && tmp_parser->cur->type != TOK_RBK
          && count < MAX_ARRAY_SIZE) {

          ColumnValue element_val;
          if (!parser_parse_value(tmp_parser, &element_val)) {
            free(col_val->array.array_value);
            return false;
          }

          if (array_type == -1) {
            array_type = element_val.type;
          }

          col_val->array.array_value[count] = element_val;
          count++;

          if (tmp_parser->cur->type != TOK_EOF 
            || tmp_parser->cur->type == TOK_COM) {
            parser_consume(tmp_parser);
          } else {
            break;
          }
        }

        parser_free(tmp_parser);

        col_val->array.array_size = count;
        col_val->array.array_type = array_type;

        col_val->is_array = true;
        col_val->type = col_val->array.array_type;
      } else if (has_timezone && parse_to_datetime_TZ(temp_str, &dt_tz)) {
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

bool is_valid_data_type(Parser *parser) {
  if (parser->cur->type > TOK_T_UINT) return false;

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
    case TOK_T_BLOB:
    case TOK_T_JSON:
    case TOK_T_UUID:
      return (literal_type == TOK_L_STRING);

    case TOK_T_DATE:
    case TOK_T_TIME:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
      return (literal_type == TOK_L_STRING); 
    default:
      return false;
  }
}
