#include "parser/parser.h"
#include "storage/database.h"

Parser* parser_init(Lexer* lexer) {
  Parser* parser = malloc(sizeof(Parser));
  
  parser->lexer = lexer;
  parser->cur = NULL;

  return parser;
}

void parser_reset(Parser* parser) {
  parser->cur = lexer_next_token(parser->lexer);
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

void jql_command_plain_init(JQLCommand* cmd, int cmd_type) {
  memset(cmd, 0, sizeof(JQLCommand)); 
  cmd->type = cmd_type; 
  cmd->is_invalid = true; 
}

JQLCommand parser_expect(Parser* parser, int expected, char* error_msg) {
  if ((parser)->cur->type != (expected)) { 
    REPORT_ERROR((parser)->lexer, (error_msg));
    return (JQLCommand){.is_invalid = true}; 
  }

  parser_consume(parser); 
}

JQLCommand parser_expect_nc(Parser* parser, int expected, char* error_msg) {
  if ((parser)->cur->type != (expected)) { 
    REPORT_ERROR((parser)->lexer, (error_msg));
    return (JQLCommand){.is_invalid = true}; 
  }
}

void parser_consume(Parser* parser) {
  if (parser->cur->type == TOK_EOF) {
    token_free(parser->cur);
    return;
  }

  token_free(parser->cur);

  parser->cur = lexer_next_token(parser->lexer);
}


ParserState parser_save_state(Parser* parser) {
  ParserState state;
  state.lexer_position = parser->lexer->i;
  state.lexer_line = parser->lexer->cl;
  state.lexer_column = parser->lexer->cc;

  state.buffer_size = parser->lexer->buf_size;
  state.buffer_copy = malloc(state.buffer_size);
  memcpy(state.buffer_copy, parser->lexer->buf, state.buffer_size);

  state.current_token = token_clone(parser->cur);

  return state;
}

void parser_restore_state(Parser* parser, ParserState state) {
  parser->lexer->i = state.lexer_position;
  parser->lexer->cl = state.lexer_line;
    parser->lexer->cc = state.lexer_column;

  free(parser->lexer->buf);

  parser->lexer->buf_size = state.buffer_size;
  parser->lexer->buf = malloc(state.buffer_size);
  memcpy(parser->lexer->buf, state.buffer_copy, state.buffer_size);

  parser->lexer->c = (parser->lexer->i < parser->lexer->buf_size) ?
                     parser->lexer->buf[parser->lexer->i] : '\0';

  if (parser->cur) token_free(parser->cur);
  parser->cur = token_clone(state.current_token);

  token_free(state.current_token);
  free(state.buffer_copy);
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


int find_column_index(TableSchema* schema, const char* name) {
  for (uint8_t i = 0; i < schema->column_count; i++) {
    if (!schema->columns[i].name) return -1;

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

TableSchema* get_validated_table(Database* db, const char* table_name) {
  uint32_t idx = hash_fnv1a(table_name, MAX_TABLES);
  if (is_struct_zeroed(&db->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Table '%s' doesn't exist", table_name);
    return NULL;
  }
  return db->tc[idx].schema;
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

AggregateType get_aggregate_type(const char* name) {
  if (strcmp(name, "COUNT") == 0) return AGG_COUNT;
  if (strcmp(name, "SUM") == 0) return AGG_SUM;
  if (strcmp(name, "AVG") == 0) return AGG_AVG;
  if (strcmp(name, "MIN") == 0) return AGG_MIN;
  if (strcmp(name, "MAX") == 0) return AGG_MAX;
  if (strcmp(name, "STDDEV") == 0) return AGG_STDDEV;
  if (strcmp(name, "VARIANCE") == 0) return AGG_VARIANCE;
  if (strcmp(name, "FIRST") == 0) return AGG_FIRST;
  if (strcmp(name, "LAST") == 0) return AGG_LAST;

  return NOT_AGG;
}

void print_column_value(ColumnValue* val) {
  if (val->is_null) {
    return;
  }

  if (val->is_array) {
    char buffer[1024];
    size_t offset = 0;
    
    for (size_t i = 0; i < val->array.array_size; i++) {
      char* elem_str = str_column_value(&val->array.array_value[i]);
      if (i > 0) {
        offset += snprintf(buffer + offset, sizeof(buffer) - offset, ", ");
      }
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s", elem_str);
    }
      
    offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%ld", val->array.array_size);

    printf("arr<%s>[%s]", get_token_type(val->type), buffer);
    return;
  }
  
  printf("<%s>[", get_token_type(val->type));

  
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

    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON: {
      if (val->is_toast) {
        printf("<%s>(%u)", token_type_strings[val->type], val->toast_object);
      } else {
        char* s = val->str_value;
        size_t len = strlen(s);
      
        if (len <= 20) {
          printf("\"%s\"", s);
        } else {
        char preview[12]; 
          memcpy(preview, s, 8);
          strcpy(preview + 8, ".");
          printf("\"%s +%zu\"", preview, len - 8);
        }

      }      
      break;
    }
    default:
      printf("unprintable type: %d", val->type);
      break;
  }

  printf("]");
}

char* str_column_value(ColumnValue* val) {
  if (val->is_null) {
    return "NULL";
  }
 
  if (val->is_array) {
    char buffer[1024];
    size_t offset = 0;
    
    for (size_t i = 0; i < val->array.array_size; i++) {
      char* elem_str = str_column_value(&val->array.array_value[i]);
      if (i > 0) {
        offset += snprintf(buffer + offset, sizeof(buffer) - offset, ", ");
      }
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s", elem_str);
    }

    offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%ld", val->array.array_size);

    return strdup(buffer);
  }

  char buffer[256];

  switch (val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      snprintf(buffer, sizeof(buffer), "%ld", val->int_value);
      break;

    case TOK_T_FLOAT:
      snprintf(buffer, sizeof(buffer), "%f", val->float_value);
      break;

    case TOK_T_DOUBLE:
      snprintf(buffer, sizeof(buffer), "%lf", val->double_value);
      break;

    case TOK_T_BOOL:
      snprintf(buffer, sizeof(buffer), "%s", val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      snprintf(buffer, sizeof(buffer), "\"%s\"", val->str_value);
      break;

    case TOK_T_DATE: {
      int y, m, d;
      decode_date(val->date_value, &y, &m, &d);
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", y, m, d);
      break;
    }

    case TOK_T_TIME: {
      int h, m, s;
      decode_time(val->time_value, &h, &m, &s);
      snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d", h, m, s);
      break;
    }

    case TOK_T_DATETIME: {
      DateTime dt = val->datetime_value;
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d",
               dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
      break;
    }

    case TOK_T_TIMESTAMP: {
      __dt ts;
      decode_timestamp(val->timestamp_value, &ts);
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d",
               ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second);
      break;
    }

    case TOK_T_TIME_TZ: {
      int h, m, s, offset_minutes;
      decode_time_TZ(val->time_tz_value, &h, &m, &s, &offset_minutes);
      int abs_offset = abs(offset_minutes);
      snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d%c%02d:%02d",
               h, m, s, (offset_minutes >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_DATETIME_TZ: {
      DateTime_TZ dt = val->datetime_tz_value;
      int abs_offset = abs(dt.time_zone_offset);
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d",
               dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
               (dt.time_zone_offset >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_INTERVAL: {
      const char* s = interval_to_string(&val->interval_value);
      return strdup(s);
    }

    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON: {
      if (val->is_toast) {
        snprintf(buffer, sizeof(buffer), "%u", token_type_strings[val->type], val->toast_object);
      } else {
        const char* s = val->str_value;
        size_t len = strlen(s);
        snprintf(buffer, sizeof(len), "\"%s\"", s);
      }
      break;
    }

    default:
      snprintf(buffer, sizeof(buffer), "unprintable type: %d", val->type);
      break;
  }

  return strdup(buffer);
}

char** stringify_column_array(ColumnValue* array_val, int* out_count) {
  if (!array_val || !array_val->is_array || array_val->array.array_size == 0) {
    if (out_count) *out_count = 0;
    return NULL;
  }

  size_t n = array_val->array.array_size;
  char** result = calloc(n + 1, sizeof(char*));
  if (!result) {
    if (out_count) *out_count = 0;
    return NULL;
  }

  for (size_t i = 0; i < n; i++) {
    ColumnValue* elem = &array_val->array.array_value[i];
    char* str = str_column_value(elem);

    if (str == NULL) {
      result[i] = NULL;
      continue;
    }

    if ((elem->type == TOK_T_TEXT || elem->type == TOK_T_BLOB || elem->type == TOK_T_STRING ||
        elem->type == TOK_T_VARCHAR || elem->type == TOK_T_CHAR || elem->type == TOK_T_JSON) &&
        !elem->is_toast) {
      size_t len = strlen(str);
      if (len >= 2 && ((str[0] == '\'' && str[len - 1] == '\'') || (str[0] == '\"' && str[len - 1] == '\"'))) {
        size_t new_len = len - 2;
        result[i] = malloc(new_len + 1);
        if (result[i]) {
          strncpy(result[i], str + 1, new_len);
          result[i][new_len] = '\0';
        }
      } else {
        result[i] = strdup(str);
      }
    } else {
      result[i] = strdup(str);
    }

    if (str != NULL && strcmp(str, "NULL") != 0) free(str);
  }

  if (out_count) *out_count = n;

  // LOG_DEBUG("!> stringified res: %s", result[0]);
  return result;
}


void format_column_value(char* out, size_t out_size, ColumnValue* val) {
  if (val->is_null) {
    snprintf(out, out_size, "NULL");
    return;
  }

  switch (val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      snprintf(out, out_size, "%ld", val->int_value);
      break;

    case TOK_T_FLOAT:
      snprintf(out, out_size, "%f", val->float_value);
      break;

    case TOK_T_DOUBLE:
      snprintf(out, out_size, "%lf", val->double_value);
      break;

    case TOK_T_BOOL:
      snprintf(out, out_size, "%s", val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      snprintf(out, out_size, "\"%s\"", val->str_value);
      break;

    case TOK_T_DATE: {
      int y, m, d;
      decode_date(val->date_value, &y, &m, &d);
      snprintf(out, out_size, "%04d-%02d-%02d", y, m, d);
      break;
    }

    case TOK_T_TIME: {
      int h, m, s;
      decode_time(val->time_value, &h, &m, &s);
      snprintf(out, out_size, "%02d:%02d:%02d", h, m, s);
      break;
    }

    case TOK_T_DATETIME: {
      DateTime dt = val->datetime_value;
      snprintf(out, out_size, "%04d-%02d-%02dT%02d:%02d:%02d",
               dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
      break;
    }

    case TOK_T_TIMESTAMP: {
      __dt ts;
      decode_timestamp(val->timestamp_value, &ts);
      snprintf(out, out_size, "%04d-%02d-%02dT%02d:%02d:%02d",
               ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second);
      break;
    }

    case TOK_T_TIME_TZ: {
      int h, m, s, offset_minutes;
      decode_time_TZ(val->time_tz_value, &h, &m, &s, &offset_minutes);
      int abs_offset = abs(offset_minutes);
      snprintf(out, out_size,
               "%02d:%02d:%02d%c%02d:%02d",
               h, m, s, (offset_minutes >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_DATETIME_TZ: {
      DateTime_TZ dt = val->datetime_tz_value;
      int abs_offset = abs(dt.time_zone_offset);
      snprintf(out, out_size,
               "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d",
               dt.year, dt.month, dt.day,
               dt.hour, dt.minute, dt.second,
               (dt.time_zone_offset >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_INTERVAL: {
      const char* s = interval_to_string(&val->interval_value);
      snprintf(out, out_size, "%s", s);
      break;
    }

    // case TOK_T_TEXT:
    // case TOK_T_BLOB:
    // // case TOK_T_JSON: {
    // //   if (val->is_toast) {
    // //     snprintf(out, out_size, "<%s>(%u)]", token_type_strings[val->type], val->toast_object);
    // //   } else {
    // //     const char* s = val->str_value;
    // //     size_t len = strlen(s);

    // //     if (len <= 8) {
    // //       snprintf(out, out_size, "\"%s\"", s);
    // //     } else {
    // //       char preview[12];
    // //       memcpy(preview, s, 8);
    // //       strcpy(preview + 8, "...");
    // //       snprintf(out, out_size, "\"%s (%zu chars)\"]", preview, len - 8);
    // //     }
    // //   }
    // //   break;
    // // }

    default:
      snprintf(out, out_size, "%s", "NULL");
      break;
  }
}

void parser_free(Parser* parser) {
  if (!parser) {
    return;
  }

  lexer_free(parser->lexer);

  parser = NULL;
}

void free_expr_node(ExprNode* node) {
  if (!node) return;
  if (node == NULL) return;

  switch (node->type) {
    case EXPR_LITERAL:
      if (node->literal.is_array && node->literal.array.array_value) {
        for (uint16_t i = 0; i < node->literal.array.array_size; i++) {
          free_expr_node((ExprNode*)&node->literal.array.array_value[i]);
        }
      }
      break;

    case EXPR_COLUMN:
      break;
    case EXPR_ARRAY_ACCESS:
      free_expr_node(node->column.array_idx);
      break;
    case EXPR_UNARY_OP:
      free_expr_node(node->unary);
      free_expr_node(node->arth_unary.expr);
      break;

    case EXPR_BINARY_OP:
      free_expr_node(node->binary.left);
      free_expr_node(node->binary.right);
      break;

    case EXPR_FUNCTION:
      if (node->fn.type == NOT_AGG) break; 
      if (node->fn.name) free(node->fn.name);
      for (uint8_t i = 0; i < node->fn.arg_count; i++) {
        free_expr_node(node->fn.args[i]);
      }
      free(node->fn.args);
      break;

    case EXPR_LIKE:
      free_expr_node(node->like.left);
      if (node->like.pattern) free(node->like.pattern);
      break;

    case EXPR_BETWEEN:
      free_expr_node(node->between.value);
      free_expr_node(node->between.lower);
      free_expr_node(node->between.upper);
      break;

    case EXPR_IN:
      free_expr_node(node->in.value);
      for (size_t i = 0; i < node->in.count; i++) {
        free_expr_node(node->in.list[i]);
      }
      free(node->in.list);
      break;

    case EXPR_LOGICAL_NOT:
      free_expr_node(node->arth_unary.expr);
      break;

    case EXPR_LOGICAL_AND:
    case EXPR_LOGICAL_OR:
    case EXPR_COMPARISON:
      free_expr_node(node->binary.left);
      free_expr_node(node->binary.right);
      break;

    default:
      break;
  }
}

void free_column_value(ColumnValue* val) {
  if (!val) return;

  if (val->is_array && val->array.array_value) {
    for (uint16_t i = 0; i < val->array.array_size; i++) {
      free_column_value(&val->array.array_value[i]);
    }
    free(val->array.array_value);
  } else if (val->str_value) {
    free(val->str_value);
  } 
}

void free_column_definition(ColumnDefinition* col_def) {
  if (!col_def || col_def == NULL) return;

  // if (col_def->has_default) {
  //   free_column_value(col_def->default_value);
  //   free(col_def->default_value);
  // }
}

void free_table_schema(TableSchema* schema) {
  if (schema == NULL) return;

  for (uint8_t i = 0; i < MAX_COLUMNS; i++) {
    // ColumnDefinition* col_def = &(schema->columns[i]); 
    // if (!col_def) continue;

    if (i >= schema->column_count) break;

    // free_column_definition(col_def);
  }

  free(schema->columns);
  schema->columns = NULL;

  schema->column_count = 0;
  schema->prim_column_count = 0;
  schema->not_null_count = 0;
}


void free_jql_command(JQLCommand* cmd) {
  if (!cmd) return;

  // if (cmd->bitmap) free(cmd->bitmap);

  if (cmd->has_where) {
    free_expr_node(cmd->where);
  }

  // if (cmd->values) {
  //   for (uint8_t i = 0; i < cmd->row_count; i++) {
  //     if (cmd->values[i]) {
  //       // for (uint8_t j = 0; j < MAX_COLUMNS; j++) {
  //       //   if (cmd->values[i][j]) free_expr_node(cmd->values[i][j]);
  //       // }
  //       // free(cmd->values[i]);
  //     }
  //   }
  //   free(cmd->values);
  // }

  // if (cmd->returning_columns) {
  //   for (uint8_t i = 0; i < cmd->ret_col_count; i++) {
  //     if (cmd->returning_columns[i]) free(cmd->returning_columns[i]);
  //   }
  //   free(cmd->returning_columns);
  // }

  // if (cmd->columns) {
  //   for (uint8_t i = 0; i < cmd->col_count; i++) {
  //     // if (cmd->columns[i]) free(cmd->columns[i]);
  //   }
  //   free(cmd->columns);
  // }

  if (cmd->sel_columns) {
    for (uint8_t i = 0; i < MAX_COLUMNS; i++) {
      if (i >= cmd->col_count) break;
      if (cmd->sel_columns[i].alias) free(cmd->sel_columns[i].alias);
      if (cmd->sel_columns[i].expr) free_expr_node(cmd->sel_columns[i].expr);
      // free(&cmd->sel_columns[i]);
    }
    free(cmd->sel_columns);
  }

  if (cmd->update_columns) {
    for (uint8_t i = 0; i < cmd->col_count; i++) {
      free_expr_node(cmd->update_columns[i].array_idx);
    }
    free(cmd->update_columns);
  }

  if (cmd->where) {
    free_expr_node(cmd->where);
  }

  if (cmd->order_by) {
    free(cmd->order_by);
  }
}