#include "kernel/executor.h"

ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx, ColumnDefinition* out){
  ColumnValue value = evaluate_expression(expr, row, schema, db, schema_idx);

  if (expr->type == EXPR_COLUMN) {
    int col_index = expr->column.index;
    value = row->values[col_index];
    *out = schema->columns[col_index];
  }

  return value;
}


void swap_rows(Row* r1, Row* r2) {
  Row temp = *r1;
  *r1 = *r2;
  *r2 = temp;
}

int compare_rows(const Row* r1, const Row* r2, JQLCommand* cmd, TableSchema* schema) {
  for (uint8_t i = 0; i < cmd->order_by_count; i++) {
    uint8_t col = cmd->order_by[i].col;
    bool desc = cmd->order_by[i].decend;

    ColumnValue v1 = r1->values[col];
    ColumnValue v2 = r2->values[col];

    if (v1.type == TOK_T_VARCHAR || v1.type == TOK_T_STRING) {
      if (v1.is_null && !v2.is_null) return desc ? 1 : -1; 
      if (!v1.is_null && v2.is_null) return desc ? -1 : 1;
      
      if (v1.is_null && v2.is_null) {
        if (strlen(v1.str_value) == 0 && strlen(v2.str_value) == 0) return 0;
        if (strlen(v1.str_value) == 0) return desc ? 1 : -1;
        if (strlen(v2.str_value) == 0) return desc ? -1 : 1;
      }

      char* str1 = v1.str_value;
      char* str2 = v2.str_value;

      while (*str1 && *str2) {
        if (*str1 != *str2) {
          return desc ? (*str1 > *str2 ? -1 : 1) : (*str1 > *str2 ? 1 : -1);
        }
        str1++;
        str2++;
      }

      if (*str1 && !*str2) return desc ? -1 : 1;
      if (!*str1 && *str2) return desc ? 1 : -1;
    }

    int cmp = key_compare(get_column_value_as_pointer(&v1),
      get_column_value_as_pointer(&v2),
      cmd->order_by[i].type);

    if (cmp != 0) return desc ? -cmp : cmp;
  }
  return 0;
}

int partition_rows(Row rows[], int low, int high,
                          JQLCommand *cmd, TableSchema *schema) {
  Row pivot = rows[high];
  int i = low - 1;
  for (int j = low; j < high; ++j) {
    if (compare_rows(&rows[j], &pivot, cmd, schema) <= 0) {
      ++i;
      swap_rows(&rows[i], &rows[j]);
    }
  }
  swap_rows(&rows[i + 1], &rows[high]);
  return i + 1;
}

void quick_sort_rows(Row rows[], int low, int high,
                     JQLCommand *cmd, TableSchema *schema) {
  if (low < high) {
    int pi = partition_rows(rows, low, high, cmd, schema);
    quick_sort_rows(rows,     low, pi - 1, cmd, schema);
    quick_sort_rows(rows, pi + 1,   high, cmd, schema);
  }
}


bool match_char_class(char** pattern_ptr, char* str) {
  char* pattern = *pattern_ptr;
  bool negated = false;

  LOG_DEBUG("Initial pattern: %s", pattern);
  LOG_DEBUG("Character to match: %c", *str);

  if (*pattern == '^') {
    negated = true;
    pattern++;
    LOG_DEBUG("Negated character class detected, moving pattern pointer to: %s", pattern);
  }

  bool matched = false;

  while (*pattern && *pattern != ']') {
    if (*(pattern + 1) == '-' && *(pattern + 2) && *(pattern + 2) != ']') {
      char start = *pattern;
      char end = *(pattern + 2);

      LOG_DEBUG("Character range: %c-%c", start, end);

      if (start <= *str && *str <= end) {
        matched = true;
        LOG_DEBUG("Matched range: %c is between %c and %c", *str, start, end);
      }
      pattern += 3; 
    } else {
      if (*pattern == *str) {
        matched = true;
        LOG_DEBUG("Matched character: %c == %c", *pattern, *str);
      }
      pattern++; 
    }
  }

  if (*pattern == ']') {
    pattern++; 
    LOG_DEBUG("Closing character class found, moving pattern pointer to: %s", pattern);
  }

  *pattern_ptr = pattern;
  LOG_DEBUG("Pattern pointer updated to: %s", pattern);

  return negated ? !matched : matched;
}

bool like_match(char* str, char* pattern) {
  bool case_insensitive = false;

  if (strncmp(pattern, "(?i)", 4) == 0) {
    case_insensitive = true;
    pattern += 4;
    str = tolower_copy(str);
    pattern = tolower_copy(pattern);
  }

  while (*pattern) {
    if (*pattern == '\\') {
      pattern++;
      if (!*pattern) return false;
      if (*str != *pattern) return false;
      str++; pattern++;
    }
    else if (*pattern == '%' || *pattern == '*') {
      pattern++;
      if (!*pattern) return true;
      while (*str) {
        if (like_match(str, pattern)) return true;
        str++;
      }
      return false;
    }
    else if (*pattern == '_') {
      if (!*str) return false;
      str++; pattern++;
    }
    else if (*pattern == '[') {
      if (!*str) return false;
      pattern++;
      bool res = match_char_class(&pattern, str);
      if (!res) return false;
    } else {
      if (*str != *pattern) return false;
      str++; pattern++;
    }
  }

  return *str == '\0';
}

void* get_column_value_as_pointer(ColumnValue* col_val) {
  switch (col_val->type) {
    case TOK_NL:
      col_val->is_null = true;
      break;
    case TOK_T_INT: case TOK_T_UINT: case TOK_T_SERIAL:
      return &(col_val->int_value);
    case TOK_T_FLOAT:
      return &(col_val->float_value);
    case TOK_T_DOUBLE:
      return &(col_val->double_value);
    case TOK_T_BOOL:
      return &(col_val->bool_value);
    case TOK_T_CHAR:
      return &(col_val->str_value[0]);
    case TOK_T_STRING:
    case TOK_T_VARCHAR:
      return col_val->str_value;
    case TOK_T_BLOB:
    case TOK_T_JSON:
    case TOK_T_TEXT:
      return &(col_val->toast_object);
    case TOK_T_DECIMAL:
      return &(col_val->decimal.decimal_value);
    case TOK_T_DATE:
      return &(col_val->date_value);
    case TOK_T_TIME:
      return &(col_val->time_value);
    case TOK_T_TIME_TZ:
      return &(col_val->time_tz_value);
    case TOK_T_DATETIME:
      return &(col_val->datetime_value);
    case TOK_T_DATETIME_TZ:
      return &(col_val->datetime_tz_value);
    case TOK_T_TIMESTAMP:
      return &(col_val->timestamp_value);
    case TOK_T_TIMESTAMP_TZ:
      return &(col_val->timestamp_tz_value);
    default:
      return NULL;
  }

  return NULL;
}

bool infer_and_cast_va(size_t count, ...) {
  bool valid = true;
  va_list args;
  va_start(args, count);

  for (size_t i = 0; i < count; i++) {
    __c item = va_arg(args, __c);
    valid = infer_and_cast_value_raw(item.value, item.expected_type);

    if (!valid) {
      LOG_ERROR("Invalid conversion on item %zu", i);
      va_end(args);
      return false;
    }
  }

  va_end(args);
  return true;
}

bool infer_and_cast_value(ColumnValue* col_val, ColumnDefinition* def) {
  uint8_t target_type = def->type;

  if (col_val->type == TOK_NL) {
    col_val->is_null = true;
    return true;
  }

  if (col_val->type == target_type) {
    return true;
  }

  if (col_val->is_array) {
    return true;    
  }

  // LOG_DEBUG("%s => %s", token_type_strings[col_val->type], token_type_strings[target_type]);

  switch (col_val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL: {
      if (target_type == TOK_T_FLOAT) {
        col_val->float_value = (float)(col_val->int_value);
      } else if (target_type == TOK_T_DOUBLE) {
        col_val->double_value = (double)(col_val->int_value);
      } else if (target_type == TOK_T_BOOL) {
        col_val->bool_value = (col_val->int_value != 0);
      } else if (target_type == TOK_T_INT || 
                 target_type == TOK_T_UINT || 
                 target_type == TOK_T_SERIAL) {
          return true;
      } else {
        return false;
      }
      break;
    }
    case TOK_T_FLOAT: {
      if (target_type == TOK_T_DOUBLE) {
        col_val->double_value = (double)(col_val->float_value);
      } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (int64_t)(col_val->float_value);
      } else if (target_type == TOK_T_BOOL) {
        col_val->bool_value = (col_val->float_value != 0.0f);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DOUBLE: {
      if (target_type == TOK_T_FLOAT) {
        col_val->float_value = (float)(col_val->double_value);
      } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (int64_t)(col_val->double_value);
      } else if (target_type == TOK_T_BOOL) {
        col_val->bool_value = (col_val->double_value != 0.0);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_BOOL: {
      if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (col_val->bool_value ? 1 : 0);
      } else if (target_type == TOK_T_FLOAT) {
        col_val->float_value = (col_val->bool_value ? 1.0f : 0.0f);
      } else if (target_type == TOK_T_DOUBLE) {
        col_val->double_value = (col_val->bool_value ? 1.0 : 0.0);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_CHAR: {
      if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (int64_t)(col_val->str_value[0]);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_VARCHAR: {
      if (target_type == TOK_T_STRING) {
        (void)(0);  // No casting needed
      }
      break;
    }
    case TOK_T_STRING: {
      if (target_type == TOK_T_CHAR) {
        if (!(col_val->str_value && strlen(col_val->str_value) > 0)) {
          return false;
        }
      } else if (target_type == TOK_T_TEXT || target_type == TOK_T_JSON || target_type == TOK_T_BLOB) {
        return true;
      } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        char* endptr;
        col_val->int_value = strtoll(col_val->str_value, &endptr, 10);
        if (*endptr != '\0') {
          return false;
        }
      } else if (target_type == TOK_T_FLOAT) {
        char* endptr;
        col_val->float_value = strtof(col_val->str_value, &endptr);
        if (*endptr != '\0') {
          return false;
        }
      } else if (target_type == TOK_T_DOUBLE) {
        char* endptr;
        col_val->double_value = strtod(col_val->str_value, &endptr);
        if (*endptr != '\0') {
          return false;
        }
      } else if (target_type == TOK_T_BOOL) {
        if (strcasecmp(col_val->str_value, "true") == 0 || 
            strcmp(col_val->str_value, "1") == 0) {
          col_val->bool_value = true;
        } else if (strcasecmp(col_val->str_value, "false") == 0 || 
                  strcmp(col_val->str_value, "0") == 0) {
          col_val->bool_value = false;
        } else {
          return false;
        }
      } else if (target_type == TOK_T_INTERVAL) {
        Interval interval = {0, 0, 0}; 
        char* input = col_val->str_value;
        bool valid = false;
        
        if (input[0] == 'P') {
          valid = parse_iso8601_interval(input, &interval);
        } else {
          valid = parse_interval(input, &interval);
        }
        
        if (!valid) {
          return false;
        }
        
        col_val->interval_value = interval;
      } else if (target_type == TOK_T_VARCHAR) {
        size_t max_len = def->type_varchar;
        size_t str_len = strlen(col_val->str_value);

        if (str_len > max_len) {
          LOG_ERROR("Definition expects VARCHAR(<=%zu) got VARCHAR(<=%zu)", max_len, str_len);
          return false;
        }

        break;
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DATE: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = (int64_t)(col_val->date_value); 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        char* str = date_to_string(col_val->date_value);

        size_t new_size = strlen(str) + 1;
        col_val->str_value = calloc(1, new_size);
        snprintf(col_val->str_value, new_size, "%s", str);
      } else if (target_type == TOK_T_TIMESTAMP) {
        int y, m, d;
        decode_date(col_val->date_value, &y, &m, &d);

        __dt ts = {
          .year = y,
          .month = m,
          .day = d,
          .hour = 0,
          .minute = 0,
          .second = 0
        };

        col_val->timestamp_value = encode_timestamp(&ts);        
      } else {
        return false;
      } 
      break;
    }
    case TOK_T_TIME: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = (int64_t)(col_val->time_value); 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", time_to_string(col_val->time_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_TIMESTAMP: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = col_val->timestamp_value.timestamp;
      } else if (target_type == TOK_T_TIMESTAMP_TZ) {
        col_val->timestamp_tz_value.timestamp = col_val->timestamp_value.timestamp;
        col_val->timestamp_tz_value.time_zone_offset = 0; // assumes UTC 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", timestamp_to_string(col_val->timestamp_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_TIMESTAMP_TZ: {
      __dt dt;
      decode_timestamp_TZ(col_val->timestamp_tz_value, &dt);

      if (target_type == TOK_T_TIMESTAMP) {
        col_val->timestamp_value.timestamp = col_val->timestamp_tz_value.timestamp;
        col_val->type = TOK_T_TIMESTAMP;
      } else if (target_type == TOK_T_DATE) {
        col_val->date_value = encode_date(dt.year, dt.month, dt.day);
        col_val->type = TOK_T_DATE;
      } else if (target_type == TOK_T_TIME) {
        col_val->time_value = encode_time(dt.hour, dt.minute, dt.second); 
        col_val->type = TOK_T_TIME;
      } else if (target_type == TOK_T_TIME_TZ) {
        col_val->time_tz_value = encode_time_TZ(dt.hour, dt.minute, dt.second, dt.tz_offset);
        col_val->type = TOK_T_TIME_TZ;
      } else if (target_type == TOK_T_DATETIME) {
        col_val->datetime_value = create_datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
        col_val->time_tz_value.time_zone_offset = col_val->timestamp_tz_value.time_zone_offset;
        col_val->type = TOK_T_DATETIME;
      } else if (target_type == TOK_T_DATETIME_TZ) {
        col_val->datetime_tz_value = create_datetime_TZ(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.tz_offset);        
        col_val->type = TOK_T_DATETIME_TZ;
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", timestamp_tz_to_string(col_val->timestamp_tz_value));
        col_val->type = target_type;
      } else {
        return false;
      }
      break;
    }    
    case TOK_T_TIME_TZ: {
      if (target_type == TOK_T_TIME) {
        col_val->time_value = col_val->timestamp_tz_value.timestamp;
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", time_tz_to_string(col_val->time_tz_value));
      } else if (target_type == TOK_T_TIMESTAMP_TZ) {
        int h, m, s;
        decode_time(col_val->date_value, &h, &m, &s);

        __dt ts_tz = {
          .year = 0,
          .month = 0,
          .day = 0,
          .hour = h,
          .minute = m,
          .second = s
        };

        col_val->timestamp_tz_value = encode_timestamp_TZ(&ts_tz, col_val->time_tz_value.time_zone_offset);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_INTERVAL: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = (int64_t)(col_val->interval_value.micros); 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", interval_to_string(&col_val->interval_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DATETIME: {
      if (target_type == TOK_T_TIMESTAMP) {
        col_val->timestamp_value = datetime_to_timestamp(col_val->datetime_value);
      } else if (target_type == TOK_T_VARCHAR) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", datetime_to_string(col_val->datetime_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DATETIME_TZ: {
      if (target_type == TOK_T_TIMESTAMP_TZ) {
        col_val->timestamp_tz_value = datetime_TZ_to_timestamp_TZ(col_val->datetime_tz_value);
      } else {
        return false;
      }
      break;
    }    
    case TOK_T_TEXT: {
      if (!(target_type == TOK_T_BLOB || target_type == TOK_T_JSON)) return false;
      break;       
    }
    default:
      return false;
  }

  col_val->type = target_type;
  return true;
}

bool infer_and_cast_value_raw(ColumnValue* col_val, uint8_t target_type) {
  if (col_val->type == TOK_NL) {
    col_val->is_null = true;
    return true;
  }

  if (col_val->type == target_type) {
    return true;
  }

  // LOG_DEBUG("%s => %s", token_type_strings[col_val->type], token_type_strings[target_type]);

  switch (col_val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL: {
      if (target_type == TOK_T_FLOAT) {
        col_val->float_value = (float)(col_val->int_value);
      } else if (target_type == TOK_T_DOUBLE) {
        col_val->double_value = (double)(col_val->int_value);
      } else if (target_type == TOK_T_BOOL) {
        col_val->bool_value = (col_val->int_value != 0);
      } else if (target_type == TOK_T_INT || 
                 target_type == TOK_T_UINT || 
                 target_type == TOK_T_SERIAL) {
          (void)(0);  
      } else {
        return false;
      }
      break;
    }
    case TOK_T_FLOAT: {
      if (target_type == TOK_T_DOUBLE) {
        col_val->double_value = (double)(col_val->float_value);
      } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (int64_t)(col_val->float_value);
      } else if (target_type == TOK_T_BOOL) {
        col_val->bool_value = (col_val->float_value != 0.0f);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DOUBLE: {
      if (target_type == TOK_T_FLOAT) {
        col_val->float_value = (float)(col_val->double_value);
      } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (int64_t)(col_val->double_value);
      } else if (target_type == TOK_T_BOOL) {
        col_val->bool_value = (col_val->double_value != 0.0);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_BOOL: {
      if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (col_val->bool_value ? 1 : 0);
      } else if (target_type == TOK_T_FLOAT) {
        col_val->float_value = (col_val->bool_value ? 1.0f : 0.0f);
      } else if (target_type == TOK_T_DOUBLE) {
        col_val->double_value = (col_val->bool_value ? 1.0 : 0.0);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_CHAR: {
      if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        col_val->int_value = (int64_t)(col_val->str_value[0]);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_VARCHAR: {
      if (target_type == TOK_T_STRING) {
        (void)(0);  // No casting needed
      }
      break;
    }
    case TOK_T_STRING: {
      if (target_type == TOK_T_CHAR) {
        if (!(col_val->str_value && strlen(col_val->str_value) > 0)) {
          return false;
        }
      } else if (target_type == TOK_T_TEXT || target_type == TOK_T_JSON || target_type == TOK_T_BLOB) {
        return true;
      } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
        char* endptr;
        col_val->int_value = strtoll(col_val->str_value, &endptr, 10);
        if (*endptr != '\0') {
          return false;
        }
      } else if (target_type == TOK_T_FLOAT) {
        char* endptr;
        col_val->float_value = strtof(col_val->str_value, &endptr);
        if (*endptr != '\0') {
          return false;
        }
      } else if (target_type == TOK_T_DOUBLE) {
        char* endptr;
        col_val->double_value = strtod(col_val->str_value, &endptr);
        if (*endptr != '\0') {
          return false;
        }
      } else if (target_type == TOK_T_BOOL) {
        if (strcasecmp(col_val->str_value, "true") == 0 || 
            strcmp(col_val->str_value, "1") == 0) {
          col_val->bool_value = true;
        } else if (strcasecmp(col_val->str_value, "false") == 0 || 
                  strcmp(col_val->str_value, "0") == 0) {
          col_val->bool_value = false;
        } else {
          return false;
        }
      } else if (target_type == TOK_T_INTERVAL) {
        Interval interval = {0, 0, 0}; 
        char* input = col_val->str_value;
        bool valid = false;
        
        if (input[0] == 'P') {
          valid = parse_iso8601_interval(input, &interval);
        } else {
          valid = parse_interval(input, &interval);
        }
        
        if (!valid) {
          return false;
        }
        
        col_val->interval_value = interval;
      } else if (target_type == TOK_T_VARCHAR) {
        LOG_ERROR("Attempting to infer and case VARCHAR with method: RAW, Invalid.");
        return false;
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DATE: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = (int64_t)(col_val->date_value); 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", date_to_string(col_val->date_value));
      } else if (target_type == TOK_T_TIMESTAMP) {
        int y, m, d;
        decode_date(col_val->date_value, &y, &m, &d);

        __dt ts = {
          .year = y,
          .month = m,
          .day = d,
          .hour = 0,
          .minute = 0,
          .second = 0
        };

        col_val->timestamp_value = encode_timestamp(&ts);        
      } else {
        return false;
      } 
      break;
    }
    case TOK_T_TIME: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = (int64_t)(col_val->time_value); 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", time_to_string(col_val->time_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_TIMESTAMP: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = col_val->timestamp_value.timestamp;
      } else if (target_type == TOK_T_TIMESTAMP_TZ) {
        col_val->timestamp_tz_value.timestamp = col_val->timestamp_value.timestamp;
        col_val->timestamp_tz_value.time_zone_offset = 0; // assumes UTC 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", timestamp_to_string(col_val->timestamp_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_TIMESTAMP_TZ: {
      __dt dt;
      decode_timestamp_TZ(col_val->timestamp_tz_value, &dt);

      if (target_type == TOK_T_TIMESTAMP) {
        col_val->timestamp_value.timestamp = col_val->timestamp_tz_value.timestamp;
        col_val->type = TOK_T_TIMESTAMP;
      } else if (target_type == TOK_T_DATE) {
        col_val->date_value = encode_date(dt.year, dt.month, dt.day);
        col_val->type = TOK_T_DATE;
      } else if (target_type == TOK_T_TIME) {
        col_val->time_value = encode_time(dt.hour, dt.minute, dt.second); 
        col_val->type = TOK_T_TIME;
      } else if (target_type == TOK_T_TIME_TZ) {
        col_val->time_tz_value = encode_time_TZ(dt.hour, dt.minute, dt.second, dt.tz_offset);
        col_val->type = TOK_T_TIME_TZ;
      } else if (target_type == TOK_T_DATETIME) {
        col_val->datetime_value = create_datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
        col_val->time_tz_value.time_zone_offset = col_val->timestamp_tz_value.time_zone_offset;
        col_val->type = TOK_T_DATETIME;
      } else if (target_type == TOK_T_DATETIME_TZ) {
        col_val->datetime_tz_value = create_datetime_TZ(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.tz_offset);        
        col_val->type = TOK_T_DATETIME_TZ;
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", timestamp_tz_to_string(col_val->timestamp_tz_value));
        col_val->type = target_type;
      } else {
        return false;
      }
      break;
    }    
    case TOK_T_TIME_TZ: {
      if (target_type == TOK_T_TIME) {
        col_val->time_value = col_val->timestamp_tz_value.timestamp;
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", time_tz_to_string(col_val->time_tz_value));
      } else if (target_type == TOK_T_TIMESTAMP_TZ) {
        int h, m, s;
        decode_time(col_val->date_value, &h, &m, &s);

        __dt ts_tz = {
          .year = 0,
          .month = 0,
          .day = 0,
          .hour = h,
          .minute = m,
          .second = s
        };

        col_val->timestamp_tz_value = encode_timestamp_TZ(&ts_tz, col_val->time_tz_value.time_zone_offset);
      } else {
        return false;
      }
      break;
    }
    case TOK_T_INTERVAL: {
      if (target_type == TOK_T_INT) {
        col_val->int_value = (int64_t)(col_val->interval_value.micros); 
      } else if (target_type == TOK_T_VARCHAR || target_type == TOK_T_TEXT) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", interval_to_string(&col_val->interval_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DATETIME: {
      if (target_type == TOK_T_TIMESTAMP) {
        col_val->timestamp_value = datetime_to_timestamp(col_val->datetime_value);
      } else if (target_type == TOK_T_VARCHAR) { 
        snprintf(col_val->str_value, sizeof(col_val->str_value), "%s", datetime_to_string(col_val->datetime_value));
      } else {
        return false;
      }
      break;
    }
    case TOK_T_DATETIME_TZ: {
      if (target_type == TOK_T_TIMESTAMP_TZ) {
        col_val->timestamp_tz_value = datetime_TZ_to_timestamp_TZ(col_val->datetime_tz_value);
      } else {
        return false;
      }
      break;
    }    
    case TOK_T_TEXT: {
      if (!(target_type == TOK_T_BLOB || target_type == TOK_T_JSON)) return false;
      break;       
    }
    default:
      return false;
  }

  col_val->type = target_type;
  return true;
}

size_t size_from_type(ColumnDefinition* fallback) {
  size_t size = 0;
  uint8_t column_type = fallback->type;

  switch (column_type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
      size = sizeof(int);
      break;
    case TOK_T_BOOL:
      size = sizeof(uint8_t);
      break;
    case TOK_T_FLOAT:
      size = sizeof(float);
      break;
    case TOK_T_DOUBLE:
      size = sizeof(double);
      break;
    case TOK_T_DECIMAL:
      size = sizeof(int) * 2 + MAX_DECIMAL_LEN; 
      break;
    case TOK_T_UUID:
      size = 16;
      break;
    case TOK_T_DATE:
      size = sizeof(Date);
      break;
    case TOK_T_TIME:
      size = sizeof(TimeStored);
      break;
    case TOK_T_TIME_TZ:
      size = sizeof(Time_TZ);
      break;
    case TOK_T_TIMESTAMP:
      size = sizeof(Timestamp);
      break;
    case TOK_T_TIMESTAMP_TZ:
      size = sizeof(Timestamp_TZ);
      break;
    case TOK_T_DATETIME:
      size = sizeof(DateTime);
      break;
    case TOK_T_DATETIME_TZ:
      size = sizeof(DateTime_TZ);
      break;
    case TOK_T_INTERVAL:
      return sizeof(Interval);
    case TOK_T_VARCHAR:
      size = fallback->type_varchar == 0 ? MAX_VARCHAR_SIZE : fallback->type_varchar;
      size += sizeof(uint16_t);
      break;
    case TOK_T_CHAR:
      size = sizeof(uint8_t);
      break;
    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON:
      size = TOAST_CHUNK_SIZE;
      size += sizeof(uint16_t);
      break;
    default:
      break;
  }

  return size;
}

size_t size_from_value(ColumnValue* val, ColumnDefinition* fallback) {
  if (!val) return -1;
  if (val->is_null) {
    return size_from_type(fallback);
  }

  switch (fallback->type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
    case TOK_T_UINT:
      return sizeof(int64_t);
    case TOK_T_BOOL:
      return sizeof(bool);
    case TOK_T_FLOAT:
      return sizeof(float);
    case TOK_T_DOUBLE:
      return sizeof(double);
    case TOK_T_DECIMAL: {
      size_t str_len = strlen(val->decimal.decimal_value);
      return sizeof(int) * 2 + sizeof(uint16_t) + str_len;
    }
    case TOK_T_UUID:
      return 16;
    case TOK_T_DATE:
      return sizeof(Date);
    case TOK_T_TIME:
      return sizeof(TimeStored);
    case TOK_T_TIME_TZ:
      return sizeof(Time_TZ);
    case TOK_T_TIMESTAMP:
      return sizeof(Timestamp);
    case TOK_T_TIMESTAMP_TZ:
      return sizeof(Timestamp_TZ);
    case TOK_T_DATETIME:
      return sizeof(DateTime);
    case TOK_T_DATETIME_TZ:
      return sizeof(DateTime_TZ);
    case TOK_T_INTERVAL:
      return sizeof(Interval);
    case TOK_T_CHAR:
      return sizeof(char);
      break;
    case TOK_T_VARCHAR:
      return strlen(val->str_value) + sizeof(uint16_t);
      break;
    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON:
      if (val->is_toast) {
        return sizeof(bool) + sizeof(uint32_t);
      }
    
      size_t len = strlen(val->str_value);

      return sizeof(uint16_t) + len;
      break;
    default:
      return size_from_type(fallback);
  }
}


uint32_t get_table_offset(Database* db, const char* table_name) {
  for (int i = 0; i < db->table_count; i++) {
    if (strcmp(db->tc[i].name, table_name) == 0) {
      return db->tc[i].offset;
    }
  }
  return 0;  
}

bool column_name_in_list(const char* name, char** list, uint8_t list_len) {
  for (uint8_t i = 0; i < list_len; i++) {
    if (strcmp(name, list[i]) == 0) return true;
  }
  return false;
}

void check_and_concat_toast(Database* db, ColumnValue* value) {
  char* result = toast_concat(db, value->toast_object);

  value->str_value = strdup(result);
  value->is_toast = false;
}

bool check_foreign_key(Database* db, ColumnDefinition def, ColumnValue val) {
  char query[1024];
  char value[300];

  format_column_value(value, sizeof(value), &val);
  snprintf(query, sizeof(query), "SELECT * FROM %s WHERE %s = %s", def.foreign_table, def.foreign_column, value);
  
  LOG_DEBUG("%s", query);
  
  Result res = process(db, query);

  return res.exec.row_count > 0;
}

bool handle_on_update_constraints(Database* db, ColumnDefinition col) {

}

bool handle_on_delete_constraints(Database* db, ColumnDefinition def, ColumnValue val) {
  char query[1024];
  char value[300];

  format_column_value(value, sizeof(value), &val);

  switch (def.on_delete) {
    case FK_CASCADE: {
      snprintf(query, sizeof(query), "DELETE FROM %s WHERE %s = %s", def.foreign_table, def.foreign_column, value);
      
      Result res = process(db, query);
    
      return res.exec.code == 0;   
    }  
    case FK_SET_NULL: {
      snprintf(query, sizeof(query), "UPDATE %s SET %s = NULL WHERE %s = %s",
       def.foreign_table, def.foreign_column, def.foreign_column, value);
      
      Result res = process(db, query);
    
      return res.exec.code == 0;   
    }
    case FK_RESTRICT: {
      LOG_INFO("Did not delete row because of foreign constraint restriction with %s.%s", 
        def.foreign_table, def.foreign_column);

      return false;

    }  
    default:
      return true;
  }

  return true;
}