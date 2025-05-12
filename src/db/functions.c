#include "functions.h"
#include "datetime.h"

FunctionRegistry global_function_registry = {NULL, 0, 0};

void register_function(const char* name, BuiltinFunction func) {
  if (global_function_registry.count >= global_function_registry.capacity) {
    global_function_registry.capacity = global_function_registry.capacity == 0 ? 8 : global_function_registry.capacity * 2;
    global_function_registry.entries = realloc(global_function_registry.entries, global_function_registry.capacity * sizeof(FunctionEntry));
  }

  global_function_registry.entries[global_function_registry.count++] = (FunctionEntry){
    .name = strdup(name),
    .func = func
  };
}

BuiltinFunction find_function(const char* name) {
  for (size_t i = 0; i < global_function_registry.count; ++i) {
    if (strcmp(global_function_registry.entries[i].name, name) == 0) {
      return global_function_registry.entries[i].func;
    }
  }
  return NULL;
}

void free_function_registry() {
  for (size_t i = 0; i < global_function_registry.count; ++i) {
    free(global_function_registry.entries[i].name);
  }
  free(global_function_registry.entries);
  global_function_registry.entries = NULL;
  global_function_registry.count = 0;
  global_function_registry.capacity = 0;
}

void register_builtin_functions() {
  register_function("ABS", fn_abs);
  register_function("ROUND", fn_round);
  register_function("NOW", fn_now);
  register_function("SIN", fn_sin);
  register_function("COS", fn_cos);
  register_function("TAN", fn_tan);
  register_function("LOG", fn_log);
  register_function("POW", fn_pow);
  register_function("CONCAT", fn_concat);
  register_function("SUBSTRING", fn_substring);
  register_function("LENGTH", fn_length);
  register_function("LOWER", fn_lower);
  register_function("UPPER", fn_upper);
  register_function("TRIM", fn_trim);
  register_function("REPLACE", fn_replace);
  register_function("COALESCE", fn_coalesce);
  register_function("CAST", fn_cast);
  register_function("DATE", fn_date);
  register_function("EXTRACT", fn_extract);
  register_function("TIME", fn_time);
  register_function("IFNULL", fn_ifnull);
  register_function("GREATEST", fn_greatest);
  register_function("LEAST", fn_least);
  register_function("RAND", fn_rand);
  register_function("FLOOR", fn_floor);
  register_function("CEIL", fn_ceiling);
  register_function("PI", fn_pi);
  register_function("DEGREES", fn_degrees);
  register_function("RADIANS", fn_radians);
}

ColumnValue evaluate_function(const char* name, ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  BuiltinFunction fn = find_function(name);

  if (!fn) {
    LOG_ERROR("Unknown function was found. Most recent query not executed");
    ColumnValue v; memset(&v, 0, sizeof(ColumnValue)); return v;
  }
  return fn(args, arg_count, row, schema, db, schema_idx);
}


ColumnValue fn_abs(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  if (input.type == TOK_T_INT || input.type == TOK_T_UINT || input.type == TOK_T_SERIAL ) {
    result.int_value = abs(input.int_value);
    result.type = TOK_T_INT;
  } else {
    bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

    if (!valid_conversion) {
      LOG_ERROR("Invalid conversion whilst trying to insert row");
      return result;
    }

    result.double_value = fabs(input.double_value);
    result.type = TOK_T_DOUBLE;
  }

  result.is_null = false;
  return result;
}

ColumnValue fn_round(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = round(input.double_value);
  result.type = TOK_T_DOUBLE;

  result.is_null = false;
  return result;
}


ColumnValue fn_now(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .type = TOK_T_TIMESTAMP_TZ };

  time_t now = time(NULL);
  struct tm local_tm = *localtime(&now);

  __dt dt = {
    .year = local_tm.tm_year + 1900,
    .month = local_tm.tm_mon + 1,
    .day = local_tm.tm_mday,
    .hour = local_tm.tm_hour,
    .minute = local_tm.tm_min,
    .second = local_tm.tm_sec
  };

  int32_t tz_offset = get_timezone_offset(); 

  result.timestamp_tz_value = encode_timestamp_TZ(&dt, tz_offset);
  result.is_null = false;
  return result;
}

ColumnValue fn_sin(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = sin(input.double_value);
  result.type = TOK_T_DOUBLE;

  result.is_null = false;
  return result;
}

ColumnValue fn_cos(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = cos(input.double_value);
  result.type = TOK_T_DOUBLE;

  result.is_null = false;
  return result;
}

ColumnValue fn_tan(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = tan(input.double_value);
  result.type = TOK_T_DOUBLE;

  result.is_null = false;
  return result;
}

ColumnValue fn_log(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = log(input.double_value);
  result.type = TOK_T_DOUBLE;

  result.is_null = false;
  return result;
}

ColumnValue fn_pow(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue base = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue exponent = evaluate_expression(args[1], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };
  
  bool valid_conversion = infer_and_cast_va(2,
    (__c){&base, TOK_T_DOUBLE},
    (__c){&exponent, TOK_T_DOUBLE});

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }


  result.double_value = pow((double)base.double_value, (double)exponent.double_value);
  result.type = TOK_T_DOUBLE;

  result.is_null = false;
  return result;
}

ColumnValue fn_concat(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str1 = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue str2 = evaluate_expression(args[1], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

  bool valid_conversion = infer_and_cast_va(2,
    (__c){&str1, TOK_T_STRING},
    (__c){&str2, TOK_T_STRING});

  if (!valid_conversion) {
    LOG_ERROR("Invalid converqsion whilst trying to insert row");
    return result;
  }

  if (str1.type == TOK_T_STRING && str2.type == TOK_T_STRING) {
    snprintf(result.str_value, MAX_IDENTIFIER_LEN, "%s%s", str1.str_value, str2.str_value);
    result.is_null = false;
  }

  return result;
}


ColumnValue fn_substring(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue start = evaluate_expression(args[1], row, schema, db, schema_idx);
  ColumnValue length = evaluate_expression(args[2], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

  bool valid_conversion = infer_and_cast_va(3, 
    (__c){&str, TOK_T_STRING},
    (__c){&start, TOK_T_UINT},
    (__c){&length, TOK_T_UINT}
  );

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }
  

  if (str.type == TOK_T_STRING && start.type == TOK_T_UINT && length.type == TOK_T_UINT) {
    size_t start_pos = (start.int_value > 0) ? (start.int_value - 1) : 0;
    size_t len = (size_t)length.int_value;

    size_t str_len = strlen(str.str_value);
    if (start_pos >= str_len) {
      result.str_value[0] = '\0'; 
    } else {
      len = (start_pos + len > str_len) ? (str_len - start_pos) : len;
      strncpy(result.str_value, str.str_value + start_pos, len);
      result.str_value[len] = '\0';  
    }

    result.is_null = false;
  }

  return result;
}

ColumnValue fn_length(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_INT };

  bool valid_conversion = infer_and_cast_value_raw(&str, TOK_T_STRING);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }
  

  if (str.type == TOK_T_STRING) {
    result.int_value = strlen(str.str_value);
    result.is_null = false;
  }

  return result;
}

ColumnValue fn_lower(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

  bool valid_conversion = infer_and_cast_value_raw(&str, TOK_T_STRING);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }
  

  if (str.type == TOK_T_STRING) {
    size_t len = strlen(str.str_value);
    for (size_t i = 0; i < len && i < MAX_IDENTIFIER_LEN - 1; i++) {
      result.str_value[i] = tolower(str.str_value[i]);
    }
    result.str_value[len] = '\0';
    result.is_null = false;
  }

  return result;
}

ColumnValue fn_upper(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

  bool valid_conversion = infer_and_cast_value_raw(&str, TOK_T_STRING);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }
  

  if (str.type == TOK_T_STRING) {
    size_t len = strlen(str.str_value);
    for (size_t i = 0; i < len && i < MAX_IDENTIFIER_LEN - 1; i++) {
      result.str_value[i] = toupper(str.str_value[i]);
    }
    result.str_value[len] = '\0';
    result.is_null = false;
  }

  return result;
}

ColumnValue fn_trim(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

  bool valid_conversion = infer_and_cast_value_raw(&str, TOK_T_STRING);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  if (str.type == TOK_T_STRING) {
    const char* start = str.str_value;
    while (*start == ' ') start++;

    const char* end = str.str_value + strlen(str.str_value) - 1;
    while (end > start && *end == ' ') end--;

    size_t len = end - start + 1;
    len = (len >= MAX_IDENTIFIER_LEN) ? (MAX_IDENTIFIER_LEN - 1) : len;

    strncpy(result.str_value, start, len);
    result.str_value[len] = '\0';

    result.is_null = false;
  }

  return result;
}

ColumnValue fn_replace(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue str = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue old_sub = evaluate_expression(args[1], row, schema, db, schema_idx);
  ColumnValue new_sub = evaluate_expression(args[2], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

  bool valid_conversion = infer_and_cast_value_raw(&str, TOK_T_STRING);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  if (str.type == TOK_T_STRING && old_sub.type == TOK_T_STRING && new_sub.type == TOK_T_STRING) {
    const char* pos = strstr(str.str_value, old_sub.str_value);

    if (!pos) {
      strncpy(result.str_value, str.str_value, MAX_IDENTIFIER_LEN - 1);
      result.str_value[MAX_IDENTIFIER_LEN - 1] = '\0';
    } else {
      size_t prefix_len = pos - str.str_value;
      size_t new_len = prefix_len + strlen(new_sub.str_value) + strlen(pos + strlen(old_sub.str_value));

      if (new_len >= MAX_IDENTIFIER_LEN) new_len = MAX_IDENTIFIER_LEN - 1;

      snprintf(result.str_value, MAX_IDENTIFIER_LEN, "%.*s%s%s",
        (int)prefix_len,
        str.str_value,
        new_sub.str_value,
        pos + strlen(old_sub.str_value));
    }

    result.is_null = false;
  }

  return result;
}

ColumnValue fn_coalesce(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .is_null = true };

  for (uint8_t i = 0; i < arg_count; i++) {
    result = evaluate_expression(args[i], row, schema, db, schema_idx);
    if (!result.is_null) {
      break;
    }
  }

  return result;
}

ColumnValue fn_cast(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue type_info = evaluate_expression(args[1], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true };

  if (type_info.type == TOK_T_INT) {
    result.int_value = (int)input.double_value;
    result.type = TOK_T_INT;
  } else if (type_info.type == TOK_T_DOUBLE) {
    result.double_value = (double)input.int_value;
    result.type = TOK_T_DOUBLE;
  }

  result.is_null = false;
  return result;
}

ColumnValue fn_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .type = TOK_T_DATE };

  time_t now = time(NULL);
  struct tm local_tm = *localtime(&now);

  Date date = encode_date(local_tm.tm_year + 1900, local_tm.tm_mon + 1, local_tm.tm_mday);
  result.date_value = date;
  result.is_null = false;

  return result;
}

ColumnValue fn_extract(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .type = TOK_T_INT };
  result.is_null = true;

  if (arg_count != 2) {
    LOG_WARN("EXTRACT() requires exactly 2 arguments");
    return result;
  }

  ColumnValue field_arg = resolve_expr_value(args[0], row, schema, db, schema_idx, NULL);
  if (field_arg.is_null || (field_arg.type != TOK_T_STRING && field_arg.type != TOK_T_VARCHAR)) {
    LOG_WARN("First argument of EXTRACT() must be a non-null string");
    return result;
  }

  ColumnValue time_arg = resolve_expr_value(args[1], row, schema, db, schema_idx, NULL);
  if (time_arg.is_null) {
    LOG_WARN("Second argument of EXTRACT() cannot be NULL");
    return result;
  }

  char field[64] = {0};
  size_t field_len = strlen(field_arg.str_value);
  if (field_len >= sizeof(field)) {
    LOG_WARN("Field name too long in EXTRACT()");
    return result;
  }
  
  for (size_t i = 0; i < field_len; i++) {
    field[i] = toupper(field_arg.str_value[i]);
  }

  int year, month, day, hour = 0, minute = 0, second = 0, ms = 0;
  
  switch (time_arg.type) {
    case TOK_T_DATE:
      decode_date(time_arg.date_value, &year, &month, &day);
      break;
      
    case TOK_T_TIME:
      decode_time(time_arg.time_value, &hour, &minute, &second);
      break;
      
    case TOK_T_DATETIME_TZ: {
      DateTime dt = convert_tz_to_local(time_arg.datetime_tz_value);
      break;
    }
      
    default:
      LOG_WARN("EXTRACT() requires a date, time, or datetime value as second argument");
      return result;
  }

  result.is_null = false;
  
  if (strcmp(field, "YEAR") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract YEAR from TIME value");
    } else {
      result.int_value = year;
    }
  } else if (strcmp(field, "MONTH") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract MONTH from TIME value");
    } else {
      result.int_value = month;
    }
  } else if (strcmp(field, "DAY") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract DAY from TIME value");
    } else {
      result.int_value = day;
    }
  } else if (strcmp(field, "HOUR") == 0) {
    if (time_arg.type == TOK_T_DATE) {
      result.int_value = 0; 
    } else {
      result.int_value = hour;
    }
  } else if (strcmp(field, "MINUTE") == 0) {
    if (time_arg.type == TOK_T_DATE) {
      result.int_value = 0;
    } else {
      result.int_value = minute;
    }
  } else if (strcmp(field, "SECOND") == 0) {
    if (time_arg.type == TOK_T_DATE) {
      result.int_value = 0; 
    } else {
      result.int_value = second;
    }
  } else if (strcmp(field, "MILLISECOND") == 0) {
    if (time_arg.type == TOK_T_DATE) {
      result.int_value = 0;
    } else {
      result.int_value = ms;
    }
  } else if (strcmp(field, "DOW") == 0 || strcmp(field, "DAYOFWEEK") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract DAY OF WEEK from TIME value");
    } else {
      result.int_value = calculate_day_of_week(year, month, day);
    }
  } else if (strcmp(field, "DOY") == 0 || strcmp(field, "DAYOFYEAR") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract DAY OF YEAR from TIME value");
    } else {
      // result.int_value = calculate_day_of_year(year, month, day);
    }
  } else if (strcmp(field, "QUARTER") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract QUARTER from TIME value");
    } else {
      result.int_value = (month - 1) / 3 + 1;
    }
  } else if (strcmp(field, "WEEK") == 0 || strcmp(field, "WEEKOFYEAR") == 0) {
    if (time_arg.type == TOK_T_TIME) {
      result.is_null = true;
      LOG_WARN("Cannot extract WEEK from TIME value");
    } else {
      result.int_value = calculate_week_of_year(year, month, day);
    }
  } else {
    result.is_null = true;
    LOG_WARN("Unknown field '%s' in EXTRACT()", field);
  }

  return result;
}

ColumnValue fn_time(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .type = TOK_T_TIME };

  time_t now = time(NULL);
  struct tm local_tm = *localtime(&now);

  TimeStored time_val = encode_time(local_tm.tm_hour, local_tm.tm_min, local_tm.tm_sec);
  result.time_value = time_val;
  result.is_null = false;

  return result;
}

ColumnValue fn_ifnull(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  
  if (input.is_null) {
    return evaluate_expression(args[1], row, schema, db, schema_idx);
  } else {
    return input;
  }
}

ColumnValue fn_greatest(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = evaluate_expression(args[0], row, schema, db, schema_idx);

  for (uint8_t i = 1; i < arg_count; i++) {
    ColumnValue temp = evaluate_expression(args[i], row, schema, db, schema_idx);
    if (temp.type == TOK_T_INT && result.type == TOK_T_INT) {
      if (temp.int_value > result.int_value) {
        result = temp;
      }
    } else if (temp.type == TOK_T_DOUBLE && result.type == TOK_T_DOUBLE) {
      if (temp.double_value > result.double_value) {
        result = temp;
      }
    }
  }

  return result;
}

ColumnValue fn_least(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = evaluate_expression(args[0], row, schema, db, schema_idx);

  for (uint8_t i = 1; i < arg_count; i++) {
    ColumnValue temp = evaluate_expression(args[i], row, schema, db, schema_idx);
    if (temp.type == TOK_T_INT && result.type == TOK_T_INT) {
      if (temp.int_value < result.int_value) {
        result = temp;
      }
    } else if (temp.type == TOK_T_DOUBLE && result.type == TOK_T_DOUBLE) {
      if (temp.double_value < result.double_value) {
        result = temp;
      }
    }
  }

  return result;
}

ColumnValue fn_rand(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .type = TOK_T_DOUBLE };
  result.double_value = (double)rand() / RAND_MAX;
  result.is_null = false;
  return result;
}

ColumnValue fn_floor(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_DOUBLE };

  if (input.type == TOK_T_DOUBLE) {
    result.double_value = floor(input.double_value);
    result.is_null = false;
  }

  return result;
}

ColumnValue fn_ceiling(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_DOUBLE };

  if (input.type == TOK_T_DOUBLE) {
    result.double_value = ceil(input.double_value);
    result.is_null = false;
  }

  return result;
}

ColumnValue fn_pi(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result = { .type = TOK_T_DOUBLE };
  result.double_value = M_PI;
  result.is_null = false;
  return result;
}

ColumnValue fn_degrees(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_DOUBLE };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = input.double_value * (180.0 / M_PI);
  result.is_null = false;

  return result;
}

ColumnValue fn_radians(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue input = evaluate_expression(args[0], row, schema, db, schema_idx);
  ColumnValue result = { .is_null = true, .type = TOK_T_DOUBLE };

  bool valid_conversion = infer_and_cast_value_raw(&input, TOK_T_DOUBLE);

  if (!valid_conversion) {
    LOG_ERROR("Invalid conversion whilst trying to insert row");
    return result;
  }

  result.double_value = input.double_value * (M_PI / 180.0);
  result.is_null = false;

  return result;
}