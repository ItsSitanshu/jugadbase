#include "functions.h"

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
  // register_function("ABS", fn_abs);
  // register_function("ROUND", fn_round);
  // // register_function("NOW", fn_now);
  // register_function("SIN", fn_sin);
  // register_function("COS", fn_cos);
  // register_function("TAN", fn_tan);
  // register_function("LOG", fn_log);
  // register_function("POW", fn_pow);
  // register_function("CONCAT", fn_concat);
  // register_function("SUBSTRING", fn_substring);
  // register_function("LENGTH", fn_length);
  // register_function("LOWER", fn_lower);
  // register_function("UPPER", fn_upper);
  // register_function("TRIM", fn_trim);
  // register_function("REPLACE", fn_replace);
  // register_function("COALESCE", fn_coalesce);
  // register_function("CAST", fn_cast);
  // // register_function("DATE", fn_date);
  // // register_function("TIME", fn_time);
  // register_function("IFNULL", fn_ifnull);
  // register_function("GREATEST", fn_greatest);
  // register_function("LEAST", fn_least);
  // register_function("RAND", fn_rand);
  // register_function("FLOOR", fn_floor);
  // register_function("CEIL", fn_ceiling);
  // register_function("PI", fn_pi);
  // register_function("DEGREES", fn_degrees);
  // register_function("RADIANS", fn_radians);
  // register_function("EXTRACT", fn_extract);
  // register_function("STR_TO_DATE", fn_str_to_date);
}

ColumnValue evaluate_function(const char* name, ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
  BuiltinFunction fn = find_function(name);
  if (!fn) {
    REPORT_ERROR(NULL, "SYE_E_UNKNOWN_FUNCTION");
    ColumnValue v; memset(&v, 0, sizeof(ColumnValue)); return v;
  }
  return fn(args, arg_count, row, schema);
}


// ColumnValue fn_abs(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_INT) {
//     result.int_value = abs(input.int_value);
//   } else if (input.type == TOK_T_DOUBLE) {
//     result.double_value = fabs(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_round(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = round(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_now(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = { .type = TOK_T_DATETIME };
//   time_t now = time(NULL);
//   result.datetime_value = *localtime(&now);
//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_sin(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = sin(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_cos(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = cos(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_tan(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = tan(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_log(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = log(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_pow(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue base = evaluate_expression(args[0], row, schema);
//   ColumnValue exponent = evaluate_expression(args[1], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (base.type == TOK_T_DOUBLE && exponent.type == TOK_T_DOUBLE) {
//     result.double_value = pow(base.double_value, exponent.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_concat(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str1 = evaluate_expression(args[0], row, schema);
//   ColumnValue str2 = evaluate_expression(args[1], row, schema);
//   ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

//   if (str1.type == TOK_T_STRING && str2.type == TOK_T_STRING) {
//     snprintf(result.str_value, MAX_IDENTIFIER_LEN, "%s%s", str1.str_value, str2.str_value);
//     result.is_null = false;
//   }

//   return result;
// }


// ColumnValue fn_substring(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str = evaluate_expression(args[0], row, schema);
//   ColumnValue start = evaluate_expression(args[1], row, schema);
//   ColumnValue length = evaluate_expression(args[2], row, schema);
//   ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

//   if (str.type == TOK_T_STRING && start.type == TOK_T_INT && length.type == TOK_T_INT) {
//     size_t start_pos = (start.int_value > 0) ? (start.int_value - 1) : 0;
//     size_t len = (size_t)length.int_value;

//     size_t str_len = strlen(str.str_value);
//     if (start_pos >= str_len) {
//       result.str_value[0] = '\0'; 
//     } else {
//       len = (start_pos + len > str_len) ? (str_len - start_pos) : len;
//       strncpy(result.str_value, str.str_value + start_pos, len);
//       result.str_value[len] = '\0';  
//     }

//     result.is_null = false;
//   }

//   return result;
// }

// ColumnValue fn_length(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (str.type == TOK_T_STRING) {
//     result.int_value = strlen(str.str_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_lower(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

//   if (str.type == TOK_T_STRING) {
//     size_t len = strlen(str.str_value);
//     for (size_t i = 0; i < len && i < MAX_IDENTIFIER_LEN - 1; i++) {
//       result.str_value[i] = tolower(str.str_value[i]);
//     }
//     result.str_value[len] = '\0';
//     result.is_null = false;
//   }

//   return result;
// }

// ColumnValue fn_upper(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

//   if (str.type == TOK_T_STRING) {
//     size_t len = strlen(str.str_value);
//     for (size_t i = 0; i < len && i < MAX_IDENTIFIER_LEN - 1; i++) {
//       result.str_value[i] = toupper(str.str_value[i]);
//     }
//     result.str_value[len] = '\0';
//     result.is_null = false;
//   }

//   return result;
// }

// ColumnValue fn_trim(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

//   if (str.type == TOK_T_STRING) {
//     const char* start = str.str_value;
//     while (*start == ' ') start++;

//     const char* end = str.str_value + strlen(str.str_value) - 1;
//     while (end > start && *end == ' ') end--;

//     size_t len = end - start + 1;
//     len = (len >= MAX_IDENTIFIER_LEN) ? (MAX_IDENTIFIER_LEN - 1) : len;

//     strncpy(result.str_value, start, len);
//     result.str_value[len] = '\0';

//     result.is_null = false;
//   }

//   return result;
// }

// ColumnValue fn_replace(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue str = evaluate_expression(args[0], row, schema);
//   ColumnValue old_sub = evaluate_expression(args[1], row, schema);
//   ColumnValue new_sub = evaluate_expression(args[2], row, schema);
//   ColumnValue result = { .is_null = true, .type = TOK_T_STRING };

//   if (str.type == TOK_T_STRING && old_sub.type == TOK_T_STRING && new_sub.type == TOK_T_STRING) {
//     const char* pos = strstr(str.str_value, old_sub.str_value);

//     if (!pos) {
//       strncpy(result.str_value, str.str_value, MAX_IDENTIFIER_LEN - 1);
//       result.str_value[MAX_IDENTIFIER_LEN - 1] = '\0';
//     } else {
//       size_t prefix_len = pos - str.str_value;
//       size_t new_len = prefix_len + strlen(new_sub.str_value) + strlen(pos + strlen(old_sub.str_value));

//       if (new_len >= MAX_IDENTIFIER_LEN) new_len = MAX_IDENTIFIER_LEN - 1;

//       snprintf(result.str_value, MAX_IDENTIFIER_LEN, "%.*s%s%s",
//         (int)prefix_len,
//         str.str_value,
//         new_sub.str_value,
//         pos + strlen(old_sub.str_value));
//     }

//     result.is_null = false;
//   }

//   return result;
// }

// ColumnValue fn_coalesce(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = { .is_null = true };

//   for (uint8_t i = 0; i < arg_count; i++) {
//     result = evaluate_expression(args[i], row, schema);
//     if (result.type != TOK_NL) {
//       break;
//     }
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_cast(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (args[1]->type == TOK_T_INT) {
//     result.int_value = (int)input.double_value;
//   } else if (args[1]->type == TOK_T_DOUBLE) {
//     result.double_value = (double)input.int_value;
//   }

//   result.is_null = false;
//   return result;
// }

// // ColumnValue fn_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
// //   ColumnValue input = evaluate_expression(args[0], row, schema);
// //   ColumnValue result = { .type = TOK_T_DATETIME };

// //   if (input.type == TOK_T_STRING) {
// //     result.datetime_value.tm_year = 2025; 
// //   }

// //   result.is_null = false;
// //   return result;
// // }

// // ColumnValue fn_time(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
// //   ColumnValue input = evaluate_expression(args[0], row, schema);
// //   ColumnValue result = { .type = TOK_T_DATETIME };

// //   if (input.type == TOK_T_STRING) {
// //     result.datetime_value.tm_hour = 12; 
// //   }

// //   result.is_null = false;
// //   return result;
// // }

// ColumnValue fn_ifnull(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .type = TOK_NL };

//   if (input.is_null) {
//     result = evaluate_expression(args[1], row, schema);
//   } else {
//     result = input;
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_greatest(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = evaluate_expression(args[0], row, schema);

//   for (uint8_t i = 1; i < arg_count; i++) {
//     ColumnValue temp = evaluate_expression(args[i], row, schema);
//     if (temp.type == TOK_T_INT) {
//       if (temp.int_value > result.int_value) {
//         result = temp;
//       }
//     } else if (temp.type == TOK_T_DOUBLE) {
//       if (temp.double_value > result.double_value) {
//         result = temp;
//       }
//     }
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_least(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = evaluate_expression(args[0], row, schema);

//   for (uint8_t i = 1; i < arg_count; i++) {
//     ColumnValue temp = evaluate_expression(args[i], row, schema);
//     if (temp.type == TOK_T_INT) {
//       if (temp.int_value < result.int_value) {
//         result = temp;
//       }
//     } else if (temp.type == TOK_T_DOUBLE) {
//       if (temp.double_value < result.double_value) {
//         result = temp;
//       }
//     }
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_rand(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = { .type = TOK_T_DOUBLE };
//   result.double_value = (double)rand() / RAND_MAX;
//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_floor(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = floor(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_ceiling(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = ceil(input.double_value);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_pi(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = { .type = TOK_T_DOUBLE };
//   result.double_value = M_PI;
//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_degrees(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = input.double_value * (180.0 / M_PI);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_radians(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue input = evaluate_expression(args[0], row, schema);
//   ColumnValue result = { .is_null = true };

//   if (input.type == TOK_T_DOUBLE) {
//     result.double_value = input.double_value * (M_PI / 180.0);
//   }

//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_extract(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = { .is_null = true };
//   result.is_null = false;
//   return result;
// }

// ColumnValue fn_str_to_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema) {
//   ColumnValue result = { .type = TOK_T_DATETIME };
//   result.is_null = false;
//   return result;
// }
