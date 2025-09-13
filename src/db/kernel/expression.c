#include "kernel/kernel.h"

ColumnValue create_null_column_value(void) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  result.is_null = true;
  return result;
}

ColumnValue create_bool_column_value(bool value, bool is_null) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  result.type = TOK_T_BOOL;
  result.bool_value = value;
  result.is_null = is_null;
  return result;
}

// --- Literal ---
ColumnValue evaluate_literal_expression(ExprNode* expr, Database* db) {
  ColumnValue* value = &expr->literal;
  if (value->type == TOK_T_STRING && value->str_value && strlen(value->str_value) > TOAST_CHUNK_SIZE) {
    uint32_t toast_id = toast_new_entry(db, value->str_value);
    value->is_toast = true;
    value->type = TOK_T_TEXT;
    value->toast_object = toast_id;
  } else if (value->is_toast) {
    check_and_concat_toast(db, value);
  }
  if (value->is_null) value->type = TOK_NL;
  return *value;
}

// --- Column ---
ColumnValue evaluate_column_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue result = {0};
  int table_idx = table_map[expr->column.table];
  Row* row = tuple->vectors[table_idx];
  TableSchema* schema = schemas[table_idx].ptr;

  if (!row || !schema) {
    LOG_ERROR("Invalid row or schema for table index %d", expr->column.table);
    return result;
  }

  int col_index = find_column_index(schema, expr->column.col_name);
  if (col_index < 0 || col_index >= schema->column_count) {
    LOG_ERROR("Column '%s' not found in schema '%s'", expr->column.col_name, schema->table_name);
    return result;
  }

  ColumnValue col = row->values[col_index];
  if (col.is_toast) check_and_concat_toast(db, &col);
  return col;
}

// --- Array access ---
ColumnValue evaluate_array_access_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue result = {0};
  ColumnValue array_index = evaluate_expression(expr->column.array_idx, tuple, db, table_map, schemas);
  if (array_index.type != TOK_T_INT && array_index.type != TOK_T_UINT) return result;

  ColumnValue col = evaluate_column_expression(expr, tuple, db, table_map, schemas);
  int idx = array_index.int_value;
  int size = col.array.array_size;
  if (idx < 0 || idx >= size) return result;
  return col.array.array_value[idx];
}

// --- Unary ---
ColumnValue evaluate_unary_op_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue result = {0};
  ColumnDefinition defn;
  ColumnValue operand = resolve_expr_value(expr->arth_unary.expr, tuple, db, table_map, schemas, &defn);
  result.type = defn.type;

  switch (expr->arth_unary.op) {
    case TOK_SUB:
      if (operand.type == TOK_T_INT || operand.type == TOK_T_UINT) result.int_value = -operand.int_value;
      else if (operand.type == TOK_T_FLOAT || operand.type == TOK_T_DOUBLE) result.double_value = -operand.double_value;
      else { LOG_ERROR("Unary minus not supported on type %d", operand.type); result.type = operand.type; }
      break;
    default: LOG_WARN("Unsupported unary operation: %d", expr->arth_unary.op); break;
  }
  return result;
}

// --- Binary ---
ColumnValue evaluate_binary_op_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue result = {0};
  ColumnDefinition defn;
  ColumnValue left = resolve_expr_value(expr->binary.left, tuple, db, table_map, schemas, &defn);
  ColumnValue right = resolve_expr_value(expr->binary.right, tuple, db, table_map, schemas, &defn);
  if (left.is_null || right.is_null) { result.is_null = true; return result; }

  switch (defn.type) {
    case TOK_T_INT: case TOK_T_UINT: case TOK_T_SERIAL: case TOK_T_FLOAT: case TOK_T_DOUBLE:
      return evaluate_numeric_binary_op(left, right, expr->binary.op);
    default: LOG_WARN("Unsupported binary expression type: %d", defn.type); return result;
  }
}

// --- Numeric Binary Op ---
ColumnValue evaluate_numeric_binary_op(ColumnValue left, ColumnValue right, int op) {
  ColumnValue result = {0};
  if (!infer_and_cast_va(2, (__c){&left, TOK_T_DOUBLE}, (__c){&right, TOK_T_DOUBLE})) return result;
  result.type = TOK_T_DOUBLE;
  switch (op) {
    case TOK_ADD: result.double_value = left.double_value + right.double_value; break;
    case TOK_SUB: result.double_value = left.double_value - right.double_value; break;
    case TOK_MUL: result.double_value = left.double_value * right.double_value; break;
    case TOK_DIV:
      if (right.double_value == 0.0) result.is_null = true;
      else result.double_value = left.double_value / right.double_value;
      break;
    default: result.is_null = true; break;
  }
  return result;
}

// --- Comparison ---
ColumnValue evaluate_comparison_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnDefinition defn;
  ColumnValue left = resolve_expr_value(expr->binary.left, tuple, db, table_map, schemas, &defn);
  ColumnValue right = resolve_expr_value(expr->binary.right, tuple, db, table_map, schemas, &defn);
  if (!infer_and_cast_value(&left, &defn) || !infer_and_cast_value(&right, &defn)) return create_null_column_value();

  int cmp = key_compare(get_column_value_as_pointer(&left), get_column_value_as_pointer(&right), defn.type);
  bool val = false;
  switch (expr->binary.op) {
    case TOK_EQ: val = cmp == 0; break;
    case TOK_NE: val = cmp != 0; break;
    case TOK_LT: val = cmp < 0; break;
    case TOK_GT: val = cmp > 0; break;
    case TOK_LE: val = cmp <= 0; break;
    case TOK_GE: val = cmp >= 0; break;
  }
  return create_bool_column_value(val, left.is_null || right.is_null);
}

// --- LIKE ---
ColumnValue evaluate_like_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnDefinition defn;
  ColumnValue left = resolve_expr_value(expr->like.left, tuple, db, table_map, schemas, &defn);
  if (left.type != TOK_T_VARCHAR || !left.str_value) return create_bool_column_value(false, false);
  bool res = like_match(left.str_value, expr->like.pattern);
  return create_bool_column_value(res, left.is_null);
}

// --- BETWEEN ---
ColumnValue evaluate_between_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnDefinition defn;
  ColumnValue val = resolve_expr_value(expr->between.value, tuple, db, table_map, schemas, &defn);
  ColumnValue lower = resolve_expr_value(expr->between.lower, tuple, db, table_map, schemas, &defn);
  ColumnValue upper = resolve_expr_value(expr->between.upper, tuple, db, table_map, schemas, &defn);
  if (!infer_and_cast_va(3, (__c){&val, TOK_T_DOUBLE}, (__c){&lower, TOK_T_DOUBLE}, (__c){&upper, TOK_T_DOUBLE})) return create_null_column_value();
  bool res = val.double_value >= lower.double_value && val.double_value <= upper.double_value;
  return create_bool_column_value(res, val.is_null || lower.is_null || upper.is_null);
}

// --- IN ---
ColumnValue evaluate_in_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnDefinition defn;
  ColumnValue val = resolve_expr_value(expr->in.value, tuple, db, table_map, schemas, &defn);
  for (size_t i = 0; i < expr->in.count; i++) {
    ColumnValue item = resolve_expr_value(expr->in.list[i], tuple, db, table_map, schemas, &defn);
    bool match = false;
    if (val.type == TOK_T_INT || val.type == TOK_T_UINT || val.type == TOK_T_SERIAL) match = val.int_value == item.int_value;
    else if (val.type == TOK_T_VARCHAR && val.str_value && item.str_value) match = strcmp(val.str_value, item.str_value) == 0;
    if (match) return create_bool_column_value(true, val.is_null || item.is_null);
  }
  return create_bool_column_value(false, val.is_null);
}

// --- Logical ---
ColumnValue evaluate_logical_and_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue left = evaluate_expression(expr->binary.left, tuple, db, table_map, schemas);
  if (!left.bool_value && !left.is_null) return create_bool_column_value(false, false);
  ColumnValue right = evaluate_expression(expr->binary.right, tuple, db, table_map, schemas);
  return create_bool_column_value(left.bool_value && right.bool_value, left.is_null || right.is_null);
}

ColumnValue evaluate_logical_or_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue left = evaluate_expression(expr->binary.left, tuple, db, table_map, schemas);
  if (left.bool_value && !left.is_null) return create_bool_column_value(true, false);
  ColumnValue right = evaluate_expression(expr->binary.right, tuple, db, table_map, schemas);
  return create_bool_column_value(left.bool_value || right.bool_value, left.is_null || right.is_null);
}

ColumnValue evaluate_logical_not_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  ColumnValue operand = evaluate_expression(expr->unary, tuple, db, table_map, schemas);
  return create_bool_column_value(!operand.bool_value, operand.is_null);
}

// --- Datetime ---
ColumnValue evaluate_datetime_binary_op(ColumnValue left, ColumnValue right, int op) {
  ColumnValue result = {0};
  if (left.type == TOK_T_INTERVAL && (right.type == TOK_T_DATETIME || right.type == TOK_T_TIMESTAMP ||
                                      right.type == TOK_T_DATETIME_TZ || right.type == TOK_T_TIMESTAMP_TZ)) {
    ColumnValue tmp = left;
    left = right;
    right = tmp;
  }

  switch (left.type) {
    case TOK_T_DATETIME:
      if (right.type == TOK_T_INTERVAL) {
        result.type = TOK_T_DATETIME;
        if (op == TOK_ADD) result.datetime_value = add_interval_to_datetime(left.datetime_value, right.interval_value);
        else if (op == TOK_SUB) result.datetime_value = subtract_interval_from_datetime(left.datetime_value, right.interval_value);
        else result.is_null = true;
      } else if (right.type == TOK_T_DATETIME && op == TOK_SUB) {
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(left.datetime_value, right.datetime_value);
      } else result.is_null = true;
      break;
    case TOK_T_TIMESTAMP:
      if (right.type == TOK_T_INTERVAL) {
        DateTime dt = timestamp_to_datetime(left.timestamp_value);
        if (op == TOK_ADD) dt = add_interval_to_datetime(dt, right.interval_value);
        else if (op == TOK_SUB) dt = subtract_interval_from_datetime(dt, right.interval_value);
        else { result.is_null = true; break; }
        result.type = TOK_T_TIMESTAMP;
        result.timestamp_value = datetime_to_timestamp(dt);
      } else if (right.type == TOK_T_TIMESTAMP && op == TOK_SUB) {
        DateTime a = timestamp_to_datetime(left.timestamp_value);
        DateTime b = timestamp_to_datetime(right.timestamp_value);
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(a, b);
      } else result.is_null = true;
      break;
    case TOK_T_DATETIME_TZ:
      if (right.type == TOK_T_INTERVAL) {
        result.type = TOK_T_DATETIME_TZ;
        if (op == TOK_ADD) result.datetime_tz_value = add_interval_to_datetime_TZ(left.datetime_tz_value, right.interval_value);
        else if (op == TOK_SUB) result.datetime_tz_value = subtract_interval_from_datetime_TZ(left.datetime_tz_value, right.interval_value);
        else result.is_null = true;
      } else if (right.type == TOK_T_DATETIME_TZ && op == TOK_SUB) {
        DateTime a = convert_tz_to_local(left.datetime_tz_value);
        DateTime b = convert_tz_to_local(right.datetime_tz_value);
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(a, b);
      } else result.is_null = true;
      break;
    case TOK_T_TIMESTAMP_TZ:
      if (right.type == TOK_T_INTERVAL) {
        DateTime_TZ base = timestamp_TZ_to_datetime_TZ(left.timestamp_tz_value);
        DateTime local = {base.year, base.month, base.day, base.hour, base.minute, base.second};
        if (op == TOK_ADD) local = add_interval_to_datetime(local, right.interval_value);
        else if (op == TOK_SUB) local = subtract_interval_from_datetime(local, right.interval_value);
        else { result.is_null = true; break; }
        result.type = TOK_T_TIMESTAMP_TZ;
        result.timestamp_tz_value = datetime_TZ_to_timestamp_TZ((DateTime_TZ){local.year, local.month, local.day,
                                                                             local.hour, local.minute, local.second,
                                                                             left.timestamp_tz_value.time_zone_offset});
      } else if (right.type == TOK_T_TIMESTAMP_TZ && op == TOK_SUB) {
        DateTime_TZ a = timestamp_TZ_to_datetime_TZ(left.timestamp_tz_value);
        DateTime_TZ b = timestamp_TZ_to_datetime_TZ(right.timestamp_tz_value);
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(convert_tz_to_local(a), convert_tz_to_local(b));
      } else result.is_null = true;
      break;
    default: result.is_null = true; break;
  }

  return result;
}

// --- Resolve & Evaluate ---
ColumnValue resolve_expr_value(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas, ColumnDefinition* out_defn) {
  ColumnValue val = {0};
  if (expr->type == EXPR_COLUMN) {
    int table_id = table_map[expr->column.table];
    TableSchema* schema = schemas[table_id].ptr;
    Row* row = tuple->vectors[table_id];
    int col_index = find_column_index(schema, expr->column.col_name);
    if (col_index < 0 || !row) return val;
    val = row->values[col_index];
    if (out_defn) *out_defn = schema->columns[col_index];
  } else {
    val = evaluate_expression(expr, tuple, db, table_map, schemas);
  }
  return val;
}

ColumnValue evaluate_expression(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  if (!expr) return (ColumnValue){0};
  switch (expr->type) {
    case EXPR_LITERAL: return evaluate_literal_expression(expr, db);
    case EXPR_COLUMN: return evaluate_column_expression(expr, tuple, db, table_map, schemas);
    case EXPR_ARRAY_ACCESS: return evaluate_array_access_expression(expr, tuple, db, table_map, schemas);
    case EXPR_UNARY_OP: return evaluate_unary_op_expression(expr, tuple, db, table_map, schemas);
    case EXPR_BINARY_OP: return evaluate_binary_op_expression(expr, tuple, db, table_map, schemas);
    case EXPR_COMPARISON: return evaluate_comparison_expression(expr, tuple, db, table_map, schemas);
    case EXPR_LIKE: return evaluate_like_expression(expr, tuple, db, table_map, schemas);
    case EXPR_BETWEEN: return evaluate_between_expression(expr, tuple, db, table_map, schemas);
    case EXPR_IN: return evaluate_in_expression(expr, tuple, db, table_map, schemas);
    case EXPR_LOGICAL_AND: return evaluate_logical_and_expression(expr, tuple, db, table_map, schemas);
    case EXPR_LOGICAL_OR: return evaluate_logical_or_expression(expr, tuple, db, table_map, schemas);
    case EXPR_LOGICAL_NOT: return evaluate_logical_not_expression(expr, tuple, db, table_map, schemas);
    default: return (ColumnValue){0};
  }
}

bool evaluate_condition(ExprNode* expr, Tuple* tuple, Database* db, int* table_map, TableSchema** schemas) {
  if (!expr) return false;
  ColumnValue val = evaluate_expression(expr, tuple, db, table_map, schemas);
  return val.bool_value && !val.is_null;
}
