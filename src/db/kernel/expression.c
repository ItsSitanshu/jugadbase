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

ColumnValue evaluate_literal_expression(ExprNode* expr, Database* db) {
  ColumnValue* value = &expr->literal;
  
  if (value->type == TOK_T_STRING && value->str_value && 
      strlen(value->str_value) > TOAST_CHUNK_SIZE) {
    uint32_t toast_id = toast_new_entry(db, value->str_value);
    value->is_toast = true;
    value->type = TOK_T_TEXT;
    value->toast_object = toast_id;
  } else if (value->is_toast) {
    check_and_concat_toast(db, value);
  }
  
  if (value->is_null) {
    value->type = TOK_NL;
  }
  
  return *value;
}

ColumnValue evaluate_column_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  
  result.column.index = expr->column.index;
  
  if (!row || expr->column.index >= schema->column_count) {
    LOG_ERROR("Invalid column access: index %d, schema column count %d", 
              expr->column.index, schema->column_count);
    result.type = (expr->column.index < schema->column_count) ? 
                 schema->columns[expr->column.index].type : TOK_NL;
    return result;
  }
  
  ColumnValue col = row->values[expr->column.index];
  
  if (col.is_toast) {
    check_and_concat_toast(db, &col);
  }
  
  return col;
}

ColumnValue evaluate_array_access_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                             Database* db, uint8_t schema_idx) {
  ColumnValue result = {0};
  
  if (!expr || expr->type != EXPR_ARRAY_ACCESS) {
    LOG_ERROR("Invalid array access expression");
    return result;
  }
  
  if (expr->column.index >= schema->column_count) {
    LOG_ERROR("Column index %d out of bounds", expr->column.index);
    return result;
  }
  
  if (!schema->columns[expr->column.index].is_array) {
    LOG_ERROR("Attempted to index into non-array type");
    return result;
  }
  
  ColumnValue array_index = evaluate_expression(expr->column.array_idx, row, schema, db, schema_idx);
  
  if (array_index.type != TOK_T_INT && array_index.type != TOK_T_UINT) {
    LOG_ERROR("Array index must be an integer");
    return result;
  }
  
  int idx = array_index.int_value;
  int array_size = row->values[expr->column.index].array.array_size;
  
  if (idx < 0 || idx >= array_size) {
    LOG_DEBUG("Array index out of bounds: %d (size: %d)", idx, array_size);
    return result;
  }
  
  result = row->values[expr->column.index].array.array_value[idx];
  return result;
}

ColumnValue evaluate_unary_op_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                         Database* db, uint8_t schema_idx) {
  ColumnValue result;
  ColumnDefinition defn;
  memset(&result, 0, sizeof(ColumnValue));
  
  ColumnValue operand = resolve_expr_value(expr->arth_unary.expr, row, schema, db, schema_idx, &defn);
  result.type = defn.type;
  
  switch (expr->arth_unary.op) {
    case TOK_SUB:
      if (operand.type == TOK_T_INT || operand.type == TOK_T_UINT) {
        result.type = TOK_T_INT;
        result.int_value = -operand.int_value;
      } else if (operand.type == TOK_T_FLOAT || operand.type == TOK_T_DOUBLE) {
        result.type = TOK_T_DOUBLE;
        result.double_value = -operand.double_value;
      } else {
        LOG_ERROR("Unary minus not supported on type %d", operand.type);
        result.type = operand.type;
      }
      break;
      
    default:
      LOG_WARN("Unsupported unary operation: %d", expr->arth_unary.op);
      memset(&result, 0, sizeof(ColumnValue));
      break;
  }
  
  return result;
}

ColumnValue evaluate_binary_op_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                          Database* db, uint8_t schema_idx) {
  ColumnValue result;
  ColumnDefinition defn;
  memset(&result, 0, sizeof(ColumnValue));
  
  ColumnValue left = resolve_expr_value(expr->binary.left, row, schema, db, schema_idx, &defn);
  ColumnValue right = resolve_expr_value(expr->binary.right, row, schema, db, schema_idx, &defn);
  
  if (left.is_null || right.is_null) {
    result.is_null = true;
    return result;
  }
  
  switch (defn.type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
    case TOK_T_FLOAT:
    case TOK_T_DOUBLE:
      return evaluate_numeric_binary_op(left, right, expr->binary.op);
      
    case TOK_T_INTERVAL:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
    case TOK_T_DATETIME_TZ:
    case TOK_T_TIMESTAMP_TZ:
      return evaluate_datetime_binary_op(left, right, expr->binary.op);
      
    default:
      LOG_DEBUG("Unsupported binary expression type: %d", defn.type);
      return result;
  }
}

ColumnValue evaluate_numeric_binary_op(ColumnValue left, ColumnValue right, int op) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  
  bool valid_conversion = infer_and_cast_va(2,
    (__c){&left, TOK_T_DOUBLE},
    (__c){&right, TOK_T_DOUBLE}
  );
  
  if (!valid_conversion) {
    LOG_ERROR("Failed to convert operands to numeric types");
    return result;
  }
  
  result.type = TOK_T_DOUBLE;
  
  switch (op) {
    case TOK_ADD:
      result.double_value = left.double_value + right.double_value;
      break;
    case TOK_SUB:
      result.double_value = left.double_value - right.double_value;
      break;
    case TOK_MUL:
      result.double_value = left.double_value * right.double_value;
      break;
    case TOK_DIV:
      if (right.double_value == 0.0) {
        LOG_ERROR("Division by zero");
        result.is_null = true;
      } else {
        result.double_value = left.double_value / right.double_value;
      }
      break;
    default:
      LOG_WARN("Invalid binary operation: %d", op);
      result.is_null = true;
      break;
  }
  
  return result;
}

ColumnValue evaluate_datetime_binary_op(ColumnValue left, ColumnValue right, int op) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  
  if (left.type == TOK_T_INTERVAL && 
      (right.type == TOK_T_DATETIME || right.type == TOK_T_TIMESTAMP || 
       right.type == TOK_T_DATETIME_TZ || right.type == TOK_T_TIMESTAMP_TZ)) {
    ColumnValue tmp = left;
    left = right;
    right = tmp;
  }
  
  switch (left.type) {
    case TOK_T_DATETIME:
      if (right.type == TOK_T_INTERVAL) {
        result.type = TOK_T_DATETIME;
        if (op == TOK_ADD) {
          result.datetime_value = add_interval_to_datetime(left.datetime_value, right.interval_value);
        } else if (op == TOK_SUB) {
          result.datetime_value = subtract_interval_from_datetime(left.datetime_value, right.interval_value);
        } else {
          LOG_WARN("Unsupported DATETIME operation: %d", op);
          result.is_null = true;
        }
      } else if (right.type == TOK_T_DATETIME && op == TOK_SUB) {
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(left.datetime_value, right.datetime_value);
      } else {
        LOG_WARN("Invalid DATETIME operation: %d with right type: %d", op, right.type);
        result.is_null = true;
      }
      break;
      
    case TOK_T_TIMESTAMP:
      if (right.type == TOK_T_INTERVAL) {
        DateTime dt = timestamp_to_datetime(left.timestamp_value);
        if (op == TOK_ADD) {
          dt = add_interval_to_datetime(dt, right.interval_value);
        } else if (op == TOK_SUB) {
          dt = subtract_interval_from_datetime(dt, right.interval_value);
        } else {
          LOG_WARN("Unsupported TIMESTAMP operation: %d", op);
          result.is_null = true;
          break;
        }
        result.type = TOK_T_TIMESTAMP;
        result.timestamp_value = datetime_to_timestamp(dt);
      } else if (right.type == TOK_T_TIMESTAMP && op == TOK_SUB) {
        DateTime a = timestamp_to_datetime(left.timestamp_value);
        DateTime b = timestamp_to_datetime(right.timestamp_value);
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(a, b);
      } else {
        LOG_WARN("Invalid TIMESTAMP operation: %d with right type: %d", op, right.type);
        result.is_null = true;
      }
      break;
      
    case TOK_T_DATETIME_TZ:
      if (right.type == TOK_T_INTERVAL) {
        result.type = TOK_T_DATETIME_TZ;
        if (op == TOK_ADD) {
          result.datetime_tz_value = add_interval_to_datetime_TZ(left.datetime_tz_value, right.interval_value);
        } else if (op == TOK_SUB) {
          result.datetime_tz_value = subtract_interval_from_datetime_TZ(left.datetime_tz_value, right.interval_value);
        } else {
          LOG_WARN("Unsupported DATETIME_TZ operation: %d", op);
          result.is_null = true;
        }
      } else if (right.type == TOK_T_DATETIME_TZ && op == TOK_SUB) {
        DateTime a = convert_tz_to_local(left.datetime_tz_value);
        DateTime b = convert_tz_to_local(right.datetime_tz_value);
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(a, b);
      } else {
        LOG_WARN("Invalid DATETIME_TZ operation: %d with right type: %d", op, right.type);
        result.is_null = true;
      }
      break;
      
    case TOK_T_TIMESTAMP_TZ:
      if (right.type == TOK_T_INTERVAL) {
        DateTime_TZ base = timestamp_TZ_to_datetime_TZ(left.timestamp_tz_value);
        DateTime local = {
          base.year, base.month, base.day,
          base.hour, base.minute, base.second
        };
        
        if (op == TOK_ADD) {
          local = add_interval_to_datetime(local, right.interval_value);
        } else if (op == TOK_SUB) {
          local = subtract_interval_from_datetime(local, right.interval_value);
        } else {
          LOG_WARN("Unsupported TIMESTAMP_TZ operation: %d", op);
          result.is_null = true;
          break;
        }
        
        result.type = TOK_T_TIMESTAMP_TZ;
        result.timestamp_tz_value = datetime_TZ_to_timestamp_TZ((DateTime_TZ){
          local.year, local.month, local.day,
          local.hour, local.minute, local.second,
          left.timestamp_tz_value.time_zone_offset
        });
      } else if (right.type == TOK_T_TIMESTAMP_TZ && op == TOK_SUB) {
        DateTime_TZ a = timestamp_TZ_to_datetime_TZ(left.timestamp_tz_value);
        DateTime_TZ b = timestamp_TZ_to_datetime_TZ(right.timestamp_tz_value);
        DateTime da = convert_tz_to_local(a);
        DateTime db = convert_tz_to_local(b);
        result.type = TOK_T_INTERVAL;
        result.interval_value = datetime_diff(da, db);
      } else {
        LOG_WARN("Invalid TIMESTAMP_TZ operation: %d with right type: %d", op, right.type);
        result.is_null = true;
      }
      break;
      
    default:
      LOG_WARN("Unsupported datetime operand type: %d", left.type);
      result.is_null = true;
      break;
  }
  
  return result;
}

ColumnValue evaluate_comparison_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                          Database* db, uint8_t schema_idx) {
  ColumnDefinition defn;
  
  ColumnValue left = resolve_expr_value(expr->binary.left, row, schema, db, schema_idx, &defn);
  ColumnValue right = resolve_expr_value(expr->binary.right, row, schema, db, schema_idx, &defn);
  
  if (expr->binary.op == TOK_EQ &&
      expr->binary.left->type == EXPR_COLUMN &&
      expr->binary.right->type == EXPR_LITERAL &&
      expr->binary.left->column.index < schema->column_count &&
      schema->columns[expr->binary.left->column.index].is_primary_key && 
      db && row) {
      
    void* key = get_column_value_as_pointer(&right);
    uint8_t btree_idx = hash_fnv1a(schema->columns[expr->binary.left->column.index].name, MAX_COLUMNS);
    RowID rid = btree_search(db->tc[schema_idx].btree[btree_idx], key);
    
    bool match = (!is_struct_zeroed(&rid, sizeof(RowID)) &&
                  row->id.page_id == rid.page_id &&
                  row->id.row_id == rid.row_id);
    
    return create_bool_column_value(match, false);
  }
  
  if (expr->binary.op == TOK_EQ &&
      expr->binary.left->type == EXPR_COLUMN &&
      expr->binary.right->type == EXPR_LITERAL &&
      expr->binary.right->literal.is_null) {
    return create_bool_column_value(left.is_null, false);
  }
  
  bool valid_conversion = infer_and_cast_value(&left, &defn) && 
                          infer_and_cast_value(&right, &defn);
  
  if (!valid_conversion) {
    return create_null_column_value();
  }
  
  int cmp = key_compare(get_column_value_as_pointer(&left),
                        get_column_value_as_pointer(&right), 
                        defn.is_array ? -1 : defn.type);
  
  bool result_value = false;
  switch (expr->binary.op) {
    case TOK_EQ: result_value = (cmp == 0); break;
    case TOK_NE: result_value = (cmp != 0); break;
    case TOK_LT: result_value = (cmp == -1); break;
    case TOK_GT: result_value = (cmp == 1); break;
    case TOK_LE: result_value = (cmp <= 0); break;
    case TOK_GE: result_value = (cmp >= 0); break;
    default: 
      LOG_WARN("Unknown comparison operator: %d", expr->binary.op);
      result_value = false; 
      break;
  }
  
  bool is_null = left.is_null || right.is_null;
  return create_bool_column_value(result_value, is_null);
}

ColumnValue evaluate_like_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                    Database* db, uint8_t schema_idx) {
  ColumnDefinition defn;
  
  ColumnValue left = resolve_expr_value(expr->like.left, row, schema, db, schema_idx, &defn);
  
  if (left.type != TOK_T_VARCHAR || left.str_value == NULL) {
    LOG_ERROR("LIKE can only be applied to VARCHAR values");
    return create_bool_column_value(false, false);
  }
  
  bool result = like_match(left.str_value, expr->like.pattern);
  return create_bool_column_value(result, left.is_null);
}

ColumnValue evaluate_between_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                       Database* db, uint8_t schema_idx) {
  ColumnDefinition defn;
  
  ColumnValue value = resolve_expr_value(expr->between.value, row, schema, db, schema_idx, &defn);
  ColumnValue lower = resolve_expr_value(expr->between.lower, row, schema, db, schema_idx, &defn);
  ColumnValue upper = resolve_expr_value(expr->between.upper, row, schema, db, schema_idx, &defn);
  
  bool valid_conversion = infer_and_cast_va(3,
    (__c){&lower, TOK_T_DOUBLE},
    (__c){&upper, TOK_T_DOUBLE},
    (__c){&value, TOK_T_DOUBLE}
  );
  
  if (!valid_conversion) {
    LOG_ERROR("BETWEEN only supports numeric or date values");
    return create_null_column_value();
  }
  
  bool result = (value.double_value >= lower.double_value) && 
                (value.double_value <= upper.double_value);
  bool is_null = value.is_null || lower.is_null || upper.is_null;
  
  return create_bool_column_value(result, is_null);
}

ColumnValue evaluate_in_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                  Database* db, uint8_t schema_idx) {
  ColumnDefinition defn;
  
  ColumnValue value = resolve_expr_value(expr->in.value, row, schema, db, schema_idx, &defn);
  
  for (size_t i = 0; i < expr->in.count; ++i) {
    ColumnValue val = resolve_expr_value(expr->in.list[i], row, schema, db, schema_idx, &defn);
    
    bool match = false;
    if (value.type == TOK_T_INT || value.type == TOK_T_UINT || value.type == TOK_T_SERIAL) {
      match = (value.int_value == val.int_value);
    } else if (value.type == TOK_T_VARCHAR && value.str_value && val.str_value) {
      match = (strcmp(value.str_value, val.str_value) == 0);
    }
    
    if (match) {
      return create_bool_column_value(true, value.is_null || val.is_null);
    }
  }

  return create_bool_column_value(false, value.is_null);
}

ColumnValue evaluate_logical_and_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                           Database* db, uint8_t schema_idx) {
  ColumnValue left = evaluate_expression(expr->binary.left, row, schema, db, schema_idx);
  
  if (!left.bool_value && !left.is_null) {
    return create_bool_column_value(false, false);
  }
  
  ColumnValue right = evaluate_expression(expr->binary.right, row, schema, db, schema_idx);
  
  bool result = left.bool_value && right.bool_value;
  bool is_null = left.is_null || right.is_null;
  
  return create_bool_column_value(result, is_null);
}

ColumnValue evaluate_logical_or_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                          Database* db, uint8_t schema_idx) {
  ColumnValue left = evaluate_expression(expr->binary.left, row, schema, db, schema_idx);
  
  if (left.bool_value && !left.is_null) {
    return create_bool_column_value(true, false);
  }
  
  ColumnValue right = evaluate_expression(expr->binary.right, row, schema, db, schema_idx);
  
  bool result = left.bool_value || right.bool_value;
  bool is_null = left.is_null || right.is_null;
  
  return create_bool_column_value(result, is_null);
}

ColumnValue evaluate_logical_not_expression(ExprNode* expr, Row* row, TableSchema* schema, 
                                           Database* db, uint8_t schema_idx) {
  ColumnValue operand = evaluate_expression(expr->unary, row, schema, db, schema_idx);
  
  return create_bool_column_value(!operand.bool_value, operand.is_null);
}

ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Database* db, 
                              uint8_t schema_idx, ColumnDefinition* out) {
  ColumnValue value = evaluate_expression(expr, row, schema, db, schema_idx);
  
  if (expr->type == EXPR_COLUMN) {
    int col_index = expr->column.index;
    if (col_index < schema->column_count) {
      value = row->values[col_index];
      *out = schema->columns[col_index];
    } else {
      LOG_ERROR("Column index %d out of bounds (max: %d)", col_index, schema->column_count);
    }
  }
  
  return value;
}

ColumnValue evaluate_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  
  if (!expr) {
    return result;
  }
  
  if (is_struct_zeroed(row, sizeof(Row)) && 
      !(expr->type == EXPR_LITERAL || expr->type == EXPR_FUNCTION || expr->type == EXPR_BINARY_OP)) {
    LOG_WARN("Query expects literals or functions, not logical comparisons. Query not processed.");
    return result;
  }
  
  switch (expr->type) {
    case EXPR_LITERAL:
      return evaluate_literal_expression(expr, db);
      
    case EXPR_ARRAY_ACCESS:
      return evaluate_array_access_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_COLUMN:
      return evaluate_column_expression(expr, row, schema, db);
      
    case EXPR_UNARY_OP:
      return evaluate_unary_op_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_BINARY_OP:
      return evaluate_binary_op_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_FUNCTION:
      if (expr->fn.type == NOT_AGG) {
        return evaluate_function(expr->fn.name, expr->fn.args, expr->fn.arg_count, 
                               row, schema, db, schema_idx);
      } else {
        LOG_WARN("Evaluation of AGG function attempted, post-evaluation will be used");
        return (ColumnValue){ .tbev = &(expr->fn) };
      }
      
    case EXPR_COMPARISON:
      return evaluate_comparison_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_LIKE:
      return evaluate_like_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_BETWEEN:
      return evaluate_between_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_IN:
      return evaluate_in_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_LOGICAL_AND:
      return evaluate_logical_and_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_LOGICAL_OR:
      return evaluate_logical_or_expression(expr, row, schema, db, schema_idx);
      
    case EXPR_LOGICAL_NOT:
      return evaluate_logical_not_expression(expr, row, schema, db, schema_idx);
      
    default:
      LOG_WARN("Unsupported expression type: %d", expr->type);
      return result;
  }
}

bool evaluate_condition(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx) {
  if (!expr) {
    return false;
  }
  
  ColumnValue result = evaluate_expression(expr, row, schema, db, schema_idx);
  
  return result.bool_value && !result.is_null;
}