#include "executor.h"

#include "../utils/log.h"
#include "../utils/security.h"

Result process(Context* ctx, char* buffer) {
  if (!ctx || !ctx->lexer || !ctx->parser) {
    return (Result){(ExecutionResult){1, "Invalid context"}, NULL};
  }

  lexer_set_buffer(ctx->lexer, buffer);
  parser_reset(ctx->parser);

  JQLCommand cmd = parser_parse(ctx);
  return execute_cmd(ctx, &cmd);
}

Result execute_cmd(Context* ctx, JQLCommand* cmd) {
  if (cmd->is_invalid) {
    return (Result){(ExecutionResult){1, "Invalid command"}, NULL};
  }

  Result result = {(ExecutionResult){0, "Execution successful"}, NULL};

  switch (cmd->type) {
    case CMD_CREATE:
      result = (Result){execute_create_table(ctx, cmd), cmd};
      break;
    case CMD_INSERT:
      result = (Result){execute_insert(ctx, cmd), cmd};
      break;
    case CMD_SELECT:
      result = (Result){execute_select(ctx, cmd), cmd};
      break;
    case CMD_UPDATE:
      result = (Result){execute_update(ctx, cmd), cmd};
      break;
    case CMD_DELETE:
      result = (Result){execute_delete(ctx, cmd), cmd};
      break;
    default:
      result = (Result){(ExecutionResult){1, "Unknown command type"}, NULL};
  }


  if (result.exec.rows && result.exec.row_count > 0) {
    printf("-> Returned %u row(s):\n", result.exec.row_count);

    for (uint32_t i = 0; i < result.exec.row_count; i++) {
      Row* row = &result.exec.rows[i];
      if (is_struct_zeroed(row, sizeof(Row))) { 
        printf("Slot %u is [nil]\n", i + 1);
        continue;
      }

      printf("Row %u [%u.%u]: ", i + 1, row->id.page_id, row->id.row_id);

      for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
        ColumnDefinition col = cmd->schema->columns[c];
        ColumnValue val = row->values[c];
        
        if (!val.is_null) {
          printf("%s=", col.name);
        }
        print_column_value(&val);

        if ((c < cmd->value_counts[0] - 1) && (!val.is_null))  {
          printf(", ");
        }
      }

      printf("\n");
    }
  }

  return result;
}


ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd) {
  /*
  [4B]  DB_INIT_MAGIC
  [4B]  Table Count
  [4B * 255] Table Offsets
  (every file not every entry) 

  For each table:
    [4B] Schema Offset

    For each schema:
    [1B] Table Name Length
    [var] Table Name
    [1B] Column Count

    For each column:
      [1B] Column Name Length
      [var] Column Name
      [4B] Column Type
      [1B] Varchar Length
      [1B] Decimal Precision
      [1B] Decimal Scale
      [1B] Column Flags
      [1B] Has Default
      [var] Default Value
      [1B] Has Check
      [var] Check Expression
      [1B] Is Foreign Key
      [var] Foreign Table
      [var] Foreign Column
  */
  if (!ctx || !cmd || !ctx->tc_appender) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  FILE* tca_io = ctx->tc_appender;
  TableSchema* schema = cmd->schema;
  
  uint32_t table_count;
  io_seek(tca_io, sizeof(uint32_t), SEEK_SET);
  if (io_read(tca_io, &table_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
    table_count = ctx->table_count ? ctx->table_count : 0;
    io_write(tca_io, &table_count, sizeof(uint32_t)); 
  }

  uint32_t schema_offset = 0;
  io_write(tca_io, &schema_offset, sizeof(uint32_t));  
  io_flush(tca_io);

  schema_offset = io_tell(tca_io) - sizeof(uint32_t);  

  uint8_t table_name_length = (uint8_t)strlen(schema->table_name);
  io_write(tca_io, &table_name_length, sizeof(uint8_t));
  io_write(tca_io, schema->table_name, table_name_length);

  uint8_t column_count = (uint8_t)schema->column_count;
  io_write(tca_io, &column_count, sizeof(uint8_t));

  for (int i = 0; i < column_count; i++) {
    ColumnDefinition* col = &schema->columns[i];

    uint8_t col_name_length = (uint8_t)strlen(col->name);
    io_write(tca_io, &col_name_length, sizeof(uint8_t));
    io_write(tca_io, col->name, col_name_length);

    io_write(tca_io, &col->type, sizeof(uint32_t));
    io_write(tca_io, &col->type_varchar, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_precision, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_scale, sizeof(uint8_t));

    io_write(tca_io, &col->is_primary_key, sizeof(bool));
    io_write(tca_io, &col->is_unique, sizeof(bool));
    io_write(tca_io, &col->is_not_null, sizeof(bool));
    io_write(tca_io, &col->is_index, sizeof(bool));
    io_write(tca_io, &col->is_auto_increment, sizeof(bool));

    io_write(tca_io, &col->has_default, sizeof(bool));
    if (col->has_default) {
      io_write(tca_io, col->default_value, MAX_IDENTIFIER_LEN);
    }

    io_write(tca_io, &col->has_check, sizeof(bool));
    if (col->has_check) {
      io_write(tca_io, col->check_expr, MAX_IDENTIFIER_LEN);
    }

    io_write(tca_io, &col->is_foreign_key, sizeof(bool));
    if (col->is_foreign_key) {
      io_write(tca_io, col->foreign_table, MAX_IDENTIFIER_LEN);
      io_write(tca_io, col->foreign_column, MAX_IDENTIFIER_LEN);
    }
  }

  table_count++;
  io_seek_write(ctx->tc_writer, TABLE_COUNT_OFFSET, &table_count, sizeof(uint32_t), SEEK_SET); 
  
  uint32_t schema_length = (uint32_t)(io_tell(tca_io) - schema_offset);
  io_seek(ctx->tc_writer, schema_offset, SEEK_SET); 
  io_write(ctx->tc_writer, &schema_length, sizeof(uint32_t)); 

  int offset_index = hash_fnv1a(schema->table_name, MAX_TABLES) * sizeof(uint32_t) + (2 * sizeof(uint32_t));
  io_seek_write(ctx->tc_writer, offset_index, &schema_offset, sizeof(uint32_t), SEEK_SET);
  
  off_t schema_offset_before_flush = io_tell(tca_io); 

  io_flush(tca_io);

  char table_dir[MAX_PATH_LENGTH];
  snprintf(table_dir, sizeof(table_dir), "%s/%s", ctx->fs->tables_dir, schema->table_name);

  if (create_directory(table_dir) != 0) {
    LOG_ERROR("Failed to create table directory");

    io_seek(tca_io, schema_offset_before_flush, SEEK_SET);

    rmdir(table_dir);

    table_count--;  
    io_seek_write(ctx->tc_writer, TABLE_COUNT_OFFSET, &table_count, sizeof(uint32_t), SEEK_SET);

    return (ExecutionResult){0, "Table creat.ion failed"};;
  }

  char rows_file[MAX_PATH_LENGTH];
  int ret = snprintf(rows_file, MAX_PATH_LENGTH, "%s" SEP "rows.db", table_dir);
  if (ret >= MAX_PATH_LENGTH) {
    LOG_WARN("Rows file path is too long, truncating to fit buffer size.");
    rows_file[MAX_PATH_LENGTH - 1] = '\0';
  }

  FILE* rows_fp = fopen(rows_file, "wb");
  if (!rows_fp) {
    LOG_ERROR("Failed to create rows file: %s", rows_file);
    rmdir(table_dir);
    return (ExecutionResult){0, "Failed to create rows file"};
  }

  uint64_t row_id = 0;
  fwrite(&row_id, sizeof(uint64_t), 1, rows_fp);

  fclose(rows_fp);

  io_flush(ctx->tc_writer);

  load_tc(ctx);
  load_schema_tc(ctx, schema->table_name);

  return (ExecutionResult){0, "Table schema written successfully"};
}

ExecutionResult execute_insert(Context* ctx, JQLCommand* cmd) {
  if (!ctx || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(ctx, cmd->schema->table_name);
  if (!schema) return (ExecutionResult){1, "Error: Invalid schema"};

  load_btree_cluster(ctx, schema->table_name);

  uint8_t column_count = schema->column_count;
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);

  ColumnDefinition* primary_key_cols = malloc(sizeof(ColumnDefinition) * column_count);
  ColumnValue* primary_key_vals = malloc(sizeof(ColumnValue) * column_count);

  uint8_t success = 0;

  for (uint32_t i = 0; i < cmd->row_count; i++) {
    if (execute_row_insert(cmd->values[i], ctx, schema_idx, 
      primary_key_cols, primary_key_vals, schema, column_count)) {
        success += 1;
    }
  }

  if (success < cmd->row_count) {
    LOG_ERROR("Inserted %d out of %d provided, could not insert %d row(s)", 
      success, cmd->row_count, (cmd->row_count - success));
  }

  return (ExecutionResult){0, "Record inserted successfully"};
}

bool execute_row_insert(ExprNode** src, Context* ctx, uint8_t schema_idx, 
                      ColumnDefinition* primary_key_cols, ColumnValue* primary_key_vals, 
                      TableSchema* schema, uint8_t column_count) {
  uint8_t primary_key_count = 0;

  Row row = {0};
  row.values = (ColumnValue*)malloc(sizeof(ColumnValue) * column_count);

  if (!row.values) {
    return false;
  }

  row.id.row_id = 0; 
  row.id.page_id = 0; 

  uint8_t null_bitmap_size = (column_count + 7) / 8;
  uint8_t* null_bitmap = (uint8_t*)malloc(null_bitmap_size);
  
  if (!null_bitmap) {
    free(row.values);
    return false;
  }
  
  memset(null_bitmap, 0, null_bitmap_size);

  row.null_bitmap_size = null_bitmap_size;
  row.null_bitmap = null_bitmap;
  row.row_length = sizeof(row.id) + null_bitmap_size;

  for (uint8_t i = 0; i < column_count; i++) {
    Row empty_row = {0};
    ColumnValue cur = evaluate_expression(src[i], &empty_row, schema, ctx, schema_idx);
    bool valid_conversion = infer_and_cast_value(&cur, schema->columns[i].type);

    if (!valid_conversion) {
      LOG_ERROR("Invalid conversion whilst trying to insert row");
      return false;
    }

    row.values[i] = cur;

    if (is_struct_zeroed(&cur, sizeof(ColumnValue))) {
      free(row.values);
      free(row.null_bitmap);
      return false;
    }

    if (cur.is_null) {
      null_bitmap[i / 8] |= (1 << (i % 8));
    }

    row.row_length += size_from_type(schema->columns[i].type); 

    if (schema->columns[i].is_primary_key) {
      primary_key_cols[primary_key_count] = schema->columns[i];
      primary_key_vals[primary_key_count] = cur;
      primary_key_count++;
    }
  }

  for (uint8_t i = 0; i < primary_key_count; i++) {
    if (&primary_key_cols[i]) {
      uint8_t idx = hash_fnv1a(primary_key_cols[i].name, MAX_COLUMNS);
      void* key = get_column_value_as_pointer(&primary_key_vals[i]);
      RowID res = btree_search(ctx->tc[schema_idx].btree[idx], key);
      if (!is_struct_zeroed(&res, sizeof(RowID))) {
        free(row.values);
        free(row.null_bitmap);
        return false;
      }
    }
  }

  BufferPool* pool = &(ctx->lake[schema_idx]);
  char row_file[MAX_PATH_LENGTH];
  snprintf(row_file, sizeof(row_file), "%s" SEP "%s" SEP "rows.db",
        ctx->fs->tables_dir, schema->table_name);

  if (is_struct_zeroed(pool, sizeof(BufferPool))) {
    initialize_buffer_pool(pool, schema_idx, row_file);
  }

  RowID row_id = serialize_insert(pool, row, ctx->tc[schema_idx]);

  for (uint8_t i = 0; i < primary_key_count; i++) {
    if (&primary_key_cols[i]) {
      uint8_t idx = hash_fnv1a(primary_key_cols[i].name, MAX_COLUMNS);
      void* key = get_column_value_as_pointer(&primary_key_vals[i]);
    
      if (!btree_insert(ctx->tc[schema_idx].btree[idx], key, row_id)) {
        free(row.values);
        free(row.null_bitmap);
        return false;
      }
    }
  }

  return true;
}

ExecutionResult execute_select(Context* ctx, JQLCommand* cmd) {
  if (!ctx || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(ctx, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(ctx, schema->table_name);
  cmd->schema = schema;

  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &ctx->lake[schema_idx];

  RowID row_start = {0};
  Row* collected_rows = malloc(sizeof(Row) * (PAGE_SIZE / 10));
  if (!collected_rows) {
    return (ExecutionResult){1, "Memory allocation failed for result rows"};
  }

  uint32_t total_found = 0;
  for (uint16_t i = 0; i < pool->num_pages; i++) {
    Page* page = pool->pages[i];
    if (!page || page->num_rows == 0) continue;
    for (uint16_t j = 0; j < page->num_rows; j++) {
      Row* row = &page->rows[j];
      if (!is_struct_zeroed(&row_start, sizeof(RowID))) {
        if (row->id.page_id != row_start.page_id || row->id.row_id != row_start.row_id)
          continue;
      }
      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, ctx, schema_idx))
        continue;
      collected_rows[total_found++] = *row;
      if (!is_struct_zeroed(&row_start, sizeof(RowID)) && total_found > 0)
        break;
    }
    if (!is_struct_zeroed(&row_start, sizeof(RowID)) && total_found > 0)
      break;
  }

  if (cmd->has_order_by && total_found > 1) {
    quick_sort_rows(collected_rows, 0, total_found - 1, cmd, schema);
  }

  uint32_t start = cmd->has_offset ? cmd->offset : 0;
  uint32_t max_out = cmd->has_limit  ? cmd->limit  : total_found;
  uint32_t available = (start < total_found) ? (total_found - start) : 0;
  uint32_t out_count = (available < max_out) ? available : max_out;

  Row* result_rows = malloc(sizeof(Row) * out_count);
  if (!result_rows) {
    free(collected_rows);
    return (ExecutionResult){1, "Memory allocation failed for limited result rows"};
  }

  if (cmd->select_all && cmd->value_counts[0] == 1) {
    for (uint32_t i = 0; i < out_count; i++) {
      result_rows[i] = collected_rows[start + i];
    }
  } else {
    for (uint32_t i = 0; i < out_count; i++) {
      Row* src = &collected_rows[start + i];
      Row* dst = &result_rows[i];
      memset(dst, 0, sizeof(Row));
      dst->id = src->id;
      dst->values = calloc(schema->column_count, sizeof(ColumnValue));
      if (!dst->values) {
        free(collected_rows);
        free(result_rows);
        return (ExecutionResult){1, "Memory allocation failed for projected values"};
      }
      for (int k = 0; k < schema->column_count; k++) {
        dst->values[k].is_null = true;
      }
      for (int j = 0; j < cmd->value_counts[0]; j++) {
        ColumnValue val = evaluate_expression(cmd->sel_columns[j].expr, src, schema, ctx, schema_idx);
        dst->values[val.column_index] = val;
      }
    }
  }

  free(collected_rows);

  return (ExecutionResult){
    .code = 0,
    .message = "Select executed successfully",
    .rows = result_rows,
    .row_count = out_count,
    .owns_rows = 1
  };
}


ExecutionResult execute_update(Context* ctx, JQLCommand* cmd) {
  if (!ctx || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(ctx, cmd->schema->table_name);
  if (!schema) return (ExecutionResult){1, "Error: Invalid schema"};

  load_btree_cluster(ctx, schema->table_name);

  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &ctx->lake[schema_idx];

  uint32_t rows_updated = 0;

  for (uint16_t i = 0; i < pool->num_pages; i++) {
    Page* page = pool->pages[i];
    if (!page || page->num_rows == 0) continue;

    for (uint16_t j = 0; j < page->num_rows; j++) {
      Row* row = &page->rows[j];
      Row* temp = malloc(sizeof(Row)); 
      memcpy(temp, &page->rows[j], sizeof(Row));

      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, ctx, schema_idx)) {
        continue;
      }

      for (int k = 0; k < cmd->value_counts[0]; k++) {
        char* colname = cmd->columns[k];
        int col_index = find_column_index(schema, colname);
        
        row->values[col_index] = evaluate_expression(cmd->values[0][k], row, schema, ctx, schema_idx);
        bool valid_conversion = infer_and_cast_value(&row->values[col_index], schema->columns[col_index].type);
        
        if (!valid_conversion) {          
          return (ExecutionResult){1, "Invalid conversion whilst trying to update row"};;
        }

        row->null_bitmap = cmd->bitmap;
        free(temp);
      }

      rows_updated++;
    }
    
    if (rows_updated > 0) {
      page->is_dirty = true;
    }
  }

  return (ExecutionResult){
    .code = 0,
    .message = "Update executed successfully",
    .row_count = rows_updated
  };
}

ExecutionResult execute_delete(Context* ctx, JQLCommand* cmd) {
  if (!ctx || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(ctx, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(ctx, schema->table_name);
  cmd->schema = schema;

  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &ctx->lake[schema_idx];

  uint32_t rows_deleted = 0;

  for (uint16_t i = 0; i < pool->num_pages; i++) {
    Page* page = pool->pages[i];
    if (!page || page->num_rows == 0) continue;

    for (uint16_t j = 0; j < page->num_rows; j++) {
      Row* row = &page->rows[j];

      if (is_struct_zeroed(row, sizeof(Row))) continue;

      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, ctx, schema_idx)) {
        continue;
      }

      for (uint8_t k = 0; k < schema->column_count; k++) {
        if (schema->columns[k].is_primary_key) {
          uint8_t btree_idx = hash_fnv1a(schema->columns[k].name, MAX_COLUMNS);
          void* key = get_column_value_as_pointer(&row->values[k]);

          if (!btree_delete(ctx->tc[schema_idx].btree[btree_idx], key)) {
            LOG_WARN("Warning: failed to delete PK from B-tree");
          }
        }
      }

      RowID id = {i, j + 1};
      serialize_delete(pool, id);
      rows_deleted++;
    }
  }

  return (ExecutionResult){
    .code = 0,
    .message = "Delete executed successfully",
    .row_count = rows_deleted
  };
}

ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx, uint8_t* out_type) {
  ColumnValue value = evaluate_expression(expr, row, schema, ctx, schema_idx);

  if (expr->type == EXPR_COLUMN) {
    int col_index = expr->column_index;
    value = row->values[col_index];
    *out_type = schema->columns[col_index].type;
  }

  return value;
}

ColumnValue evaluate_expression(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx) {
  ColumnValue result;
  memset(&result, 0, sizeof(ColumnValue));
  
  if (is_struct_zeroed(row, sizeof(Row)) && !(
    expr->type == EXPR_LITERAL || 
    expr->type == EXPR_FUNCTION ||
    expr->type == EXPR_BINARY_OP
  )) {
    LOG_WARN("Latest query expects literals or functions, not logical comparisons. Query not processed.");
  }

  if (!expr) return result;
  switch (expr->type) {
    case EXPR_LITERAL:
      return expr->literal;
    case EXPR_COLUMN: {
      result.column_index = expr->column_index;

      if (row && expr->column_index < schema->column_count) {
        ColumnValue col = row->values[expr->column_index];
        col.type = schema->columns[expr->column_index].type;
        col.column_index = expr->column_index;
        return col;
      }
      result.type = schema->columns[expr->column_index].type;
      return result;
    }
    case EXPR_UNARY_OP: {
      ColumnValue operand = resolve_expr_value(expr->arth_unary.expr, row, schema, ctx, schema_idx, &result.type);
    
      switch (expr->arth_unary.op) {
        case TOK_SUB:
          if (operand.type == TOK_T_INT || operand.type == TOK_T_UINT) {
            result.type = TOK_T_INT;
            result.int_value = -operand.int_value;
          } else if (operand.type == TOK_T_FLOAT || operand.type == TOK_T_DOUBLE) {
            result.type = TOK_T_DOUBLE;
            result.double_value = -operand.double_value;
          } else {
            LOG_ERROR("Unary minus not supported on this type");
            result.type = operand.type;
          }
          return result;
    
        default:
          LOG_WARN("Unsupported unary operation: %d", expr->arth_unary.op);
          return (ColumnValue){0};
      }
    }    
    case EXPR_BINARY_OP: {
      uint8_t type = 0;

      ColumnValue left = resolve_expr_value(expr->binary.left, row, schema, ctx, schema_idx, &type);
      ColumnValue right = resolve_expr_value(expr->binary.right, row, schema, ctx, schema_idx, &type);

      switch (type) {
        case TOK_T_INT:
        case TOK_T_UINT:
        case TOK_T_SERIAL:
        case TOK_T_FLOAT:
        case TOK_T_DOUBLE:
          bool valid_conversion = infer_and_cast_va(2,
            (__c){&left, TOK_T_DOUBLE},
            (__c){&right, TOK_T_DOUBLE}  
          );

          if (!valid_conversion) {          
            return (ColumnValue){0};
          }
          
          result.type = TOK_T_DOUBLE;
          switch (expr->binary.op) {
            case TOK_ADD: result.double_value = left.double_value + right.double_value; break;
            case TOK_SUB: result.double_value = left.double_value - right.double_value; break;
            case TOK_MUL: result.double_value = left.double_value * right.double_value; break;
            case TOK_DIV: result.double_value = left.double_value / right.double_value; break;
            default: LOG_WARN("Invalid binary-operation found, will produce incorrect results");
          }
          break;
        default:
          LOG_DEBUG("SYE_E_UNSUPPORTED_BINARY_EXPR_TYPE: %d", type);
          break;
      }
      return result;
    }
    case EXPR_FUNCTION:
      return evaluate_function(expr->fn.name,
                               expr->fn.args,
                               expr->fn.arg_count,
                               row, schema, ctx, schema_idx);

    case EXPR_COMPARISON: {
      uint8_t type = 0;

      ColumnValue left = resolve_expr_value(expr->binary.left, row, schema, ctx, schema_idx, &type);
      ColumnValue right = resolve_expr_value(expr->binary.right, row, schema, ctx, schema_idx, &type);
      
      if (expr->binary.op == TOK_EQ &&
          expr->binary.left->type == EXPR_COLUMN &&
          expr->binary.right->type == EXPR_LITERAL &&
          schema->columns[expr->binary.left->column_index].is_primary_key && ctx) {
        
        void* key = get_column_value_as_pointer(&right);
        uint8_t btree_idx = hash_fnv1a(schema->columns[expr->binary.left->column_index].name, MAX_COLUMNS);
        RowID rid = btree_search(ctx->tc[schema_idx].btree[btree_idx], key);

        result.type = TOK_T_BOOL;
        result.bool_value = (!is_struct_zeroed(&rid, sizeof(RowID)) &&
                             row->id.page_id == rid.page_id &&
                             row->id.row_id == rid.row_id);
        return result;
      }


      bool valid_conversion = infer_and_cast_va(2,
        (__c){&left, type},
        (__c){&right, type}
      );

      if (!valid_conversion) {
        LOG_ERROR("Invalid conversion whilst trying to evaluate conditions");
        return (ColumnValue){0};
      }

      int cmp = key_compare(get_column_value_as_pointer(&left), get_column_value_as_pointer(&right), type);

      result.type = TOK_T_BOOL;

      switch (expr->binary.op) {
        case TOK_EQ: result.bool_value = (cmp == 0); break;
        case TOK_NE: result.bool_value = (cmp != 0); break;
        case TOK_LT: result.bool_value = (cmp == -1); break;
        case TOK_GT: result.bool_value = (cmp == 1); break;
        case TOK_LE: result.bool_value = (cmp == 0 || cmp == -1); break;
        case TOK_GE: result.bool_value = (cmp == 0 || cmp == 1); break;
        default: result.bool_value = false; break;
      }
      
      return result;
    }
    case EXPR_LIKE: {
      uint8_t type = 0;
    
      ColumnValue left = resolve_expr_value(expr->like.left, row, schema, ctx, schema_idx, &type);
      if (left.type != TOK_T_VARCHAR || left.str_value == NULL) {
        LOG_ERROR("LIKE can only be applied to VARCHAR values");
        return (ColumnValue){ .type = TOK_T_BOOL, .bool_value = false };
      }
          
      bool res = like_match(left.str_value, expr->like.pattern);
      if (res) {
        return (ColumnValue){ .type = TOK_T_BOOL, .bool_value = true };
      }

      return (ColumnValue){ .type = TOK_T_BOOL, .bool_value = false };
    }
    case EXPR_BETWEEN: {
      uint8_t type = 0;
    
      ColumnValue value = resolve_expr_value(expr->between.value, row, schema, ctx, schema_idx, &type);
      ColumnValue lower = resolve_expr_value(expr->between.lower, row, schema, ctx, schema_idx, &type);
      ColumnValue upper = resolve_expr_value(expr->between.upper, row, schema, ctx, schema_idx, &type);
    
      ColumnValue result = { .type = TOK_T_BOOL, .bool_value = false };
    
      if (value.type == TOK_T_INT) {
        result.bool_value = (value.int_value >= lower.int_value) && (value.int_value <= upper.int_value);
      } /* else if (value.type == TOK_T_DATE) {
        result.bool_value = (value.date_value >= lower.date_value) && (value.date_value <= upper.date_value);
      } */ else {
        LOG_ERROR("BETWEEN only supports INT or DATE values");
        result.is_null = true;
      }
    
      return result;
    }
    case EXPR_IN: {
      uint8_t type = 0;
      ColumnValue value = resolve_expr_value(expr->in.value, row, schema, ctx, schema_idx, &type);
      ColumnValue result = { .type = TOK_T_BOOL, .bool_value = false };
    
      for (size_t i = 0; i < expr->in.count; ++i) {
        ColumnValue val = resolve_expr_value(expr->in.list[i], row, schema, ctx, schema_idx, &type);
    
        bool match = false;
        if (value.type == TOK_T_INT || value.type == TOK_T_UINT || value.type == TOK_T_SERIAL) {
          match = (value.int_value == val.int_value);
        } else if (value.type == TOK_T_VARCHAR) {
          match = (strcmp(value.str_value, val.str_value) == 0);
        } /* else if (value.type == TOK_T_DATE) {
          match = (value.date_value == val.date_value);
        } */
    
        if (match) {
          result.bool_value = true;
          return result;
        }
      }
    
      return result;
    }        
    case EXPR_LOGICAL_AND: {
      ColumnValue left = evaluate_expression(expr->binary.left, row, schema, ctx, schema_idx);
      if (!left.bool_value) {
        result.type = TOK_T_BOOL;
        result.bool_value = false;
        return result;
      }
      ColumnValue right = evaluate_expression(expr->binary.right, row, schema, ctx, schema_idx);
      result.type = TOK_T_BOOL;
      result.bool_value = left.bool_value && right.bool_value;

      return result;
    }
    case EXPR_LOGICAL_OR: {
      ColumnValue left = evaluate_expression(expr->binary.left, row, schema, ctx, schema_idx);
      if (left.bool_value) {
        result.type = TOK_T_BOOL;
        result.bool_value = true;
        return result;
      }
      ColumnValue right = evaluate_expression(expr->binary.right, row, schema, ctx, schema_idx);
      result.type = TOK_T_BOOL;
      result.bool_value = left.bool_value || right.bool_value;
      return result;
    }

    case EXPR_LOGICAL_NOT: {
      ColumnValue operand = evaluate_expression(expr->unary, row, schema, ctx, schema_idx);
            
      result.type = TOK_T_BOOL;
      result.bool_value = !operand.bool_value;
      return result;
    }
    default:
      return result;
  }
}

bool evaluate_condition(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx) {
  if (!expr) return false;
  
  ColumnValue result = evaluate_expression(expr, row, schema, ctx, schema_idx);

  return result.bool_value;
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
      return &(col_val->blob_value);
    case TOK_T_JSON:
      return &(col_val->json_value);
    case TOK_T_DECIMAL:
      return &(col_val->decimal.decimal_value);
    case TOK_T_DATE:
      return &(col_val->date);
    case TOK_T_TIME:
      return &(col_val->time);
    case TOK_T_DATETIME:
      return &(col_val->datetime);
    case TOK_T_TIMESTAMP:
      return &(col_val->timestamp);
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
    valid = infer_and_cast_value(item.value, item.expected_type);

    if (!valid) {
      LOG_ERROR("Invalid conversion on item %zu", i);
      va_end(args);
      return false;
    }
  }

  va_end(args);
  return true;
}

bool infer_and_cast_value(ColumnValue* col_val, uint8_t target_type) {
  if (col_val->type == TOK_NL) {
    col_val->is_null = true;
    return true;
  }

  if (col_val->type == target_type) {
    return true;
  }

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
        (void)(0);
      }
      break;
    }
    case TOK_T_STRING: {
      if (target_type == TOK_T_CHAR) {
        if (!(col_val->str_value && strlen(col_val->str_value) > 0)) {
          return false;
        }
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
        } else {
        }
      } else if (target_type == TOK_T_DOUBLE) {
        char* endptr;
        col_val->double_value = strtod(col_val->str_value, &endptr);
        if (*endptr != '\0') {
          return false;
        } else {
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
      } else if (target_type == TOK_T_VARCHAR) {
        (void)(0);
      } else {
        return false;
      }
      break;
    }
    // case TOK_T_BLOB: {
    //   if (target_type == TOK_T_STRING) {
    //     if (col_val->blob_data && col_val->blob_size > 0) {
    //       col_val->str_value = malloc(col_val->blob_size + 1);
    //       memcpy(col_val->str_value, col_val->blob_data, col_val->blob_size);
    //       col_val->str_value[col_val->blob_size] = '\0';
    //       col_val->type = target_type;
    //     } else {
    //       return false;
    //     }
    //   } else {
    //     return false;
    //   }
    //   break;
    // }
    // case TOK_T_JSON: {
    //   if (target_type == TOK_T_STRING) {
    //     if (col_val->json_value) {
    //       col_val->str_value = strdup(col_val->json_value);
    //       col_val->type = target_type;
    //     } else {
    //       return false;
    //     }
    //   } else {
    //     return false;
    //   }
    //   break;
    // }
    // case TOK_T_DECIMAL: {
    //   if (target_type == TOK_T_FLOAT) {
    //     col_val->type = target_type;
    //     col_val->float_value = (float)(col_val->decimal.decimal_value);
    //   } else if (target_type == TOK_T_DOUBLE) {
    //     col_val->type = target_type;
    //     col_val->double_value = (double)(col_val->decimal.decimal_value);
    //   } else if (target_type == TOK_T_INT || target_type == TOK_T_UINT || target_type == TOK_T_SERIAL) {
    //     col_val->type = target_type;
    //     col_val->int_value = (int64_t)(col_val->decimal.decimal_value);
    //   } else if (target_type == TOK_T_STRING) {
    //     // Convert decimal to string (implementation depends on decimal structure)
    //     // ...
    //     col_val->type = target_type;
    //   } else {
    //     return false;
    //   }
    //   break;
    // }
    default:
      return false;
  }
  
  col_val->type = target_type;
  return true;
}


size_t size_from_type(uint8_t column_type) {
  size_t size = 0;

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
    case TOK_T_TIMESTAMP:
    case TOK_T_DATETIME:
    case TOK_T_TIME:
    case TOK_T_DATE:
      size = sizeof(int);  
      break;
    case TOK_T_VARCHAR:
      size = MAX_VARCHAR_SIZE;
      break;
    case TOK_T_CHAR:
      size = sizeof(uint8_t);
      break;
    case TOK_T_TEXT:
    case TOK_T_JSON:
      size = MAX_JSON_SIZE;
      break;
    default:
      size = 0;  
      break;
  }

  return size;
}

uint32_t get_table_offset(Context* ctx, const char* table_name) {
  for (int i = 0; i < ctx->table_count; i++) {
    if (strcmp(ctx->tc[i].name, table_name) == 0) {
      return ctx->tc[i].offset;
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

// ExecutionOrder* generate_execution_plan(JQLCommand* command) {
//   ExecutionOrder* order = malloc(sizeof(ExecutionOrder));
//   if (!order) return NULL;

//   order->num_steps = 1;  
//   order->steps = malloc(sizeof(ExecutionStep) * order->num_steps);
//   if (!order->steps) {
//     free(order);
//     return NULL;
//   }

//   order->steps[0].type = EXECUTFILEN_CREATE_TABLE;  
//   order->steps[0].command = *command;  

//   return order;
// }