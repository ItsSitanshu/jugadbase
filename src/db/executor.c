#include "executor.h"

#include "../utils/log.h"
#include "../utils/security.h"

ExecutionResult process(Context* ctx, char* buffer) {
  if (!ctx || !ctx->lexer || !ctx->parser) {
    return (ExecutionResult){1, "Invalid execution context"};
  }

  lexer_set_buffer(ctx->lexer, buffer);
  parser_reset(ctx->parser);

  JQLCommand cmd = parser_parse(ctx);
  return execute_cmd(ctx, &cmd);
}

ExecutionResult execute_cmd(Context* ctx, JQLCommand* cmd) {
  if (cmd->is_invalid) {
    return (ExecutionResult){1, "Invalid command"};
  }

  ExecutionResult result = {0, "Execution successful"};

  switch (cmd->type) {
    case CMD_CREATE:
      result = execute_create_table(ctx, cmd);
      break;
    case CMD_INSERT:
      result = execute_insert(ctx, cmd);
      break;
    case CMD_SELECT:
      result = execute_select(ctx, cmd);
      break;
    default:
      result = (ExecutionResult){1, "Unknown command type"};
  }

  if (result.rows && result.row_count > 0) {
    printf("-> Returned %u row(s):\n", result.row_count);

    for (uint32_t i = 0; i < result.row_count; i++) {
      Row* row = &result.rows[i];
      printf("Row %u [ID: %u.%u]: ", i + 1, row->id.page_id, row->id.row_id);

      for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
        ColumnDefinition col = cmd->schema->columns[c];
        ColumnValue val = row->values[c];

        printf("%s=", col.name);
        print_column_value(&val);

        if (c < cmd->schema->column_count - 1) {
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
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(ctx, schema->table_name);

  uint8_t column_count = schema->column_count;
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);

  ColumnDefinition* primary_key_cols[MAX_COLUMNS] = {NULL};
  ColumnValue* primary_key_vals[MAX_COLUMNS] = {NULL};
  uint8_t primary_key_count = 0;

  for (uint8_t i = 0; i < column_count; i++) {
    if (schema->columns[i].is_primary_key) {
      primary_key_cols[primary_key_count] = &schema->columns[i];
      primary_key_vals[primary_key_count] = &cmd->values[i];
      primary_key_count++;
    }
  }

  for (uint8_t i = 0; i < primary_key_count; i++) {
    if (primary_key_cols[i]) {
      uint8_t idx = hash_fnv1a(primary_key_cols[i]->name, MAX_COLUMNS);
      void* key = get_column_value_as_pointer(primary_key_vals[i]);
      RowID res = btree_search(ctx->tc[schema_idx].btree[idx], key);
      if (!is_struct_zeroed(&res, sizeof(RowID))) {
        return (ExecutionResult){1, "Primary Key already exists"};
      }
    }
  }

  BufferPool* pool = &(ctx->lake[schema_idx]);
  char row_file[MAX_PATH_LENGTH];
  snprintf(row_file, sizeof(row_file), "%s" SEP "%s" SEP "rows.db",
        ctx->fs->tables_dir, schema->table_name);

  if (is_struct_zeroed(pool, sizeof(BufferPool))) {
    initialize_buffer_pool(pool, schema_idx, row_file);
    LOG_INFO("%d %p", pool->idx, pool->pages);
  }

  Row row = {0};
  row.id.row_id = 0; 
  row.id.page_id = 0; 

  uint8_t null_bitmap_size = (column_count + 7) / 8;
  uint8_t* null_bitmap = (uint8_t*)malloc(null_bitmap_size);
  if (!null_bitmap) {
    return (ExecutionResult){1, "Memory allocation failed for null bitmap"};
  }
  memset(null_bitmap, 0, null_bitmap_size);

  for (uint8_t i = 0; i < column_count; i++) {
    if (cmd->values[i].is_null) {
      null_bitmap[i / 8] |= (1 << (i % 8));
    }
  }

  row.null_bitmap_size = null_bitmap_size;
  row.null_bitmap = null_bitmap;
  row.row_length = sizeof(row.id) + null_bitmap_size;

  row.values = (ColumnValue*)malloc(sizeof(ColumnValue) * column_count);
  if (!row.values) {
    free(null_bitmap);
    return (ExecutionResult){1, "Memory allocation failed for column data"};
  }

  for (uint8_t i = 0; i < column_count; i++) {
    row.values[i] = cmd->values[i];
    row.row_length += size_from_type(schema->columns[i].type); 
  }

  RowID row_id = serialize_insert(pool, row, ctx->tc[schema_idx]);

  for (uint8_t i = 0; i < primary_key_count; i++) {
    if (primary_key_cols[i]) {
      uint8_t idx = hash_fnv1a(primary_key_cols[i]->name, MAX_COLUMNS);
      void* key = get_column_value_as_pointer(primary_key_vals[i]);
    
      if (!btree_insert(ctx->tc[schema_idx].btree[idx], key, row_id)) {
        free(row.values);
        free(row.null_bitmap);
        return (ExecutionResult){1, "Failed to insert record into B-tree"};
      }
    }
  }

  return (ExecutionResult){0, "Record inserted successfully"};
}

ExecutionResult execute_select(Context* ctx, JQLCommand* cmd) {
  if (!ctx || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(ctx, cmd->schema->table_name);
  if (!schema) return (ExecutionResult){1, "Error: Invalid schema"};

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
      Row* row = &(page->rows[j]);

      if (!is_struct_zeroed(&row_start, sizeof(RowID))) {
        if (row->id.page_id != row_start.page_id || row->id.row_id != row_start.row_id) continue;
      }

      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, ctx, schema_idx)) {
        continue;
      }

      collected_rows[total_found++] = *row;

      if (!is_struct_zeroed(&row_start, sizeof(RowID))) break;
    }

    if (!is_struct_zeroed(&row_start, sizeof(RowID)) && total_found > 0) break;
  }

  Row* result_rows = malloc(sizeof(Row) * (PAGE_SIZE / 10));

  if (!result_rows) {
    free(collected_rows);
    return (ExecutionResult){1, "Memory allocation failed for final result copy"};
  }

  if (cmd->value_count == 1 && strcmp(cmd->columns[0], "*") == 0) {
    memcpy(result_rows, collected_rows, sizeof(Row) * total_found);
  } else {
    for (uint32_t i = 0; i < total_found; i++) {
      Row* src = &collected_rows[i];
      Row* dst = &result_rows[i];
      memset(dst, 0, sizeof(Row));
      dst->id = src->id;

      dst->values = calloc(schema->column_count, sizeof(ColumnValue));
      if (!dst->values) {
        free(collected_rows);
        free(result_rows);
        return (ExecutionResult){1, "Memory allocation failed for projected values"};
      }

      for (int j = 0; j < cmd->value_count; j++) {
        char* colname = cmd->columns[j];

        for (uint8_t k = 0; k < schema->column_count; k++) {
          if (strcmp(schema->columns[k].name, colname) == 0) {
            dst->values[k] = src->values[k];
            break;
          }
        }
      }
    }
  }

  free(collected_rows);

  return (ExecutionResult){
    .code = 0,
    .message = "Select executed successfully",
    .rows = result_rows,
    .row_count = total_found,
    .owns_rows = 1
  };
}


void* get_column_value_as_pointer(ColumnValue* col_val) {
  switch (col_val->type) {
    case TOK_NL:
      col_val->is_null = true;
      break;
    case TOK_L_I8:
    case TOK_L_I16:
    case TOK_L_I32:
    case TOK_L_I64:
    case TOK_L_U8:
    case TOK_L_U16:
    case TOK_L_U32:
    case TOK_L_U64:
      return &(col_val->int_value);
    case TOK_L_FLOAT:
      return &(col_val->float_value);
    case TOK_L_DOUBLE:
      return &(col_val->double_value);
    case TOK_L_BOOL:
      return &(col_val->bool_value);
    case TOK_L_CHAR:
      return &(col_val->str_value[0]);
    case TOK_L_STRING:
      LOG_DEBUG("%s", col_val->str_value);
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

bool evaluate_condition(ConditionNode* cond, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx) {
  if (!cond) return false;

  switch (cond->type) {
    case CONDITION_COMPARISON: {
      ColumnValue left, right;
      uint8_t column_type = 0;

      if (cond->left_is_column) {
        left = row->values[cond->left_column_index];
        column_type = schema->columns[cond->left_column_index].type;
      } else {
        left = cond->left_value;
      }

      if (cond->right_is_column) {
        right = row->values[cond->right_column_index];
        if (!column_type) {
          column_type = schema->columns[cond->right_column_index].type;
        }
      } else {
        right = cond->right_value;
      }

      // print_column_value(&left);
      // printf("left \n");
      // print_column_value(&right);
      // printf("right \n");

      if (cond->op == COMP_EQ && cond->left_is_column && !cond->right_is_column &&
          schema->columns[cond->left_column_index].is_primary_key) {

        void* key = get_column_value_as_pointer(&left);
        uint8_t btree_idx = hash_fnv1a(schema->columns[cond->left_column_index].name, MAX_COLUMNS);
        RowID rid = btree_search(ctx->tc[schema_idx].btree[btree_idx], key);
          
        return !(!is_struct_zeroed(&rid, sizeof(RowID)) &&
                row->id.page_id == rid.page_id &&
                row->id.row_id == rid.row_id);
      }

      int cmp = key_compare(get_column_value_as_pointer(&left),
                            get_column_value_as_pointer(&right),
                            column_type);

      switch (cond->op) {
        case COMP_EQ:  return cmp == 0;
        case COMP_NEQ: return cmp != 0;
        case COMP_LT:  return cmp < 0;
        case COMP_GT:  return cmp > 0;
        case COMP_LTE: return cmp <= 0;
        case COMP_GTE: return cmp >= 0;
        default: return false;
      }
    }

    case CONDITION_AND:
      evaluate_condition(cond->right, row, schema, ctx, schema_idx), evaluate_condition(cond->left, row, schema, ctx, schema_idx) &&
      evaluate_condition(cond->right, row, schema, ctx, schema_idx));
      return evaluate_condition(cond->left, row, schema, ctx, schema_idx) &&
             evaluate_condition(cond->right, row, schema, ctx, schema_idx);

    case CONDITION_OR:
      return evaluate_condition(cond->left, row, schema, ctx, schema_idx) ||
             evaluate_condition(cond->right, row, schema, ctx, schema_idx);

    case CONDITION_NOT:
      return !evaluate_condition(cond->right, row, schema, ctx, schema_idx);

    default:
      return false;
  }
}

void print_column_value(ColumnValue* val) {
  if (val->is_null) {
    printf("NULL");
    return;
  }

  printf("[");

  switch (val->type) {
    case TOK_L_I8: case TOK_L_I16: case TOK_L_I32: case TOK_L_I64:
    case TOK_L_U8: case TOK_L_U16: case TOK_L_U32: case TOK_L_U64:
      printf("%d", val->int_value);
      break;

    case TOK_L_FLOAT:
      printf("%f", val->float_value);
      break;

    case TOK_L_DOUBLE:
      printf("%lf", val->double_value);
      break;

    case TOK_L_BOOL:
      printf(val->bool_value ? "true" : "false");
      break;

    case TOK_L_STRING:
    case TOK_L_CHAR:
      printf("\"%s\"", val->str_value);
      break;

    default:
      printf("unprintable type: %d", val->type);
      break;
  }

  printf("]");
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