#include "kernel/executor.h"

Result process(Database* db, char* buffer) {
  if (!db || !db->lexer || !db->parser) {
    return (Result){(ExecutionResult){1, "Invalid context"}, NULL};
  }

  lexer_set_buffer(db->lexer, buffer);
  parser_reset(db->parser);

  JQLCommand cmd = parser_parse(db);
  return execute_cmd(db, &cmd);
}

Result process_core(Database* db, char* buffer) {
  lexer_set_buffer(db->core->lexer, buffer);
  parser_reset(db->core->parser);

  Lexer* original_lexer = db->lexer;
  Parser* original_parser = db->parser;

  db->lexer = db->core->lexer;
  db->parser = db->core->parser;

  JQLCommand cmd = parser_parse(db);

  db->lexer = original_lexer;
  db->parser = original_parser;

  return execute_cmd(db, &cmd);
}

Result execute_cmd(Database* db, JQLCommand* cmd) {
  if (cmd->is_invalid) {
    return (Result){(ExecutionResult){1, "Invalid command"}, NULL};
  }

  Result result = {(ExecutionResult){0, "Execution successful"}, NULL};

  switch (cmd->type) {
    case CMD_CREATE:
      result = (Result){execute_create_table(db, cmd), cmd};
      break;
    case CMD_INSERT:
      result = (Result){execute_insert(db, cmd), cmd};
      break;
    case CMD_SELECT:
      result = (Result){execute_select(db, cmd), cmd};
      break;
    case CMD_UPDATE:
      result = (Result){execute_update(db, cmd), cmd};
      break;
    case CMD_DELETE:
      result = (Result){execute_delete(db, cmd), cmd};
      break;
    default:
      result = (Result){(ExecutionResult){1, "Unknown command type"}, NULL};
  }

  printf("%s (effected %u rows)\n", result.exec.message, result.exec.row_count);

  if (result.exec.rows && result.exec.alias_limit > 0 && result.exec.row_count > 0) {
    printf("-> Returned %u row(s):\n", result.exec.row_count);

    for (uint32_t i = 0; i < result.exec.row_count; i++) {
      Row* row = &result.exec.rows[i];
      if (is_struct_zeroed(row, sizeof(Row))) { 
        printf("Slot %u is [nil]\n", i + 1);
        continue;
      }

      printf("Row %u [%u.%u]: ", i + 1, row->id.page_id, row->id.row_id);

      uint8_t alias_count = 0;
      for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
        ColumnDefinition col = cmd->schema->columns[c];
        ColumnValue val = row->values[c];

        // LOG_DEBUG("%d : alias: %s norm: %s", c, result.exec.aliases[alias_count], col.name);

        if (!val.is_null && result.exec.aliases[alias_count]) {
          char* name = result.exec.aliases[alias_count] ? 
            result.exec.aliases[alias_count] : col.name;
          printf("%s: ", name);
          alias_count++;
        }
        print_column_value(&val);

        if (alias_count >= result.exec.alias_limit) {
          break;
        }

        if ((c < cmd->value_counts[0] - 1) && (!val.is_null))  {
          printf(", ");
        }
      }

      printf("\n");
    }
  }
  
  return result;
}


ExecutionResult execute_create_table(Database* db, JQLCommand* cmd) {
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
  if (!db || !cmd || !db->tc_appender) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  FILE* tca_io = db->tc_appender;
  TableSchema* schema = cmd->schema;

  bool res = insert_table(db, schema->table_name);
  if (!res) { 
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  uint32_t table_count;
  io_seek(tca_io, sizeof(uint32_t), SEEK_SET);
  if (io_read(tca_io, &table_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
    table_count = db->table_count ? db->table_count : 0;
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

    io_write(tca_io, &col->has_sequence, sizeof(bool));
    if (col->has_sequence) {
      char seq_name[MAX_IDENTIFIER_LEN * 2];
      sprintf(seq_name, "%s%s", schema->table_name, col->name);
      col->sequence_id = create_default_squence(db, seq_name);

      if (col->sequence_id == -1) {
        return (ExecutionResult){0, "Table creation failed"};;
      }
    }

    io_write(tca_io, &col->has_constraints, sizeof(bool));
    // if (col->has_constraints) insert_constraint(db, )
    io_write(tca_io, &col->is_primary_key, sizeof(bool));
    io_write(tca_io, &col->is_unique, sizeof(bool));
    io_write(tca_io, &col->is_not_null, sizeof(bool));
    io_write(tca_io, &col->is_array, sizeof(bool));
    io_write(tca_io, &col->is_index, sizeof(bool));
    io_write(tca_io, &col->is_auto_increment, sizeof(bool));

    io_write(tca_io, &col->has_default, sizeof(bool));
    // if (col->has_default) {
    //   io_write(tca_io, col->default_value, MAX_IDENTIFIER_LEN);
    // }

    io_write(tca_io, &col->has_check, sizeof(bool));
    if (col->has_check) {
      io_write(tca_io, col->check_expr, MAX_IDENTIFIER_LEN);
    }

    io_write(tca_io, &col->is_foreign_key, sizeof(bool));
    if (col->is_foreign_key) {
      io_write(tca_io, col->foreign_table, MAX_IDENTIFIER_LEN);
      io_write(tca_io, col->foreign_column, MAX_IDENTIFIER_LEN);
      io_write(tca_io, &col->on_delete, sizeof(FKAction));
      io_write(tca_io, &col->on_update, sizeof(FKAction));
    }
  }

  table_count++;
  io_seek_write(db->tc_writer, TABLE_COUNT_OFFSET, &table_count, sizeof(uint32_t), SEEK_SET); 
  
  uint32_t schema_length = (uint32_t)(io_tell(tca_io) - schema_offset);
  io_seek(db->tc_writer, schema_offset, SEEK_SET); 
  io_write(db->tc_writer, &schema_length, sizeof(uint32_t)); 

  int offset_index = hash_fnv1a(schema->table_name, MAX_TABLES) * sizeof(uint32_t) + (2 * sizeof(uint32_t));
  io_seek_write(db->tc_writer, offset_index, &schema_offset, sizeof(uint32_t), SEEK_SET);
  
  off_t schema_offset_before_flush = io_tell(tca_io); 

  io_flush(tca_io);

  char table_dir[MAX_PATH_LENGTH];
  snprintf(table_dir, sizeof(table_dir), "%s/%s", db->fs->tables_dir, schema->table_name);

  if (create_directory(table_dir) != 0) {
    LOG_ERROR("Failed to create table directory");

    io_seek(tca_io, schema_offset_before_flush, SEEK_SET);

    rmdir(table_dir);

    table_count--;  
    io_seek_write(db->tc_writer, TABLE_COUNT_OFFSET, &table_count, sizeof(uint32_t), SEEK_SET);

    return (ExecutionResult){0, "Table creation failed"};;
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

  io_flush(db->tc_writer);

  load_tc(db);
  load_schema_tc(db, schema->table_name);

  return (ExecutionResult){0, "Table schema written successfully"};
}

ExecutionResult execute_insert(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(db, cmd->schema->table_name);
  if (!schema) return (ExecutionResult){1, "Error: Invalid schema"};

  load_btree_cluster(db, schema->table_name);

  uint8_t column_count = schema->column_count;
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);

  ColumnDefinition* primary_key_cols = calloc(column_count, sizeof(ColumnDefinition));
  ColumnValue* primary_key_vals = calloc(column_count, sizeof(ColumnValue));
  RowID* inserted_rows = calloc(cmd->row_count, sizeof(RowID));
  uint32_t inserted_count = 0;

  uint8_t wal_buf[MAX_ROW_BUFFER];
  uint32_t wal_len = 0;

  Row* ret_rows = calloc(cmd->row_count, sizeof(Row));
  Row* row = NULL;
  
  for (uint32_t i = 0; i < cmd->row_count; i++) {
    row = execute_row_insert(cmd->values[i], db, schema_idx, primary_key_cols,
      primary_key_vals, schema, column_count, cmd->columns, cmd->col_count, cmd->specified_order);

    if (!row) {
      for (uint32_t j = 0; j < inserted_count; j++) {
        serialize_delete(cmd->schema, inserted_rows[j]);  
      }

      free(primary_key_cols);
      free(primary_key_vals);
      free(inserted_rows);

      return (ExecutionResult){1, "Insert failed"};
    }

    inserted_rows[inserted_count] = row->id;
    ret_rows[inserted_count] = *row; 

    inserted_count++;
    // wal_len += row_to_buffer(row, cmd->schema, schema, wal_buf + wal_len);
    row = NULL;
  }

  // wal_write(db->wal, WAL_INSERT, schema_idx, wal_buf, wal_len);

  free(primary_key_cols);
  free(primary_key_vals);
  free(inserted_rows);

  if (cmd->ret_col_count <= 0) {
    free(ret_rows);
  } 
  
  return (ExecutionResult) {
    .code = 0,
    .message = "Inserted successfully",
    .row_count = inserted_count,
    .rows = ret_rows,
    .aliases = cmd->returning_columns,
    .alias_limit = cmd->ret_col_count
  };
}


Row* execute_row_insert(ExprNode** src, Database* db, uint8_t schema_idx, 
                      ColumnDefinition* primary_key_cols, ColumnValue* primary_key_vals, 
                      TableSchema* schema, uint8_t column_count,
                      char** columns, uint8_t up_col_count, bool specified_order) {
  uint8_t primary_key_count = 0;

  Row* row = calloc(1, sizeof(Row));
  row->n_values = column_count;
  row->values = (ColumnValue*)calloc(column_count, sizeof(ColumnValue));

  if (!row->values) {
    return NULL;
  }

  row->id.row_id = 0; 
  row->id.page_id = 0; 

  uint8_t null_bitmap_size = (column_count + 7) / 8;
  uint8_t* null_bitmap = (uint8_t*)malloc(null_bitmap_size);
  
  if (!null_bitmap) {
    free(row->values);
    return NULL;
  }
  
  memset(null_bitmap, 0, null_bitmap_size);

  row->null_bitmap_size = null_bitmap_size;
  row->null_bitmap = null_bitmap;
  row->row_length = sizeof(row->id) + null_bitmap_size;
  
  if (specified_order) up_col_count = column_count;

  // Initialize all columns as NULL
  for (uint8_t i = 0; i < column_count; i++) {
    row->values[i].type = schema->columns[i].type;
    row->values[i].is_null = true;

    null_bitmap[i / 8] |= (1 << (i % 8));
  }

  bool sequence_confirmations[column_count];

  for (uint8_t j = 0; j < up_col_count; j++) {
    int i = specified_order ? j : find_column_index(schema, columns[j]);

    Row empty_row = {0};
    ColumnValue cur = evaluate_expression(src[i], &empty_row, schema, db, schema_idx);
    
    bool valid_conversion = infer_and_cast_value(&cur, &(schema->columns[i]));
    
    if (!valid_conversion) {
      LOG_ERROR("Invalid conversion whilst trying to insert row");
      return NULL;
    }

    if (schema->columns[i].is_foreign_key) {
      if (!check_foreign_key(db, schema->columns[i], cur)) {
        LOG_ERROR("Foreign key constraint evaluation failed: \n> %s does not match any %s.%s",
          str_column_value(&cur), schema->columns[i].foreign_table, schema->columns[i].foreign_column);
        return NULL;
      }
    }

    row->values[i] = cur;

    if (is_struct_zeroed(&cur, sizeof(ColumnValue))) {
      free(row->values);
      free(row->null_bitmap);
      return NULL;
    }

    if (cur.is_null && schema->columns[i].is_not_null) {
      LOG_ERROR("Column '%s' is bound by NOT NULL constraint, value breaks constraint.", schema->columns[i].name);
    }

    if (!cur.is_null) {
      null_bitmap[i / 8] &= ~(1 << (i % 8));
      row->values[i].is_null = false;
    }

    if (schema->columns[i].is_primary_key) {
      primary_key_cols[primary_key_count] = schema->columns[i];
      primary_key_vals[primary_key_count] = cur;
      primary_key_count++;
    }

    row->row_length += size_from_value(&row->values[i], &schema->columns[i]);
    // LOG_DEBUG("Row size + %zu = %zu", size_from_value(&row.values[i], &schema->columns[i]), row.row_length);
  }
  
  for (uint8_t i = 0; i < column_count; i++) {
    if (row->values[i].type == TOK_T_SERIAL) {
      row->values[i].is_null = false;
      char seq_name[MAX_IDENTIFIER_LEN * 2];
      sprintf(seq_name, "%s%s", schema->table_name, schema->columns[i].name);
      row->values[i].int_value = sequence_next_val(db, seq_name);
      row->values[i].int_value = sequence_next_val(db, seq_name);
      null_bitmap[i / 8] &= ~(1 << (i % 8));
    }
  }


  for (uint8_t i = 0; i < primary_key_count; i++) {
    if (&primary_key_cols[i]) {
      uint8_t idx = hash_fnv1a(primary_key_cols[i].name, MAX_COLUMNS);
      void* key = get_column_value_as_pointer(&primary_key_vals[i]);
      RowID res = btree_search(db->tc[schema_idx].btree[idx], key);
      if (!is_struct_zeroed(&res, sizeof(RowID))) {
        free(row->values);
        free(row->null_bitmap);
        return NULL;
      }
    }
  }

  BufferPool* pool = &(db->lake[schema_idx]);
  char row_file[MAX_PATH_LENGTH];
  snprintf(row_file, sizeof(row_file), "%s" SEP "%s" SEP "rows.db",
        db->fs->tables_dir, schema->table_name);

  if (is_struct_zeroed(pool, sizeof(BufferPool))) {
    initialize_buffer_pool(pool, schema_idx, row_file);
  }

  RowID row_id = serialize_insert(pool, *row, db->tc[schema_idx]);

  for (uint8_t i = 0; i < primary_key_count; i++) {
    if (&primary_key_cols[i]) {
      uint8_t idx = hash_fnv1a(primary_key_cols[i].name, MAX_COLUMNS);
      void* key = get_column_value_as_pointer(&primary_key_vals[i]);
    
      if (!btree_insert(db->tc[schema_idx].btree[idx], key, row_id)) {
        free(row->values);
        free(row->null_bitmap);
        return NULL;
      }
    }
  }

  return (Row*)row;  
}

ExecutionResult execute_select(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(db, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(db, schema->table_name);
  cmd->schema = schema;

  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &db->lake[schema_idx];

  RowID row_start = {0};
  Row* collected_rows = calloc(100, sizeof(Row));
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

      if (is_struct_zeroed(row, sizeof(Row))) continue;
      if (row->deleted) continue;
      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, db, schema_idx))  continue;

      collected_rows[total_found] = *row;
      total_found++;  
      
      if (!is_struct_zeroed(&row_start, sizeof(RowID)) && total_found > 0)
        break;
    }
  }

  if (cmd->has_order_by && total_found > 1) {
    quick_sort_rows(collected_rows, 0, total_found - 1, cmd, schema);
  }

  uint32_t start = cmd->has_offset ? cmd->offset : 0;
  uint32_t max_out = cmd->has_limit  ? cmd->limit  : total_found;
  uint32_t available = (start < total_found) ? (total_found - start) : 0;
  uint32_t out_count = (available < max_out) ? available : max_out;

  Row* result_rows = calloc(out_count, sizeof(Row));
  char** aliases = malloc(sizeof(char*) * cmd->value_counts[0]);
  for (int i = 0; i < cmd->value_counts[0]; i++) {
    aliases[i] = malloc(256);
  }
  
  if (!result_rows) {
    free(collected_rows);
    return (ExecutionResult){1, "Memory allocation failed for limited result rows"};
  }

  for (uint32_t i = 0; i < out_count; i++) {
    Row* src = &collected_rows[start + i];
    Row* dst = &result_rows[i];
  
    memset(dst, 0, sizeof(Row));
    dst->id = src->id;
    dst->values = calloc(schema->column_count, sizeof(ColumnValue));
    dst->n_values = schema->column_count;
    if (!dst->values) {
      free(collected_rows);
      free(result_rows);
      return (ExecutionResult){1, "Memory allocation failed for projected values"};
    }
  
    for (int k = 0; k < schema->column_count; k++) {
      dst->values[k].is_null = true;
    }
  
    int col_count = cmd->value_counts[0];

    for (int j = 0; j < col_count; j++) {
      ExprNode* expr = cmd->sel_columns[j].expr;

      if (cmd->sel_columns[j].alias) {
        aliases[j] = strdup(cmd->sel_columns[j].alias);
      } else if (expr->type == EXPR_ARRAY_ACCESS) {
        int base_idx = expr->column.index;
        int array_idx = expr->column.array_idx->literal.int_value;

        const char* base_name = cmd->schema->columns[base_idx].name;
        char buffer[256];

        snprintf(buffer, sizeof(buffer), "%s[%d]", base_name, array_idx);
        aliases[j] = strdup(buffer);
      } else {
        aliases[j] = strdup(cmd->schema->columns[expr->column.index].name);
      }

      if (!expr) {
        ColumnValue raw = src->values[j];
        dst->values[j] = raw; 
        dst->values[j].column.index = j;
      } else {
        ColumnValue val = evaluate_expression(expr, src, schema, db, schema_idx);
        // LOG_DEBUG("printing using eval %d w. %d %d > %s", j, src->id.page_id, src->id.row_id, str_column_value(&val));      
        dst->values[j] = val;
      }

      free_expr_node(expr);
    }  
  }
  
  free(collected_rows);

  return (ExecutionResult){
    .code = 0,
    .message = "Select executed successfully",
    .rows = result_rows,
    .aliases = aliases,
    .row_count = out_count,
    .alias_limit = cmd->value_counts[0],
    .owns_rows = 1
  };
}

ExecutionResult execute_update(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }
  
  TableSchema* schema = find_table_schema_tc(db, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(db, schema->table_name);
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);

  BufferPool* pool = &db->lake[schema_idx];
  uint32_t rows_updated = 0;
  
  for (uint16_t page_idx = 0; page_idx < pool->num_pages; ++page_idx) {
    Page* page = pool->pages[page_idx];
    if (!page || page->num_rows == 0) continue;
  
    for (uint16_t row_idx = 0; row_idx < page->num_rows; ++row_idx) {
      Row* row = &page->rows[row_idx];
      
      if (row->deleted) continue;
      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, db, schema_idx)) {
        continue;
      }
  
      int max_updates = cmd->value_counts[0];
      uint16_t* update_cols = malloc(sizeof(uint16_t) * max_updates);
      ColumnValue* old_vals = malloc(sizeof(ColumnValue) * max_updates);
      ColumnValue* new_vals = malloc(sizeof(ColumnValue) * max_updates);
  
      int valid_updates = 0;
      for (int k = 0; k < max_updates; ++k) {
        int col_index = cmd->update_columns[k].index;

        ColumnValue evaluated = evaluate_expression(cmd->values[0][k], row, schema, db, schema_idx);
        ColumnValue array_idx = evaluate_expression(cmd->update_columns->array_idx, row, schema, db, schema_idx);
        
        if (!infer_and_cast_value(&evaluated, &schema->columns[col_index])) {
          free(update_cols);
          free(old_vals);
          free(new_vals);
          return (ExecutionResult){1, "Invalid conversion whilst trying to update row"};
        }
        
        if (schema->columns[col_index].is_foreign_key && !check_foreign_key(db, schema->columns[col_index], evaluated)) {
          free(update_cols);
          free(old_vals);
          free(new_vals);
          return (ExecutionResult){1, "Foreign key constraint restricted UPDATE"};
        }

        update_cols[valid_updates] = col_index;

        if (!is_struct_zeroed(&array_idx, sizeof(ColumnValue))) {
          old_vals[valid_updates] = row->values[col_index];
          new_vals[valid_updates] = old_vals[valid_updates];
          new_vals[valid_updates].array.array_value[array_idx.int_value] = evaluated;
          continue;
        }
        
        old_vals[valid_updates] = row->values[col_index];
        new_vals[valid_updates] = evaluated;
        valid_updates++;
      }
      
      if (valid_updates == 0) {
        free(update_cols);
        free(old_vals);
        free(new_vals);
        continue;
      }
      
      write_update_wal(db->wal, schema_idx, page_idx, row_idx, update_cols, old_vals, new_vals, valid_updates, schema);
      
      for (int u = 0; u < valid_updates; ++u) {
        row->values[update_cols[u]] = new_vals[u];
      }
      
      row->null_bitmap = cmd->bitmap;
      rows_updated++;
      page->is_dirty = true;
      free(update_cols);
      free(old_vals);
      free(new_vals);
    }
  }
  return (ExecutionResult){0, "Update executed successfully", .row_count = rows_updated};
}

ExecutionResult execute_delete(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = find_table_schema_tc(db, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(db, schema->table_name);
  cmd->schema = schema;

  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &db->lake[schema_idx];

  uint32_t rows_deleted = 0;

  for (uint16_t page_idx = 0; page_idx < pool->num_pages; page_idx++) {
    Page* page = pool->pages[page_idx];
    if (!page || page->num_rows == 0) continue;

    for (uint16_t row_idx = 0; row_idx < page->num_rows; row_idx++) {
      Row* row = &page->rows[row_idx];

      if (is_struct_zeroed(row, sizeof(Row))) continue;
      if (row->deleted) continue;
      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, db, schema_idx)) continue;
      
      write_delete_wal(db->wal, schema_idx, page_idx, row_idx, row, schema);

      for (uint8_t k = 0; k < schema->column_count; k++) {
        if (schema->columns[k].is_primary_key) {
          uint8_t btree_idx = hash_fnv1a(schema->columns[k].name, MAX_COLUMNS);
          void* key = get_column_value_as_pointer(&row->values[k]);

          if (!btree_delete(db->tc[schema_idx].btree[btree_idx], key)) {
            LOG_WARN("Warning: failed to delete PK from B-tree");
          }
        }

        if (row->values[k].is_toast) {
          bool res = toast_delete(db, row->values[k].toast_object);

          if (!res) LOG_WARN("Unable to delete TOAST entries \n > run 'fix'");
        }

        if (schema->columns[k].is_foreign_key) {
          if (!handle_on_delete_constraints(db, schema->columns[k], row->values[k])) {
            return (ExecutionResult){1, "DELETE restricted by foreign constraint"};
          }
        }
      }

      RowID id = {page_idx, row_idx + 1};
      serialize_delete(pool, id);
      
      page->is_dirty = true;
      rows_deleted += 1;
    }
  }

  return (ExecutionResult){
    .code = 0,
    .message = "Delete executed successfully",
    .row_count = rows_deleted
  };
}

void write_update_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  uint16_t* col_indices, ColumnValue* old_values, ColumnValue* new_values, 
  uint16_t num_columns, TableSchema* schema) {
  /**
  WAL Update Format:
  [2B] Page Index
  [2B] Row Index
  [2B] Number of Updated Columns
  
  For each updated column:
    [2B] Column Index
    [4B] Old Value Size
    [var] Old Value Data
    [4B] New Value Size
    [var] New Value Data
  */

  uint32_t header_size = sizeof(uint16_t) * 3;  // page_idx, row_idx, num_columns
  uint32_t data_size = 0;
  uint8_t temp_buf[1024];
  
  for (int i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[col_indices[i]];
    uint32_t old_val_size = write_column_value_to_buffer(temp_buf, &old_values[i], def);
    uint32_t new_val_size = write_column_value_to_buffer(temp_buf, &new_values[i], def);
    
    data_size += sizeof(uint16_t) +          // Column index
                  sizeof(uint32_t) * 2 +      // Old and new value sizes
                  old_val_size + new_val_size; // Actual values
  }
  
  uint32_t total_size = header_size + data_size;
  uint8_t* wal_buf = malloc(total_size);
  if (!wal_buf) {
    return; 
  }
  
  uint32_t offset = 0;
  memcpy(wal_buf + offset, &page_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &row_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &num_columns, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  for (int i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[col_indices[i]];
    
    memcpy(wal_buf + offset, &col_indices[i], sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    uint32_t old_val_size = write_column_value_to_buffer(temp_buf, &old_values[i], def);
    memcpy(wal_buf + offset, &old_val_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(wal_buf + offset, temp_buf, old_val_size);
    offset += old_val_size;
    
    uint32_t new_val_size = write_column_value_to_buffer(temp_buf, &new_values[i], def);
    memcpy(wal_buf + offset, &new_val_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(wal_buf + offset, temp_buf, new_val_size);
    offset += new_val_size;
  }
  
  wal_write(wal, WAL_UPDATE, schema_idx, wal_buf, total_size);
  
  free(wal_buf);
}

void write_delete_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  Row* row, TableSchema* schema) {
  /**
  WAL Delete Format:
  [2B] Page Index
  [2B] Row Index
  [2B] Number of Columns
  [2B] Null Bitmap
  
  For each column:
    [2B] Column Index
    [4B] Value Size
    [var] Value Data
  */

  uint32_t header_size = sizeof(uint16_t) * 4;  // page_idx, row_idx, num_columns, null_bitmap
  uint32_t data_size = 0;
  uint8_t temp_buf[1024];
  uint16_t num_columns = schema->column_count;
  
  for (uint16_t i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[i];
    uint32_t val_size = write_column_value_to_buffer(temp_buf, &row->values[i], def);
    
    data_size += sizeof(uint16_t) +  // Column index
                 sizeof(uint32_t) +  // Value size
                 val_size;           // Actual value
  }
  
  uint32_t total_size = header_size + data_size;
  uint8_t* wal_buf = malloc(total_size);
  if (!wal_buf) {
    return; 
  }
  
  uint32_t offset = 0;
  
  memcpy(wal_buf + offset, &page_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &row_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &num_columns, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &row->null_bitmap, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  for (uint16_t i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[i];
    
    memcpy(wal_buf + offset, &i, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    uint32_t val_size = write_column_value_to_buffer(temp_buf, &row->values[i], def);
    memcpy(wal_buf + offset, &val_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(wal_buf + offset, temp_buf, val_size);
    offset += val_size;
  }
  
  wal_write(wal, WAL_DELETE, schema_idx, wal_buf, total_size);
  
  free(wal_buf);
}

bool insert_table(Database* db, char* name) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_table");
    return false;
  }

  if (strcmp(name, "jb_tables") == 0
      || strcmp(name, "jb_sequences") == 0) {
    return true;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[2048];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_tables "
    "(name, database_name, owner) "
    "VALUES ('%s', '%s', NULL);",
    name,
    db->uuid
  );

  Result res = process(db->core, query);
  bool success = res.exec.code == 0;

  if (!success) {
    LOG_ERROR("Failed to insert table '%s'", name);
    return false;
  }
  free_result(&res);

  parser_restore_state(db->core->parser, state);

  return true;
}

int64_t sequence_next_val(Database* db, char* name) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_table");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char pquery[2048];
  snprintf(pquery, sizeof(pquery),
    "UPDATE jb_sequences SET current_value = current_value + increment_by "
    "WHERE name = '%s'; ",
    name
  );

  Result pres = process(db->core, pquery);
  bool psuccess = pres.exec.code == 0;
  if (!psuccess) {
    LOG_ERROR("Failed to update the sequence '%s'", name);
    return -1;
  }

  free_result(&pres);

  char query[2048];
  snprintf(query, sizeof(query),
    "SELECT current_value, increment_by FROM jb_sequences "
    "WHERE name = '%s'; ",
    name
  );

  Result res = process(db->core, query);
  bool success = res.exec.code == 0;
  if (!success) {
    LOG_ERROR("Failed to find a valid sequence '%s'", name);
    return -1;
  }

  int table_idx = hash_fnv1a("jb_sequences", MAX_TABLES);
  int cv_idx = find_column_index(db->core->tc[table_idx].schema, "current_value");

  int copy = res.exec.rows[0].values[0].int_value;
  // free_result(&res);

  parser_restore_state(db->core->parser, state);

  return copy;
}

int64_t create_default_squence(Database* db, char* name) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_table");
    return -1;
  }

  if (strcmp(name, "jb_tablesid") == 0) {
    return 0;
  }

  if (strcmp(name, "jb_sequencesid") == 0) {
    return 1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[2048];

  snprintf(query, sizeof(query),
    "INSERT INTO jb_sequences "
    "(name, current_value, increment_by, min_value, max_value, cycle)"
    "VALUES ('%s', 0, 1, 0, NULL, false)"
    "RETURNING id;",
    name
  );

  Result res = process(db->core, query);
  bool success = res.exec.code == 0;
  if (!success || res.exec.alias_limit == 0) {
    LOG_ERROR("Failed to create a default sequence '%s'", name);
    return -1;
  }

  parser_restore_state(db->core->parser, state);

  return res.exec.rows[0].values[0].int_value;
}


int64_t find_sequence(Database* db, char* name) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_table");
    return -1;
  }

  if (strcmp(name, "jb_tablesid") == 0) {
    return 0;
  }

  if (strcmp(name, "jb_sequencesid") == 0) {
    return 1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);
  char query[2048];

  snprintf(query, sizeof(query),
    "SELECT id FROM jb_sequences"
    "WHERE name = '%s';",
    name
  );

  Result res = process(db->core, query);
  bool success = res.exec.code == 0;
  if (!success) {
    LOG_ERROR("Failed to find a valid sequence '%s'", name);
    return -1;
  }

  parser_restore_state(db->core->parser, state);
  return res.exec.rows[0].values[0].int_value;
}