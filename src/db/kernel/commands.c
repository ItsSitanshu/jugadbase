#include "kernel/kernel.h"

ExecutionResult execute_create_table(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !db->tc_appender) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  FILE* tca_io = db->tc_appender;
  TableSchema* schema = cmd->schema;

  int64_t table_id = insert_table(db, schema->table_name);
  if (table_id == -1) { 
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

  uint32_t idx = hash_fnv1a(schema->table_name, MAX_TABLES);

  uint8_t column_count = (uint8_t)schema->column_count;
  io_write(tca_io, &column_count, sizeof(uint8_t));

  for (int i = 0; i < column_count; i++) {
    ColumnDefinition* col = &schema->columns[i];

    uint8_t col_name_length = (uint8_t)strlen(col->name);
    io_write(tca_io, &col_name_length, sizeof(uint8_t));
    io_write(tca_io, col->name, col_name_length);

    io_write(tca_io, &col->type_varchar, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_precision, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_scale, sizeof(uint8_t));

    if (col->has_sequence) {
      char seq_name[MAX_IDENTIFIER_LEN * 2];
      sprintf(seq_name, "%s%s", schema->table_name, col->name);
      col->sequence_id = create_default_sequence(db->core, seq_name, cmd->is_unsafe);

      if (col->sequence_id == -1) {
        return (ExecutionResult){-1, "Table creation failed"};;
      }
    }

    io_write(tca_io, &col->is_array, sizeof(bool));
    io_write(tca_io, &col->is_index, sizeof(bool));
    io_write(tca_io, &col->is_foreign_key, sizeof(bool));
    
    insert_attribute(db->core, table_id, col->name, col->type, i, 
      !col->is_not_null, col->has_default, col->has_constraints, cmd->is_unsafe);

    if (col->has_default) {
      char* default_value = str_column_value(col->default_value);
      insert_attr_default(db->core, table_id, col->name, default_value, cmd->is_unsafe);
    } 

    if (col->is_primary_key) {
      insert_constraint(db, table_id, col->constraint.constraint_name,
        col->constraint.constraint_type, col->constraint.columns, col->constraint.columns_count,
        strdup(col->constraint.constraint_expr), -1, col->constraint.ref_columns,
        col->constraint.ref_columns_count, col->constraint.on_delete, col->constraint.on_update);
    } else if (col->is_unique) {
      insert_constraint(db, table_id, col->constraint.constraint_name,
        col->constraint.constraint_type, col->constraint.columns, col->constraint.columns_count,
        strdup(col->constraint.constraint_expr), -1, col->constraint.ref_columns,
        col->constraint.ref_columns_count, col->constraint.on_delete, col->constraint.on_update);
    }
    
    if (col->is_foreign_key) {
      int64_t ref_table = find_table(db, col->constraint.ref_table);

      insert_constraint(db, table_id, col->constraint.constraint_name,
        col->constraint.constraint_type, col->constraint.columns, col->constraint.columns_count,
        strdup(col->constraint.constraint_expr), ref_table, col->constraint.ref_columns,
        col->constraint.ref_columns_count, col->constraint.on_delete, col->constraint.on_update);
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

ExecutionResult execute_alter_table(Database* db, JQLCommand* cmd) {
  ExecutionResult result = {0};
  AlterTableCommand* alter_cmd = cmd->alter;
  
  uint32_t table_offset = hash_fnv1a(alter_cmd->table_name, MAX_TABLES);
  if (table_offset == UINT32_MAX) {
    result.code = -1;
    result.message = "Table not found";
    return result;
  }
  
  TableSchema* schema = db->tc[table_offset].schema;
  int64_t table_id = find_table(db, alter_cmd->table_name);

  if (table_id == -1) {
    result.code = -1;
    result.message = "Table not found in catalog";
    return result;
  }
  
  switch (alter_cmd->operation) {
    case ALTER_ADD_COLUMN: {
      if (schema->column_count >= MAX_COLUMNS) {
        result.code = -1;
        result.message = "Maximum column count exceeded";
        return result;
      }
      
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->add_column.column_name) == 0) {
          result.code = -1;
          result.message = "Column already exists";
          return result;
        }
      }
      
      ColumnDefinition* new_col = &schema->columns[schema->column_count];
      strcpy(new_col->name, alter_cmd->add_column.column_name);
      new_col->type = alter_cmd->add_column.data_type;
      new_col->is_not_null = alter_cmd->add_column.not_null;
      new_col->has_default = alter_cmd->add_column.has_default;
      
      schema->column_count++;
      
      // if (alter_cmd->add_column.not_null) {
      //   const char* col_names[] = { alter_cmd->add_column.column_name };
      //   insert_constraint(db, table_id, "", CONSTRAINT_NOT_NULL, col_names, 1,
      //     NULL, NULL, NULL, 0, 0, 0, false, false, false, false, false);
      // }
      
      result.code = 0;
      result.message = "Column added successfully";
      break;
    }
    
    case ALTER_DROP_COLUMN: {
      int col_idx = -1;
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->column.column_name) == 0) {
          col_idx = i;
          break;
        }
      }
      
      if (col_idx == -1) {
        result.code = -1;
        result.message = "Column not found";
        return result;
      }
      
      for (uint8_t i = col_idx; i < schema->column_count - 1; i++) {
        schema->columns[i] = schema->columns[i + 1];
      }
      schema->column_count--;
      
      result.code = 0;
      result.message = "Column dropped successfully";
      break;
    }
    
    case ALTER_RENAME_COLUMN: {
      int col_idx = -1;
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->column.column_name) == 0) {
          col_idx = i;
          break;
        }
      }
      
      if (col_idx == -1) {
        result.code = -1;
        result.message = "Column not found";
        return result;
      }
      
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (i != col_idx && strcmp(schema->columns[i].name, alter_cmd->column.new_column_name) == 0) {
          result.code = -1;
          result.message = "New column name already exists";
          return result;
        }
      }
      
      result.code = 0;
      result.message = "Column renamed successfully";
      break;
    }
    
    case ALTER_SET_DEFAULT: {
      int col_idx = -1;
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->column.column_name) == 0) {
          col_idx = i;
          break;
        }
      }
      
      if (col_idx == -1) {
        result.code = -1;
        result.message = "Column not found";
        return result;
      }
      
      int64_t existing_default = find_default_constraint(db, table_id, alter_cmd->column.column_name);
      if (existing_default >= 0) {
        // delete_constraint(db, existing_default);
      }
      
      schema->columns[col_idx].has_default = true;
      
      insert_attr_default(db, table_id, alter_cmd->column.column_name, alter_cmd->column.default_expr, false);

      result.code = 0;
      result.message = "Default value set successfully";
      break;
    }
    
    case ALTER_DROP_DEFAULT: {
      int col_idx = -1;
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->column.column_name) == 0) {
          col_idx = i;
          break;
        }
      }
      
      if (col_idx == -1) {
        result.code = -1;
        result.message = "Column not found";
        return result;
      }
      
      int64_t default_constraint = find_default_constraint(db, table_id, alter_cmd->column.column_name);
      if (default_constraint >= 0) {
        // delete_constraint(db, default_constraint);
      }
      
      schema->columns[col_idx].has_default = false;
      memset(schema->columns[col_idx].default_value, 0, sizeof(ColumnValue));
      result.code = 0;
      result.message = "Default value dropped successfully";
      break;
    }
    
    case ALTER_SET_NOT_NULL: {
      int col_idx = -1;
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->column.column_name) == 0) {
          col_idx = i;
          break;
        }
      }
      
      if (col_idx == -1) {
        result.code = -1;
        result.message = "Column not found";
        return result;
      }
      
      schema->columns[col_idx].is_not_null = true;
      
      // const char* col_names[] = { alter_cmd->column.column_name };
      // insert_constraint(db, table_id, "", CONSTRAINT_NOT_NULL, col_names, 1,
      //   NULL, NULL, NULL, 0, 0, 0, false, false, false, false, false);
      
      result.code = 0;
      result.message = "NOT NULL constraint added successfully";
      break;
    }
    
    case ALTER_DROP_NOT_NULL: {
      int col_idx = -1;
      for (uint8_t i = 0; i < schema->column_count; i++) {
        if (strcmp(schema->columns[i].name, alter_cmd->column.column_name) == 0) {
          col_idx = i;
          break;
        }
      }
      
      if (col_idx == -1) {
        result.code = -1;
        result.message = "Column not found";
        return result;
      }
      
      schema->columns[col_idx].is_not_null = false;
      result.code = 0;
      result.message = "NOT NULL constraint dropped successfully";
      break;
    }
    
    case ALTER_ADD_CONSTRAINT: {
      ParsedConstraint cnstr = alter_cmd->constraint;

      for (int i = 0; i < cnstr.columns_count; i++) {
        bool found = false;
        for (uint8_t j = 0; j < schema->column_count; j++) {
          if (strcmp(schema->columns[j].name, cnstr.columns[i]) == 0) {
            found = true;
            break;
          }
        }

        if (!found) {
          result.code = -1;
          result.message = "Referenced column not found";
          return result;
        }
      }

      int64_t ref_table = find_table(db, cnstr.ref_table);

      int64_t constraint_id = insert_constraint(db, table_id, cnstr.constraint_name,
        cnstr.constraint_type, cnstr.columns, cnstr.columns_count,
        strdup(cnstr.constraint_expr), ref_table, cnstr.ref_columns,
        cnstr.ref_columns_count, cnstr.on_delete, cnstr.on_update);
      
      if (constraint_id < 0) {
        result.code = -1;
        result.message = "Failed to add constraint";
        return result;
      }
      
      result.code = 0;
      result.message = "Constraint added successfully";
      break;
    }
    
    case ALTER_DROP_CONSTRAINT: {
      int64_t constraint_id = find_constraint_by_name(db, table_id, alter_cmd->constraint.constraint_name);
      if (constraint_id < 0) {
        result.code = -1;
        result.message = "Constraint not found";
        return result;
      }
      
      if (!delete_constraint(db, constraint_id)) {
        result.code = -1;
        result.message = "Failed to drop constraint";
        return result;
      }
      
      result.code = 0;
      result.message = "Constraint dropped successfully";
      break;
    }
    
    case ALTER_RENAME_CONSTRAINT: {
      int64_t constraint_id = find_constraint_by_name(db, table_id, alter_cmd->constraint.constraint_name);
      if (constraint_id < 0) {
        result.code = -1;
        result.message = "Constraint not found";
        return result;
      }
      
      const char* new_name = alter_cmd->constraint.constraint_expr;
      if (!update_constraint_name(db, constraint_id, new_name)) {
        result.code = -1;
        result.message = "Failed to rename constraint";
        return result;
      }
      
      result.code = 0;
      result.message = "Constraint renamed successfully";
      break;
    }
    
    case ALTER_RENAME_TABLE: {
      for (uint8_t i = 0; i < MAX_TABLES; i++) {
        if (is_struct_zeroed(db->tc[i].schema, sizeof(TableSchema))) break;

        if (strcmp(db->tc[i].schema->table_name, alter_cmd->rename_table.new_table_name) == 0) {
          result.code = -1;
          result.message = "Table name already exists";
          return result;
        }
      }
      
      strcpy(schema->table_name, alter_cmd->rename_table.new_table_name);
      result.code = 0;
      result.message = "Table renamed successfully";
      break;
    }
    
    case ALTER_SET_OWNER: {
      result.code = 0;
      result.message = "Owner set successfully";
      break;
    }
    
    case ALTER_SET_TABLESPACE: {
      result.code = 0;
      result.message = "Tablespace set successfully";
      break;
    }
    
    default:
      result.code = -1;
      result.message = "Unknown ALTER operation";
      break;
  }
  
  return result;
}

ExecutionResult execute_insert(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = get_table_schema(db, cmd->schema->table_name);
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

  int64_t table_id = find_table(db, schema->table_name);

  if (table_id == -1) {
    return (ExecutionResult) {
      .code = -1,
      .message = "Could not find matching schema",
    };
  }
  
  for (uint32_t i = 0; i < cmd->row_count; i++) {
    row = execute_row_insert(cmd->values[i], db, schema_idx, primary_key_cols,
      primary_key_vals, schema, column_count, cmd->columns, cmd->col_count, cmd->specified_order, table_id, cmd->is_unsafe);

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
                      char** columns, uint8_t up_col_count, bool specified_order, int64_t table_id, bool is_unsafe) {

  uint8_t primary_key_count = 0;

  Row* row = calloc(1, sizeof(Row));
  Row empty_row = {0};

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
  
  // if (specified_order) up_col_count = column_count;

  // Initialize all columns as NULL
  for (uint8_t i = 0; i < column_count; i++) {
    row->values[i].type = schema->columns[i].type;
    row->values[i].is_null = true;

    null_bitmap[i / 8] |= (1 << (i % 8));
  }

  bool sequence_confirmations[column_count];

  for (uint8_t j = 0; j < up_col_count; j++) {
    int i = find_column_index(schema, columns[j]);

    // LOG_INFO("%d: %s type of %d", i, schema->columns[i].name, schema->columns[i].type);

    ColumnValue cur = evaluate_expression(src[i], &empty_row, schema, db, schema_idx);

    bool valid_conversion = infer_and_cast_value(&cur, &(schema->columns[i]));

    if (!valid_conversion) {
      LOG_ERROR("Invalid conversion whilst trying to insert row");
      return NULL;
    }
        
    // if (schema->columns[i].is_foreign_key) {
    //   if (!check_foreign_key(db, schema->columns[i], cur)) {
    //     LOG_ERROR("Foreign key constraint evaluation failed: \n> %s does not match any %s.%s",
    //       str_column_value(&cur), schema->columns[i].foreign_table, schema->columns[i].foreign_column);
    //     return NULL;
    //   }
    // }

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
      null_bitmap[i / 8] &= ~(1 << (i % 8));
    }

    if (row->values[i].is_null && schema->columns[i].has_default && schema->columns[i].default_value) {      
      row->values[i] = *schema->columns[i].default_value; 
      null_bitmap[i / 8] &= ~(1 << (i % 8));
      row->values[i].is_null = false;
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

  if (!is_unsafe) {
    // LOG_INFO("========= validating constraints for: %d", table_id);
    if (!validate_all_constraints(db, table_id, row->values, schema->column_count)) {
      free(row->values);
      free(row->null_bitmap);
      return NULL;
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

  TableSchema* schema = get_table_schema(db, cmd->schema->table_name);
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

  bool* is_aggregate_col = calloc(schema->column_count, sizeof(bool));
  ColumnValue* aggregate_results = calloc(schema->column_count, sizeof(ColumnValue));

  for (int j = 0; j < schema->column_count; j++) {
    ExprNode* expr = cmd->sel_columns[j].expr;
    
    if (expr && expr->type == EXPR_FUNCTION && expr->fn.type != NOT_AGG) {
      is_aggregate_col[j] = true;
      aggregate_results[j] = evaluate_aggregate(expr, collected_rows, total_found, schema, db, schema_idx);
    }
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
      free(is_aggregate_col);
      free(aggregate_results);
      return (ExecutionResult){1, "Memory allocation failed for projected values"};
    }
        
  
    for (int k = 0; k < schema->column_count; k++) {
      dst->values[k].is_null = true;
    }

    int col_count = cmd->value_counts[0];

    for (int j = 0; j < col_count; j++) {
      ExprNode* expr = cmd->sel_columns[j].expr;
      ColumnValue val = {0};

      if (cmd->sel_columns[j].alias) {
        aliases[j] = strdup(cmd->sel_columns[j].alias);
      } else if (expr->type == EXPR_ARRAY_ACCESS) {
        int base_idx = expr->column.index;
        int array_idx = expr->column.array_idx->literal.int_value;
        const char* base_name = cmd->schema->columns[base_idx].name;
        char buffer[256];
        snprintf(buffer, sizeof(buffer), "%s[%d]", base_name, array_idx);
        aliases[j] = strdup(buffer);
      } else if (expr->type == EXPR_FUNCTION) {
        aliases[j] = strdup(expr->fn.name);
      } else {
        aliases[j] = strdup(cmd->schema->columns[expr->column.index].name);
      }
      
      if (!expr) {
        dst = src;
        continue;
      }
      
      if (is_aggregate_col[j]) {
        dst->values[j] = aggregate_results[j];
      } else {
        val = evaluate_expression(expr, src, schema, db, schema_idx);
        dst->values[j] = val;
      }
    }
  }

  free(is_aggregate_col);
  free(aggregate_results);
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
  
  TableSchema* schema = get_table_schema(db, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(db, schema->table_name);

  int64_t table_id = find_table(db, schema->table_name);
  if (table_id == -1) {
    return (ExecutionResult){-1, "Table not found in catalog"};
  }

  int fk_count = 0;
  Constraint* referencing_fks = get_fk_constr_ref_table(db, table_id, &fk_count);

  for (int i = 0; i < fk_count; i++) {
    Constraint* fk = &referencing_fks[i];
    if (fk->ref_column_count != fk->column_count) {
      return (ExecutionResult){1, "Foreign key constraint validation failed: ref_column_count != column_count"};
    }
    if (!fk->on_update) {
      return (ExecutionResult){1, "UPDATE restricted by foreign key constraint - ON UPDATE not enabled"};
    }
  }

  RowSet update_set = {malloc(sizeof(RowID) * 4096), 0, 4096};
  FKConstraintValues* old_fk = malloc(sizeof(FKConstraintValues) * fk_count);
  FKConstraintValues* new_fk = malloc(sizeof(FKConstraintValues) * fk_count);

  if (!update_set.rows || !old_fk || !new_fk) {
    free(update_set.rows);
    free(old_fk);
    free(new_fk);
    return (ExecutionResult){1, "OOM"};
  }

  if (!init_fk_constraints(old_fk, referencing_fks, fk_count) ||
      !init_fk_constraints(new_fk, referencing_fks, fk_count)) {
    cleanup_fk_constraints(old_fk, fk_count);
    cleanup_fk_constraints(new_fk, fk_count);
    free(old_fk);
    free(new_fk);
    free(update_set.rows);
    return (ExecutionResult){1, "OOM"};
  }

  ExecutionResult result = collect_fk_tuples_update(db, schema, cmd, referencing_fks, fk_count,
                                                   &update_set, old_fk, new_fk);
  if (result.code != 0) {
    cleanup_fk_constraints(old_fk, fk_count);
    cleanup_fk_constraints(new_fk, fk_count);
    free(old_fk);
    free(new_fk);
    free(update_set.rows);
    return result;
  }
  
  LOG_DEBUG("Collected unique FK key tuples across %d constraints", fk_count);
  for (int fk_idx = 0; fk_idx < fk_count; fk_idx++) {
    Constraint* fk = &referencing_fks[fk_idx];
    LOG_DEBUG("Constraint %s has %d unique key tuples", fk->name, old_fk[fk_idx].count);
    
    if (!handle_on_update_constraints(db, fk, &(old_fk[fk_idx]), &(new_fk[fk_idx]))) {
      cleanup_fk_constraints(old_fk, fk_count);
      cleanup_fk_constraints(new_fk, fk_count);
      free(old_fk);
      free(new_fk);
      free(update_set.rows);
      return (ExecutionResult){1, "UPDATE restricted by foreign constraint"};
    }
  }


  result = perform_updates(db, schema, cmd, &update_set);

  cleanup_fk_constraints(old_fk, fk_count);
  cleanup_fk_constraints(new_fk, fk_count);
  free(old_fk);
  free(new_fk);
  free(update_set.rows);

  return result;
}

ExecutionResult execute_delete(Database* db, JQLCommand* cmd) {
  if (!db || !cmd || !cmd->schema) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  TableSchema* schema = get_table_schema(db, cmd->schema->table_name);
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  load_btree_cluster(db, schema->table_name);
  cmd->schema = schema;

  int64_t table_id = find_table(db, schema->table_name);
  if (table_id == -1) {
    return (ExecutionResult){-1, "Table not found in catalog"};
  }

  int fk_count = 0;
  Constraint* referencing_fks = get_fk_constr_ref_table(db, table_id, &fk_count);

  for (int i = 0; i < fk_count; i++) {
    Constraint* fk = &referencing_fks[i];
    if (fk->ref_column_count != fk->column_count) {
      return (ExecutionResult){1, "Foreign key constraint validation failed: ref_column_count != column_count"};
    }
  }

  RowSet delete_set = {malloc(sizeof(RowID) * 4096), 0, 4096};
  FKConstraintValues* fk_constraints = malloc(sizeof(FKConstraintValues) * fk_count);

  if (!delete_set.rows || !fk_constraints) {
    free(delete_set.rows); 
    free(fk_constraints);
    return (ExecutionResult){1, "OOM"};
  }

  if (!init_fk_constraints(fk_constraints, referencing_fks, fk_count)) {
    cleanup_fk_constraints(fk_constraints, fk_count);
    free(fk_constraints);
    free(delete_set.rows);
    return (ExecutionResult){1, "OOM"};
  }

  ExecutionResult result = collect_fk_tuples_delete(db, schema, cmd, referencing_fks, fk_count,
                                                   &delete_set, fk_constraints);
  if (result.code != 0) {
    cleanup_fk_constraints(fk_constraints, fk_count);
    free(fk_constraints);
    free(delete_set.rows);
    return result;
  }

  LOG_DEBUG("Collected unique FK key tuples across %d constraints", fk_count);
  for (int fk_idx = 0; fk_idx < fk_count; fk_idx++) {
    Constraint* fk = &referencing_fks[fk_idx];
    LOG_DEBUG("Constraint %s has %d unique key tuples", fk->name, fk_constraints[fk_idx].count);
    
    if (!handle_on_delete_constraints(db, fk, &fk_constraints[fk_idx])) {
      cleanup_fk_constraints(fk_constraints, fk_count);
      free(fk_constraints);
      free(delete_set.rows);
      return (ExecutionResult){1, "DELETE restricted by foreign constraint"};
    }
  }

  result = perform_deletes(db, schema, &delete_set);

  cleanup_fk_constraints(fk_constraints, fk_count);
  free(fk_constraints);
  free(delete_set.rows);

  return result;
}