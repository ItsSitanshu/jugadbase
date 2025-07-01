#include "kernel/kernel.h"

bool init_fk_constraints(FKConstraintValues* fk_constraints, Constraint* referencing_fks, int count) {
  for (int i = 0; i < count; i++) {
    fk_constraints[i].values = malloc(sizeof(ColumnValue) * 256 * referencing_fks[i].ref_column_count);
    fk_constraints[i].count = 0;
    fk_constraints[i].capacity = 256;
    fk_constraints[i].column_idx = 0;
    
    if (!fk_constraints[i].values) {
      for (int j = 0; j < i; j++) {
        free(fk_constraints[j].values);
      }
      return false;
    }
  }
  return true;
}

int64_t find_default_constraint(Database* db, int64_t table_id, const char* column_name) {
  if (!db || !column_name) {
    LOG_ERROR("Invalid parameters to find_default_constraint");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);


  char query[512];
  snprintf(query, sizeof(query),
    "SELECT id FROM jb_attrdef WHERE table_id = %ld AND column_name = '%s';",
    table_id, column_name
  );

  // LOG_DEBUG("[+] looking for default constraint: %s", query);

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0 && res.exec.row_count > 0;

  int64_t value = -1;
  if (success) {
    value = res.exec.rows[0].values[0].int_value;
  } 

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
}


int64_t insert_constraint(Database* db, int64_t table_id, char* name, 
                          int constraint_type, char (*columns)[MAX_IDENTIFIER_LEN], int col_count,
                          char* check_expr, int ref_table, 
                          char (*ref_columns)[MAX_IDENTIFIER_LEN], int ref_col_count,
                          int on_delete, int on_update) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_constraint");
    return -1;
  }

  if (!db->core) db->core = db;

  char columns_array[512];
  size_t pos = 0;
  pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, "{");
  for (int i = 0; i < col_count && pos < sizeof(columns_array); i++) {
    if (i > 0) pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, ",");
    pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, "'%s'", columns[i]);
  }
  pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, "}");

  char ref_columns_array[512];
  pos = 0;
  pos += snprintf(ref_columns_array + pos, sizeof(ref_columns_array) - pos, "{");
  if (ref_columns && ref_col_count > 0) {
    for (int i = 0; i < ref_col_count && pos < sizeof(ref_columns_array); i++) {
      if (i > 0) pos += snprintf(ref_columns_array + pos, sizeof(ref_columns_array) - pos, ",");
      pos += snprintf(ref_columns_array + pos, sizeof(ref_columns_array) - pos, "'%s'", ref_columns[i]);
    }
  }
  pos += snprintf(ref_columns_array + pos, sizeof(ref_columns_array) - pos, "}");
  
  char ref_table_buf[16];
  char* ref_table_str = (ref_table != -1) ? (snprintf(ref_table_buf, sizeof(ref_table_buf), "%d", ref_table), ref_table_buf) : "NULL";

  char* check = process_str_arg(check_expr);
  char** flags = CONSTRAINT_FLAGS[constraint_type];

  ParserState state = parser_save_state(db->core->parser);

  // LOG_DEBUG("ref array: %s", ref_columns_array);

  char query[2048];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_constraints "
    "(table_id, columns, name, constraint_type, check_expr, ref_table, ref_columns, "
    "on_delete, on_update, is_deferrable, is_deferred, is_nullable, is_primary, is_unique, created_at) "
    "VALUES (%ld, \"%s\", \"%s\", %d, %s, %s, \"%s\", %d, %d, %s, %s, %s, %s, %s, NOW()) RETURNING id;",
    table_id,
    columns_array,
    name,
    constraint_type,
    check,
    ref_table_str, 
    ref_columns_array,
    on_delete,
    on_update,
    flags[0],
    flags[1], 
    flags[2],
    flags[3],
    flags[4]
  );

  LOG_DEBUG("[+] constraint: %s", query);

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  if (!success) {
    LOG_ERROR("Failed to insert constraint '%s'", name);
    return -1;
  }

  int64_t value = res.exec.rows[0].values[0].int_value;
  parser_restore_state(db->core->parser, state);

  return value;
}

int64_t find_constraint_by_name(Database* db, int64_t table_id, const char* name) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to find_constraint_by_name");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[512];
  snprintf(query, sizeof(query),
    "SELECT id FROM jb_constraints WHERE table_id = %ld AND name = '%s';",
    table_id, name
  );

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0 && res.exec.row_count > 0;

  int64_t value = -1;
  if (success) {
    value = res.exec.rows[0].values[0].int_value;
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
}

bool delete_constraint(Database* db, int64_t constraint_id) {
  if (!db) {
    LOG_ERROR("Invalid parameters to delete_constraint");
    return false;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[256];
  snprintf(query, sizeof(query),
    "DELETE FROM jb_constraints WHERE id = %ld;",
    constraint_id
  );

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return success;
}

bool update_constraint_name(Database* db, int64_t constraint_id, const char* new_name) {
  if (!db || !new_name) {
    LOG_ERROR("Invalid parameters to update_constraint_name");
    return false;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[512];
  snprintf(query, sizeof(query),
    "UPDATE jb_constraints SET name = '%s' WHERE id = %ld;",
    new_name, constraint_id
  );

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return success;
}

int64_t insert_single_column_constraint(Database* db, int64_t table_id, int64_t column_id, 
                                       const char* name, uint32_t constraint_type, bool is_nullable,
                                       bool is_unique, bool is_primary) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_single_column_constraint");
    return -1;
  }

  char column_arr[32];
  snprintf(column_arr, sizeof(column_arr), "{%ld}", column_id);

  char query[1024];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_constraints "
    "(table_id, column_names, name, constraint_type, is_nullable, is_array, is_primary, created_at) "
    "VALUES (%ld, '%s', '%s', %d, %d, %d, %d, NOW()) RETURNING id;",
    table_id,
    column_arr,
    name,
    (int)constraint_type,
    is_nullable ? "true" : "false",
    is_unique ? "true" : "false",
    is_primary ? "true" : "false"
  );

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  if (res.exec.code != 0) {
    LOG_ERROR("Failed to insert constraint '%s'", name);
    return -1;
  }

  return res.exec.rows[0].values[0].int_value;
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

// Parse TEXT[] column from database
char** parse_text_array(const char* text_array_str, int* count) {
  if (!text_array_str || !count) {
    *count = 0;
    return NULL;
  }

  // Remove curly braces and split by comma
  char* str_copy = strdup(text_array_str);
  if (str_copy[0] == '{') {
    memmove(str_copy, str_copy + 1, strlen(str_copy));
  }
  
  char* end = str_copy + strlen(str_copy) - 1;
  if (*end == '}') {
    *end = '\0';
  }

  // Count elements
  *count = 1;
  for (char* p = str_copy; *p; p++) {
    if (*p == ',') (*count)++;
  }


  char** result = calloc((*count) + 1, sizeof(char*));
  char* token = strtok(str_copy, ",");
  int i = 0;


  // LOG_DEBUG("token %s , i %d", token, i);
  
  while (token && i < *count) {
    // LOG_DEBUG("token %s , i %d", token, i);

    while (*token == ' ') token++;
    char* end_trim = token + strlen(token) - 1;
    while (end_trim > token && *end_trim == ' ') {
      *end_trim = '\0';
      end_trim--;
    }
    
    result[i] = strdup(token);
    token = strtok(NULL, ",");
    i++;
  }


  free(str_copy);
  return result;
}

Constraint parse_constraint_from_row(Row* row) {
  Constraint constraint = {0};
  
  int out_count = 0;
  int ref_out_count = 0;

  constraint.id = row->values[0].int_value;
  constraint.table_id = row->values[1].int_value;
  constraint.columns = stringify_column_array(&row->values[2], &out_count);
  constraint.column_count = (int)out_count;
  constraint.name = strdup(row->values[3].str_value);
  constraint.constraint_type = (ConstraintType)row->values[4].int_value;
  constraint.check_expr = row->values[5].str_value ? strdup(row->values[5].str_value) : NULL;
  constraint.ref_table_id = row->values[6].int_value;
  constraint.ref_columns = stringify_column_array(&row->values[7], &ref_out_count);
  constraint.ref_column_count = ref_out_count;
  constraint.on_delete = (FKAction)row->values[8].int_value;
  constraint.on_update = (FKAction)row->values[9].int_value;
  constraint.is_deferrable = row->values[10].bool_value;
  constraint.is_deferred = row->values[11].bool_value;
  constraint.is_nullable = row->values[12].bool_value;
  constraint.is_primary = row->values[13].bool_value;
  constraint.is_unique = row->values[14].bool_value;

  return constraint;
}

void free_constraint(Constraint* constraint) {
  if (constraint) {
    if (constraint->columns) {
      for (int i = 0; i < constraint->column_count; i++) {
        free(constraint->columns[i]);
      }
      free(constraint->columns);
    }
    if (constraint->ref_columns) {
      for (int i = 0; i < constraint->ref_column_count; i++) {
        free(constraint->ref_columns[i]);
      }
      free(constraint->ref_columns);
    }
    free(constraint->name);
    free(constraint->check_expr);
  }
}

Result get_table_constraints(Database* db, int64_t table_id) {
  if (!db) {
    LOG_ERROR("Invalid database parameter");
    Result empty_result = {0};
    return empty_result;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[512];
  snprintf(query, sizeof(query),
    "SELECT id, table_id, columns, name, constraint_type, check_expr, "
    "ref_table, ref_columns, on_delete, on_update, is_deferrable, "
    "is_deferred, is_nullable, is_primary, is_unique "
    "FROM jb_constraints WHERE table_id = %ld;",
    table_id
  );

  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  return res;
}

bool validate_not_null_constraint(Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {  
  for (int i = 0; i < constraint->column_count; i++) {
    int column_idx = find_column_index(schema, constraint->columns[i]);
    if (column_idx >= 0 && column_idx < value_count) {
      if (values[column_idx].is_null) {
        LOG_ERROR("NOT NULL constraint '%s' violated for column '%s'", 
          constraint->name, constraint->columns[i]);
        return false;
      }
    }
  }
  return true;
}

bool validate_unique_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {
  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  // Build WHERE clause for uniqueness check
  char where_clause[1024] = {0};
  bool first = true;
  
  for (int i = 0; i < constraint->column_count; i++) {
    int column_idx = find_column_index(schema, constraint->columns[i]);

    if (column_idx >= 0 && column_idx < value_count) {
      if (!first) {
        strncat(where_clause, " AND ", 5);
      }

      char value_str[256] = {0};
      format_column_value(value_str, sizeof(value_str), &values[column_idx]);

      char condition[512] = {0};
      snprintf(condition, sizeof(condition), "%s = %s", constraint->columns[i], value_str);

      strncat(where_clause, condition, strlen(condition));

      first = false;
    } else {
      printf("Skipping column: index invalid or out of value range.\n");
    }
  }

  char query[2048];
  snprintf(query, sizeof(query),
    "SELECT COUNT() FROM %s WHERE %s;", schema->table_name, where_clause);
      
  Result res = process_silent(db, query);
  bool is_unique = (res.exec.code == 0 && res.exec.row_count <= 0);

  if (!is_unique) {
    LOG_ERROR("UNIQUE constraint '%s' violated", constraint->name);
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return is_unique;
}

bool validate_primary_key_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {
  return validate_not_null_constraint(constraint, schema, values, value_count) &&
         validate_unique_constraint(db, constraint, schema, values, value_count);
}

bool validate_foreign_key_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {
  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  TableSchema* ref_schema = get_table_schema_by_id(db, constraint->ref_table_id);
  
  if (!ref_schema) {
    LOG_ERROR("Referenced table not found for constraint '%s'", constraint->name);
    parser_restore_state(db->core->parser, state);
    return false;
  }

  char where_clause[1024] = {0};
  bool first = true;
  
  for (int i = 0; i < constraint->column_count && i < constraint->ref_column_count; i++) {
    int column_idx = find_column_index(schema, constraint->columns[i]);
    if (column_idx >= 0 && column_idx < value_count) {
      if (!first) {
        strcat(where_clause, " AND ");
      }
      
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[column_idx]);
      
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", 
        constraint->ref_columns[i], value_str);
      strcat(where_clause, condition);
      
      first = false;
    }
  }

  if (!where_clause) return true;

  char query[2048];
  snprintf(query, sizeof(query),
    "SELECT COUNT() FROM %s WHERE %s;", ref_schema->table_name, where_clause);

  Result res = process_silent(db, query);
  bool fk_valid = (res.exec.code == 0 && res.exec.row_count > 0 && 
                   res.exec.rows[0].values[0].int_value > 0);

  if (!fk_valid) {
    LOG_ERROR("FOREIGN KEY constraint '%s' violated", constraint->name);
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return fk_valid;
}

// Validate CHECK constraint
bool validate_check_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {
  if (!constraint->check_expr) {
    return true; // No check expression means constraint passes
  }

  // Simple check expression evaluation
  // This is a basic implementation - a full implementation would need expression parsing
  ParserState state = parser_save_state(db->core->parser);

  // Create a temporary query to evaluate the check expression
  char query[2048];
  snprintf(query, sizeof(query), "SELECT (%s) AS check_result;", constraint->check_expr);

  Result res = process_silent(db->core, query);
  bool check_passed = (res.exec.code == 0 && res.exec.row_count > 0 && 
                       res.exec.rows[0].values[0].bool_value);

  if (!check_passed) {
    LOG_ERROR("CHECK constraint '%s' violated", constraint->name);
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return check_passed;
}

// Validate a single constraint
bool validate_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {
  if (!db || !constraint || !schema) {
    LOG_ERROR("Invalid parameters to validate_constraint");
    return false;
  }

  if (!db->core) db->core = db;  

  switch (constraint->constraint_type) {
    case CONSTRAINT_UNIQUE:
      return validate_unique_constraint(db, constraint, schema, values, value_count);
    
    case CONSTRAINT_PRIMARY_KEY:
      return validate_primary_key_constraint(db, constraint, schema, values, value_count);
    
    case CONSTRAINT_FOREIGN_KEY:
      return validate_foreign_key_constraint(db, constraint, schema, values, value_count);
    
    case CONSTRAINT_CHECK:
      return validate_check_constraint(db, constraint, schema, values, value_count);
    
    default:
      LOG_WARN("Unknown constraint type: %d", constraint->constraint_type);
      return true; // Allow unknown constraints to pass
  }
}


void cleanup_fk_constraints(FKConstraintValues* fk_constraints, int count) {
  for (int i = 0; i < count; i++) {
    free(fk_constraints[i].values);
  }
}

bool expand_row_set(RowSet* set) {
  if (set->count < set->capacity) return true;
  
  set->capacity <<= 1;
  RowID* new_rows = realloc(set->rows, sizeof(RowID) * set->capacity);
  if (!new_rows) return false;
  
  set->rows = new_rows;
  return true;
}

bool expand_fk_constraint(FKConstraintValues* fk_constraint, int ref_col_count) {
  if (fk_constraint->count < fk_constraint->capacity) return true;
  
  fk_constraint->capacity <<= 1;
  ColumnValue* new_vals = realloc(fk_constraint->values, 
                                 sizeof(ColumnValue) * fk_constraint->capacity * ref_col_count);
  if (!new_vals) return false;
  
  fk_constraint->values = new_vals;
  return true;
}

bool tuple_exists(FKConstraintValues* fk_constraint, ColumnValue* key_tuple, 
                        Constraint* fk, TableSchema* schema) {
  for (uint32_t i = 0; i < fk_constraint->count; i++) {
    ColumnValue* existing = &fk_constraint->values[i * fk->ref_column_count];
    bool match = true;
    
    for (uint8_t col_idx = 0; col_idx < fk->ref_column_count; col_idx++) {
      int schema_col_idx = find_column_index(schema, fk->ref_columns[col_idx]);
      void* existing_val = get_column_value_as_pointer(&existing[col_idx]);
      void* new_val = get_column_value_as_pointer(&key_tuple[col_idx]);
      int type = schema->columns[schema_col_idx].type;
      
      if (key_compare(existing_val, new_val, type) != 0) {
        match = false;
        break;
      }
    }
    
    if (match) return true;
  }
  return false;
}

bool store_fk_tuple(FKConstraintValues* fk_constraint, ColumnValue* key_tuple, 
                          Constraint* fk, TableSchema* schema) {
  if (tuple_exists(fk_constraint, key_tuple, fk, schema)) return true;
  
  if (!expand_fk_constraint(fk_constraint, fk->ref_column_count)) return false;
  
  ColumnValue* dest = &fk_constraint->values[fk_constraint->count * fk->ref_column_count];
  for (uint8_t col_idx = 0; col_idx < fk->ref_column_count; col_idx++) {
    dest[col_idx] = key_tuple[col_idx];
  }
  fk_constraint->count++;
  return true;
}

bool extract_fk_tuple(Row* row, TableSchema* schema, Constraint* fk, ColumnValue* tuple) {
  for (uint8_t col_idx = 0; col_idx < fk->ref_column_count; col_idx++) {
    int schema_col_idx = find_column_index(schema, fk->ref_columns[col_idx]);
    if (schema_col_idx == -1) return false;
    tuple[col_idx] = row->values[schema_col_idx];
  }
  return true;
}

ExecutionResult collect_fk_tuples_update(Database* db, TableSchema* schema, JQLCommand* cmd,
                                               Constraint* referencing_fks, int fk_count,
                                               RowSet* update_set, FKConstraintValues* old_fk,
                                               FKConstraintValues* new_fk) {
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &db->lake[schema_idx];
  
  for (uint16_t page_idx = 0; page_idx < pool->num_pages; ++page_idx) {
    Page* page = pool->pages[page_idx];
    if (!page || page->num_rows == 0) continue;
  
    for (uint16_t row_idx = 0; row_idx < page->num_rows; ++row_idx) {
      Row* row = &page->rows[row_idx];
      
      if (row->deleted || !row) continue;
      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, db, schema_idx)) continue;

      if (!expand_row_set(update_set)) return (ExecutionResult){1, "OOM"};
      update_set->rows[update_set->count++] = (RowID){page_idx, row_idx};

      Row temp_row = *row;
      int max_updates = cmd->value_counts[0];
      
      for (int k = 0; k < max_updates; ++k) {
        int col_index = cmd->update_columns[k].index;
        ColumnValue eval = evaluate_expression(cmd->values[0][k], row, schema, db, schema_idx);
        ColumnValue array_idx = evaluate_expression(cmd->update_columns->array_idx, row, schema, db, schema_idx);
        
        if (!infer_and_cast_value(&eval, &schema->columns[col_index])) {
          return (ExecutionResult){-1, "Invalid conversion whilst trying to update row"};
        }

        if (schema->columns[col_index].is_foreign_key && !check_foreign_key(db, schema->columns[col_index], eval)) {
          return (ExecutionResult){-1, "Foreign key constraint restricted UPDATE"};
        }

        if (!is_struct_zeroed(&array_idx, sizeof(ColumnValue))) {
          temp_row.values[col_index].array.array_value[array_idx.int_value] = eval;
        } else {
          temp_row.values[col_index] = eval;
        }
      }

      for (int fk_idx = 0; fk_idx < fk_count; fk_idx++) {
        Constraint* fk = &referencing_fks[fk_idx];
        
        ColumnValue* old_tuple = malloc(sizeof(ColumnValue) * fk->ref_column_count);
        ColumnValue* new_tuple = malloc(sizeof(ColumnValue) * fk->ref_column_count);
        
        if (!old_tuple || !new_tuple) {
          free(old_tuple);
          free(new_tuple);
          return (ExecutionResult){1, "OOM"};
        }
        
        bool valid_old = extract_fk_tuple(row, schema, fk, old_tuple);
        bool valid_new = extract_fk_tuple(&temp_row, schema, fk, new_tuple);
        
        if (valid_old && valid_new) {
          if (!store_fk_tuple(&old_fk[fk_idx], old_tuple, fk, schema) ||
              !store_fk_tuple(&new_fk[fk_idx], new_tuple, fk, schema)) {
            free(old_tuple);
            free(new_tuple);
            return (ExecutionResult){1, "OOM"};
          }
        }
        
        free(old_tuple);
        free(new_tuple);
      }
    }
  }
  return (ExecutionResult){0, "Success"};
}

ExecutionResult collect_fk_tuples_delete(Database* db, TableSchema* schema, JQLCommand* cmd,
                                               Constraint* referencing_fks, int fk_count,
                                               RowSet* delete_set, FKConstraintValues* fk_constraints) {
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &db->lake[schema_idx];

  for (uint16_t page_idx = 0; page_idx < pool->num_pages; page_idx++) {
    Page* page = pool->pages[page_idx];
    if (!page || page->num_rows == 0) continue;

    for (uint16_t row_idx = 0; row_idx < page->num_rows; row_idx++) {
      Row* row = &page->rows[row_idx];

      if (is_struct_zeroed(row, sizeof(Row)) || row->deleted) continue;
      if (cmd->has_where && !evaluate_condition(cmd->where, row, schema, db, schema_idx)) continue;

      if (!expand_row_set(delete_set)) return (ExecutionResult){1, "OOM"};
      delete_set->rows[delete_set->count++] = (RowID){page_idx, row_idx};

      for (int fk_idx = 0; fk_idx < fk_count; fk_idx++) {
        Constraint* fk = &referencing_fks[fk_idx];
        
        ColumnValue* key_tuple = malloc(sizeof(ColumnValue) * fk->ref_column_count);
        if (!key_tuple) return (ExecutionResult){1, "OOM"};
        
        if (extract_fk_tuple(row, schema, fk, key_tuple)) {
          if (!store_fk_tuple(&fk_constraints[fk_idx], key_tuple, fk, schema)) {
            free(key_tuple);
            return (ExecutionResult){1, "OOM"};
          }
        }
        
        free(key_tuple);
      }
    }
  }
  return (ExecutionResult){0, "Success"};
}

ExecutionResult perform_updates(Database* db, TableSchema* schema, JQLCommand* cmd, RowSet* update_set) {
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &db->lake[schema_idx];
  size_t null_bitmap_size = (schema->column_count + 7) / 8;
  uint32_t rows_updated = 0;

  for (uint32_t i = 0; i < update_set->count; i++) {
    uint16_t page_idx = update_set->rows[i].page_id;
    uint16_t row_idx = update_set->rows[i].row_id;
    
    Page* page = pool->pages[page_idx];
    Row* row = &page->rows[row_idx];

    int max_updates = cmd->value_counts[0];
    UpdateData upd = {
      .cols = malloc(sizeof(uint16_t) * max_updates),
      .old_vals = malloc(sizeof(ColumnValue) * max_updates),
      .new_vals = malloc(sizeof(ColumnValue) * max_updates),
      .count = 0
    };

    if (!upd.cols || !upd.old_vals || !upd.new_vals) {
      free(upd.cols);
      free(upd.old_vals);
      free(upd.new_vals);
      return (ExecutionResult){1, "OOM"};
    }

    for (int k = 0; k < max_updates; ++k) {
      int col_index = cmd->update_columns[k].index;
      ColumnValue eval = evaluate_expression(cmd->values[0][k], row, schema, db, schema_idx);
      ColumnValue array_idx = evaluate_expression(cmd->update_columns->array_idx, row, schema, db, schema_idx);
      
      if (!infer_and_cast_value(&eval, &schema->columns[col_index])) {
        free(upd.cols);
        free(upd.old_vals);
        free(upd.new_vals);
        return (ExecutionResult){-1, "Invalid conversion whilst trying to update row"};
      }

      upd.cols[upd.count] = col_index;
      upd.old_vals[upd.count] = row->values[col_index];

      if (!is_struct_zeroed(&array_idx, sizeof(ColumnValue))) {
        upd.new_vals[upd.count] = upd.old_vals[upd.count];
        upd.new_vals[upd.count].array.array_value[array_idx.int_value] = eval;
      } else {
        upd.new_vals[upd.count] = eval;
      }
      upd.count++;
    }
    
    if (upd.count > 0) {
      write_update_wal(db->wal, schema_idx, page_idx, row_idx, upd.cols, upd.old_vals, upd.new_vals, upd.count, schema);
      
      for (int u = 0; u < upd.count; ++u) {
        row->values[upd.cols[u]] = upd.new_vals[u];
      }

      if (cmd->bitmap) {
        row->null_bitmap = (uint8_t*)malloc(null_bitmap_size);
        memcpy(row->null_bitmap, cmd->bitmap, null_bitmap_size);
      } else {
        row->null_bitmap = (uint8_t*)calloc(null_bitmap_size, 1);
      }

      rows_updated++;
      page->is_dirty = true;
    }

    free(upd.cols);
    free(upd.old_vals);
    free(upd.new_vals);
  }

  return (ExecutionResult){0, "Update executed successfully", .row_count = rows_updated};
}

ExecutionResult perform_deletes(Database* db, TableSchema* schema, RowSet* delete_set) {
  uint8_t schema_idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  BufferPool* pool = &db->lake[schema_idx];
  uint32_t rows_deleted = 0;

  for (uint32_t i = 0; i < delete_set->count; i++) {
    uint16_t page_idx = delete_set->rows[i].page_id;
    uint16_t row_idx = delete_set->rows[i].row_id;

    Page* page = pool->pages[page_idx];
    Row* row = &page->rows[row_idx];

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
        if (!toast_delete(db, row->values[k].toast_object)) {
          LOG_WARN("Unable to delete TOAST entries \n > run 'fix'");
        }
      }
    }

    RowID id = {page_idx, row_idx + 1};
    serialize_delete(pool, id);

    page->is_dirty = true;
    rows_deleted++;
  }

  return (ExecutionResult){0, "Delete executed successfully", .row_count = rows_deleted};
}

bool cascade_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    return false;
  }

  char where_clause[1024] = {0};
  bool first = true;
  
  for (int i = 0; i < ref_column_count && i < value_count; i++) {
    if (!first) {
      strcat(where_clause, " AND ");
    }
    
    char value_str[256];
    format_column_value(value_str, sizeof(value_str), &values[i]);
    
    char condition[512];
    snprintf(condition, sizeof(condition), "%s = %s", ref_columns[i], value_str);
    strcat(where_clause, condition);
    
    first = false;
  }

  char query[2048];
  snprintf(query, sizeof(query), "DELETE FROM %s WHERE %s;", ref_schema->table_name, where_clause);

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  bool success = (res.exec.code == 0);
  free_result(&res);

  return success;
}

Constraint* get_fk_constr_ref_table(Database* db, int64_t table_id, int* out_count) {
  if (!db || !out_count) return NULL;

  char query[1024];
  snprintf(query, sizeof(query),
    "SELECT id, table_id, columns, name, constraint_type, check_expr, "
    "ref_table, ref_columns, on_delete, on_update, is_deferrable, "
    "is_deferred, is_nullable, is_primary, is_unique "
    "FROM jb_constraints WHERE ref_table = %ld AND constraint_type = %d;",
    table_id, CONSTRAINT_FOREIGN_KEY
  );

  ParserState saved_state = parser_save_state(db->core->parser);
  Result res = process(db->core, query);
  parser_restore_state(db->core->parser, saved_state);

  if (res.exec.code != 0 || res.exec.row_count == 0) {
    free_result(&res);
    *out_count = 0;
    return NULL;
  }

  int count = res.exec.row_count;
  Constraint* constraints = malloc(sizeof(Constraint) * count);
  if (!constraints) {
    free_result(&res);
    *out_count = 0;
    return NULL;
  }

  for (int i = 0; i < count; i++) {
    constraints[i] = parse_constraint_from_row(&res.exec.rows[i]);
  }

  free_result(&res);
  *out_count = count;
  return constraints;
}

bool set_null_on_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    return false;
  }

  // Build SET clause
  char set_clause[512] = {0};
  bool first = true;
  
  for (int i = 0; i < ref_column_count; i++) {
    if (!first) {
      strcat(set_clause, ", ");
    }
    
    char set_part[128];
    snprintf(set_part, sizeof(set_part), "%s = NULL", ref_columns[i]);
    strcat(set_clause, set_part);
    
    first = false;
  }

  // Build WHERE clause
  char where_clause[1024] = {0};
  first = true;
  
  for (int i = 0; i < ref_column_count && i < value_count; i++) {
    if (!first) {
      strcat(where_clause, " AND ");
    }
    
    char value_str[256];
    format_column_value(value_str, sizeof(value_str), &values[i]);
    
    char condition[512];
    snprintf(condition, sizeof(condition), "%s = %s", ref_columns[i], value_str);
    strcat(where_clause, condition);
    
    first = false;
  }

  char query[2048];
  snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s;", 
    ref_schema->table_name, set_clause, where_clause);

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  bool success = (res.exec.code == 0);
  free_result(&res);

  return success;
}

// Set default on delete operation
bool set_default_on_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    return false;
  }

  // Build SET clause with default values
  char set_clause[512] = {0};
  bool first = true;
  
  for (int i = 0; i < ref_column_count; i++) {
    if (!first) {
      strcat(set_clause, ", ");
    }
    
    // Find column definition to get default value
    int col_idx = find_column_index(ref_schema, ref_columns[i]);
    char set_part[128];
    
    if (col_idx >= 0 && ref_schema->columns[col_idx].default_value) {
      snprintf(set_part, sizeof(set_part), "%s = %s", 
        ref_columns[i], ref_schema->columns[col_idx].default_value);
    } else {
      snprintf(set_part, sizeof(set_part), "%s = NULL", ref_columns[i]);
    }
    
    strcat(set_clause, set_part);
    first = false;
  }

  // Build WHERE clause
  char where_clause[1024] = {0};
  first = true;
  
  for (int i = 0; i < ref_column_count && i < value_count; i++) {
    if (!first) {
      strcat(where_clause, " AND ");
    }
    
    char value_str[256];
    format_column_value(value_str, sizeof(value_str), &values[i]);
    
    char condition[512];
    snprintf(condition, sizeof(condition), "%s = %s", ref_columns[i], value_str);
    strcat(where_clause, condition);
    
    first = false;
  }

  char query[2048];
  snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s;", 
    ref_schema->table_name, set_clause, where_clause);

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  bool success = (res.exec.code == 0);
  free_result(&res);

  return success;
}

bool check_no_del_references(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    return false;
  }

  char where_clause[1024] = {0};
  bool first = true;

  for (int i = 0; i < value_count; i++) {
    LOG_DEBUG("at %i %s", i, str_column_value(&(values[i])));
  }
  
  for (int i = 0; i < ref_column_count && i < value_count; i++) {
    if (!first) {
      strncat(where_clause, " AND ", sizeof(where_clause) - strlen(where_clause) - 1);
    }

    int idx = find_column_index(ref_schema, ref_columns[i]);
    
    char value_str[256];
    LOG_DEBUG("At %s/%d got %s", ref_columns[i], idx, str_column_value(&values[i]));
    format_column_value(value_str, sizeof(value_str), &values[i]);
    
    char condition[512];
    snprintf(condition, sizeof(condition), "%s = %s", ref_columns[i], value_str);
    strncat(where_clause, condition, sizeof(where_clause) - strlen(where_clause) - 1);
    
    first = false;
  }

  char query[2048];
  snprintf(query, sizeof(query), "SELECT COUNT() FROM %s WHERE %s;", 
    ref_schema->table_name, where_clause);

  LOG_DEBUG("[~frnkey no references]: %s", query);

  ParserState state = parser_save_state(db->core->parser);
  Result res = process(db, query);
  parser_restore_state(db->core->parser, state);

  bool no_references = false;
  if (res.exec.code == 0 && res.exec.row_count == 1) {
    // char* count_str = res.exec.rows[0].columns[0];
    // int count = atoi(count_str);
    no_references = true;
  }

  if (!no_references) {
    LOG_INFO("Operation restricted due to foreign key references in table '%s'", 
      ref_schema->table_name);
  }

  free_result(&res);

  return no_references;
}

bool check_no_references(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) return false;

  for (int i = 0; i < value_count; i++) {
    char where_clause[512] = {0};
    bool first = true;

    for (int j = 0; j < ref_column_count; j++) {
      if (!first) strcat(where_clause, " AND ");

      int idx = find_column_index(ref_schema, ref_columns[j]);
      if (idx == -1) {
        LOG_WARN("Reference column '%s' not found in schema '%s'", ref_columns[j], ref_schema->table_name);
        return false;
      }

      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[i * ref_column_count + j]);

      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", ref_columns[j], value_str);
      strcat(where_clause, condition);

      first = false;
    }

    char query[1024];
    snprintf(query, sizeof(query), "SELECT COUNT() FROM %s WHERE %s;", ref_schema->table_name, where_clause);

    LOG_DEBUG("[~frnkey no references]: %s", query);

    ParserState state = parser_save_state(db->core->parser);
    Result res = process(db, query);
    parser_restore_state(db->core->parser, state);

    bool no_references = (res.exec.code == 0 && res.exec.row_count == 0);

    if (!no_references) {
      LOG_INFO("Operation restricted due to foreign key references in table '%s'", ref_schema->table_name);
      free_result(&res);
      return false; 
    }

    free_result(&res);
  }

  return true;
}


// Cascade update operation
bool cascade_update(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* old_values, ColumnValue* new_values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    return false;
  }

  // Build SET clause
  char set_clause[512] = {0};
  bool first = true;
  
  for (int i = 0; i < ref_column_count && i < value_count; i++) {
    if (!first) {
      strcat(set_clause, ", ");
    }
    
    char value_str[256];
    format_column_value(value_str, sizeof(value_str), &new_values[i]);
    
    char set_part[256];
    snprintf(set_part, sizeof(set_part), "%s = %s", ref_columns[i], value_str);
    strcat(set_clause, set_part);
    
    first = false;
  }

  char where_clause[1024] = {0};
  first = true;
  
  for (int i = 0; i < ref_column_count && i < value_count; i++) {
    if (!first) {
      strcat(where_clause, " AND ");
    }
    
    char value_str[256];
    format_column_value(value_str, sizeof(value_str), &old_values[i]);
    
    char condition[512];
    snprintf(condition, sizeof(condition), "%s = %s", ref_columns[i], value_str);
    strcat(where_clause, condition);
    
    first = false;
  }

  char query[2048];
  snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s;", 
    ref_schema->table_name, set_clause, where_clause);

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  bool success = (res.exec.code == 0);
  free_result(&res);

  return success;
}

// Handle a single ON DELETE constraint
bool handle_single_on_delete_constraint(Database* db, Constraint* constraint, ColumnValue* values, int value_count) {
  switch (constraint->on_delete) {
    case FK_CASCADE:
      return cascade_delete(db, constraint->table_id, constraint->columns, 
                           constraint->column_count, values, value_count);
    
    case FK_SET_NULL:
      return set_null_on_delete(db, constraint->table_id, constraint->columns, 
                               constraint->column_count, values, value_count);
        
    case FK_RESTRICT:
      return check_no_references(db, constraint->table_id, constraint->columns, 
                                constraint->column_count, values, value_count);
    
    case FK_NO_ACTION:
    default:
      return true; // No action required
  }
}

// Handle a single ON UPDATE constraint
bool handle_single_on_update_constraint(Database* db, Constraint* constraint, ColumnValue* old_values, ColumnValue* new_values, int value_count) {
  switch (constraint->on_update) {
    case FK_CASCADE:
      return cascade_update(db, constraint->table_id, constraint->columns, 
                           constraint->column_count, old_values, new_values, value_count);
    
    case FK_SET_NULL:
      return set_null_on_delete(db, constraint->table_id, constraint->columns, 
                               constraint->column_count, old_values, value_count);
    
    case FK_RESTRICT:
      return check_no_references(db, constraint->table_id, constraint->columns, 
                                constraint->column_count, old_values, value_count);
    
    case FK_NO_ACTION:
    default:
      return true; // No action required
  }
}

bool handle_on_delete_constraints(Database* db, Constraint* constraint, FKConstraintValues* fk_constraint) {
  bool success = true;

  switch (constraint->on_delete) {
    // case FK_CASCADE:
    // case FK_SET_NULL:
    case FK_RESTRICT:
      success = check_no_references(db, constraint->table_id, constraint->columns, 
        constraint->column_count, fk_constraint->values, fk_constraint->count
      );
      break;
    case FK_NO_ACTION:
    default:
      break;
  }

  return success;
}


bool handle_on_update_constraints(Database* db, int64_t table_id, ColumnValue* old_values, ColumnValue* new_values, int value_count) {
  if (!db) {
    LOG_ERROR("Invalid database parameter");
    return false;
  }

  char query[512];
  snprintf(query, sizeof(query),
    "SELECT id, table_id, columns, name, constraint_type, check_expr, "
    "ref_table, ref_columns, on_delete, on_update, is_deferrable, "
    "is_deferred, is_nullable, is_primary, is_unique "
    "FROM jb_constraints WHERE ref_table = %ld AND constraint_type = %d;",
    table_id, CONSTRAINT_FOREIGN_KEY
  );

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  if (res.exec.code != 0) {
    free_result(&res);
    return false;
  }

  bool success = true;
  for (int i = 0; i < res.exec.row_count; i++) {
    Constraint constraint = parse_constraint_from_row(&res.exec.rows[i]);
    
    if (!handle_single_on_update_constraint(db, &constraint, old_values, new_values, value_count)) {
      success = false;
    }
    
    free_constraint(&constraint);
  }

  free_result(&res);
  return success;
}

// Validate all constraints for a table operation
bool validate_all_constraints(Database* db, int64_t table_id, ColumnValue* values, int value_count) {
  if (!db->core) db->core = db;
 
  
  TableSchema* schema = get_table_schema_by_id(db, table_id);
  if (!schema) {
    LOG_ERROR("Could not get table schema for table_id %ld", table_id);
    return false;
  }

  Result constraints = get_table_constraints(db, table_id);
  
  if (constraints.exec.code != 0) {
    free_result(&constraints);
    return false;
  }

  bool all_valid = true;
  for (int i = 0; i < constraints.exec.row_count; i++) {
    Constraint constraint = parse_constraint_from_row(&constraints.exec.rows[i]);
    
    if (!validate_constraint(db, &constraint, schema, values, value_count)) {
      all_valid = false;
    }
    
    free_constraint(&constraint);
  }

  free_result(&constraints);
  
  return all_valid;
}