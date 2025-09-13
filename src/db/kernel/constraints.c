#include "kernel/kernel.h"
#include "storage/syscache.h"


bool init_fk_constraints(FKConstraintValues* fk_constraints, Constraint* referencing_fks, int count) {
  for (int i = 0; i < count; i++) {
    fk_constraints[i].values = xmalloc(sizeof(ColumnValue) * 256 * referencing_fks[i].ref_column_count);
    if (!fk_constraints[i].values) {
      for (int j = 0; j < i; j++) {
        xfree(fk_constraints[j].values);
      }
      return false;
    }
    fk_constraints[i].count = 0;
    fk_constraints[i].capacity = 256;
    fk_constraints[i].column_idx = 0;
  }
  return true;
}

void cleanup_fk_constraints(FKConstraintValues* fk_constraints, int count) {
  for (int i = 0; i < count; i++) {
    xfree(fk_constraints[i].values);
  }
}

void free_constraint(Constraint* constraint) {
  if (!constraint) return;

  if (constraint->columns) {
    for (int i = 0; i < constraint->column_count; i++) {
      xfree(constraint->columns[i]);
    }
    xfree(constraint->columns);
  }

  if (constraint->ref_columns) {
    for (int i = 0; i < constraint->ref_column_count; i++) {
      xfree(constraint->ref_columns[i]);
    }
    xfree(constraint->ref_columns);
  }

  xfree(constraint->name);
  xfree(constraint->check_expr);
}

int64_t insert_constraint(Database* db, int64_t table_id, char* name, int constraint_type,
                          char (*columns)[MAX_IDENTIFIER_LEN], int col_count, char* check_expr,
                          int ref_table, char (*ref_columns)[MAX_IDENTIFIER_LEN],
                          int ref_col_count, int on_delete, int on_update) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_constraint");
    return -1;
  }
  if (!db->core) db->core = db;

  char columns_array[512] = {0};
  size_t pos = 0;
  pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, "{");
  for (int i = 0; i < col_count && pos < sizeof(columns_array); i++) {
    if (i > 0) pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, ",");
    pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, "'%s'", columns[i]);
  }
  pos += snprintf(columns_array + pos, sizeof(columns_array) - pos, "}");

  char ref_columns_array[512] = {0};
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

  char query[2048];
  snprintf(query, sizeof(query),
           "INSERT INTO jb_constraints "
           "(table_id, columns, name, constraint_type, check_expr, ref_table, ref_columns, "
           "on_delete, on_update, is_deferrable, is_deferred, is_nullable, is_primary, is_unique, created_at) "
           "VALUES (%ld, \"%s\", \"%s\", %d, %s, %s, \"%s\", %d, %d, %s, %s, %s, %s, %s, NOW()) RETURNING *;",
           table_id, columns_array, name, constraint_type, check, ref_table_str,
           ref_columns_array, on_delete, on_update, flags[0], flags[1], flags[2], flags[3], flags[4]);

  Result res = process_silent(db->core, query);
  bool success = (res.exec.code == 0) && (res.exec.row_count == 1);

  if (!success) {
    LOG_ERROR("Failed to insert constraint '%s'", name);
    parser_restore_state(db->core->parser, state);
    free_result(&res);
    return -1;
  }

  Constraint constr = parse_constraint_from_row(&(res.exec.rows[0]));
  int64_t value = res.exec.rows[0].values[0].int_value;

  syscache_add_constraint(db->constr_cache, &constr);
  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
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
           "VALUES (%ld, '%s', '%s', %d, %s, %s, %s, NOW()) RETURNING id;",
           table_id, column_arr, name, (int)constraint_type,
           is_nullable ? "true" : "false", is_unique ? "true" : "false",
           is_primary ? "true" : "false");

  ParserState state = parser_save_state(db->core->parser);
  Result res = process_silent(db->core, query);
  parser_restore_state(db->core->parser, state);

  if (res.exec.code != 0) {
    LOG_ERROR("Failed to insert constraint '%s'", name);
    free_result(&res);
    return -1;
  }

  int64_t value = res.exec.rows[0].values[0].int_value;
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
  snprintf(query, sizeof(query), "DELETE FROM jb_constraints WHERE id = %ld;", constraint_id);

  Result res = process_silent(db->core, query);
  bool success = (res.exec.code == 0);

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
  snprintf(query, sizeof(query), "UPDATE jb_constraints SET name = '%s' WHERE id = %ld;", new_name, constraint_id);

  Result res = process_silent(db->core, query);
  bool success = (res.exec.code == 0);

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return success;
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
           table_id, column_name);

  Result res = process_silent(db->core, query);
  int64_t value = -1;

  if (res.exec.code == 0 && res.exec.row_count > 0) {
    value = res.exec.rows[0].values[0].int_value;
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
}

Constraint* get_fk_constr_ref_table(Database* db, int64_t table_id, int* out_count) {
  if (!db || !db->constr_cache || !out_count) {
    if (out_count) *out_count = 0;
    return NULL;
  }

  int count = 0;
  ConstraintCacheEntry *entry, *tmp;
  HASH_ITER(hh_refid, db->constr_cache->by_ref_id, entry, tmp) {
    if (entry->constraint.ref_table_id == table_id) {
      count++;
    }
  }

  if (count == 0) {
    *out_count = 0;
    return NULL;
  }

  Constraint* constraints = xmalloc(sizeof(Constraint) * count);
  if (!constraints) {
    *out_count = 0;
    return NULL;
  }

  int i = 0;
  HASH_ITER(hh_refid, db->constr_cache->by_ref_id, entry, tmp) {
    if (entry->constraint.ref_table_id == table_id) {
      constraints[i++] = entry->constraint;
    }
  }

  *out_count = count;
  return constraints;
}

char** parse_text_array(const char* text_array_str, int* count) {
  if (!text_array_str || !count) {
    if (count) *count = 0;
    return NULL;
  }

  char* str_copy = xstrdup(text_array_str);
  if (str_copy[0] == '{') {
    xmemmove(str_copy, str_copy + 1, strlen(str_copy));
  }

  char* end = str_copy + strlen(str_copy) - 1;
  if (*end == '}') {
    *end = '\0';
  }

  *count = 0;
  if (strlen(str_copy) > 0) {
    *count = 1;
    for (char* p = str_copy; *p; p++) {
      if (*p == ',') (*count)++;
    }
  }

  if (*count == 0) {
    xfree(str_copy);
    return NULL;
  }

  char** result = xcalloc((*count) + 1, sizeof(char*));
  char* token = strtok(str_copy, ",");
  int i = 0;

  while (token && i < *count) {
    while (*token == ' ') token++;
    char* end_trim = token + strlen(token) - 1;
    while (end_trim > token && *end_trim == ' ') {
      *end_trim-- = '\0';
    }
    result[i++] = xstrdup(token);
    token = strtok(NULL, ",");
  }

  xfree(str_copy);
  return result;
}

Constraint parse_constraint_from_row(Row* row) {
  Constraint constraint = {0};
  int out_count = 0;
  int ref_out_count = 0;

  constraint.id = row->values[0].int_value;
  constraint.table_id = row->values[1].int_value;
  constraint.columns = stringify_column_array(&row->values[2], &out_count);
  constraint.column_count = out_count;
  constraint.name = xstrdup(row->values[3].str_value);
  constraint.constraint_type = (ConstraintType)row->values[4].int_value;
  constraint.check_expr = row->values[5].str_value ? xstrdup(row->values[5].str_value) : NULL;
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

bool extract_fk_tuple(Row* row, TableSchema* schema, Constraint* fk, ColumnValue* tuple) {
  for (uint8_t col_idx = 0; col_idx < fk->ref_column_count; col_idx++) {
    int schema_col_idx = find_column_index(schema, fk->ref_columns[col_idx]);
    if (schema_col_idx == -1) return false;
    tuple[col_idx] = row->values[schema_col_idx];
  }
  return true;
}

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
      LOG_WARN("Unknown constraint type: %d called %s", constraint->constraint_type, constraint->name);
      return true; // Allow unknown constraints to pass
  }
}

bool validate_all_constraints(Database* db, int64_t table_id, ColumnValue* values, int value_count) {
  if (!db || !db->constr_cache) {
    LOG_ERROR("Invalid database or cache");
    return false;
  }

  TableSchema* schema = get_table_schema_by_id(db, table_id);
  if (!schema) {
    LOG_ERROR("Could not get table schema for table_id %ld", table_id);
    return false;
  }

  ConstraintListNode* constraints = syscache_get_constraints_by_table(db->constr_cache, table_id);
  if (!constraints) return true;

  bool all_valid = true;
  for (ConstraintListNode* cur = constraints; cur != NULL; cur = cur->next) {
    if (!validate_constraint(db, cur->constraint, schema, values, value_count)) {
      all_valid = false;
      // No break here to report all constraint violations
    }
  }

  free_constraint_list(constraints);
  return all_valid;
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

  char where_clause[1024] = {0};
  bool first = true;
  for (int i = 0; i < constraint->column_count; i++) {
    int column_idx = find_column_index(schema, constraint->columns[i]);
    if (column_idx >= 0 && column_idx < value_count) {
      if (!first) strncat(where_clause, " AND ", sizeof(where_clause) - strlen(where_clause) - 1);

      char value_str[256] = {0};
      format_column_value(value_str, sizeof(value_str), &values[column_idx]);
      char condition[512] = {0};
      snprintf(condition, sizeof(condition), "%s = %s", constraint->columns[i], value_str);
      strncat(where_clause, condition, sizeof(where_clause) - strlen(where_clause) - 1);
      first = false;
    }
  }

  char query[2048];
  snprintf(query, sizeof(query), "SELECT COUNT() FROM %s WHERE %s;", schema->table_name, where_clause);

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
      if (!first) strcat(where_clause, " AND ");

      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[column_idx]);
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", constraint->ref_columns[i], value_str);
      strcat(where_clause, condition);
      first = false;
    }
  }

  if (strlen(where_clause) == 0) return true;

  char query[2048];
  snprintf(query, sizeof(query), "SELECT COUNT() FROM %s WHERE %s;", ref_schema->table_name, where_clause);

  Result res = process_silent(db, query);
  bool fk_valid = (res.exec.code == 0 && res.exec.row_count > 0 && res.exec.rows[0].values[0].int_value > 0);
  if (!fk_valid) {
    LOG_ERROR("FOREIGN KEY constraint '%s' violated", constraint->name);
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);
  return fk_valid;
}

bool validate_check_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count) {
  if (!constraint->check_expr) return true;

  ParserState state = parser_save_state(db->core->parser);
  char query[2048];
  snprintf(query, sizeof(query), "SELECT (%s) AS check_result;", constraint->check_expr);

  Result res = process_silent(db->core, query);
  bool check_passed = (res.exec.code == 0 && res.exec.row_count > 0 && res.exec.rows[0].values[0].bool_value);
  if (!check_passed) {
    LOG_ERROR("CHECK constraint '%s' violated", constraint->name);
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);
  return check_passed;
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
      if (cmd->has_where && !evaluate_condition(cmd->where, &__tup(row), db, NULL, NULL)) continue;

      if (!expand_row_set(delete_set)) return (ExecutionResult){1, "Out of memory"};
      delete_set->rows[delete_set->count++] = (RowID){page_idx, row_idx};

      for (int fk_idx = 0; fk_idx < fk_count; fk_idx++) {
        Constraint* fk = &referencing_fks[fk_idx];
        ColumnValue* key_tuple = xmalloc(sizeof(ColumnValue) * fk->ref_column_count);
        if (!key_tuple) return (ExecutionResult){1, "Out of memory"};

        if (extract_fk_tuple(row, schema, fk, key_tuple)) {
          if (!store_fk_tuple(&fk_constraints[fk_idx], key_tuple, fk, schema)) {
            xfree(key_tuple);
            return (ExecutionResult){1, "Out of memory"};
          }
        }
        xfree(key_tuple);
      }
    }
  }
  return (ExecutionResult){0, "Success"};
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
      if (cmd->has_where && !evaluate_condition(cmd->where, &__tup(row), db, NULL, NULL)) continue;

      if (!expand_row_set(update_set)) return (ExecutionResult){1, "Out of memory"};
      update_set->rows[update_set->count++] = (RowID){page_idx, row_idx};

      for (int fk_idx = 0; fk_idx < fk_count; fk_idx++) {
        Constraint* fk = &referencing_fks[fk_idx];
        ColumnValue* old_tuple = xmalloc(sizeof(ColumnValue) * fk->ref_column_count);
        ColumnValue* new_tuple = xmalloc(sizeof(ColumnValue) * fk->ref_column_count);

        if (!old_tuple || !new_tuple) {
          xfree(old_tuple);
          xfree(new_tuple);
          return (ExecutionResult){1, "Out of memory"};
        }

        if (!extract_fk_tuple(row, schema, fk, old_tuple)) {
          xfree(old_tuple);
          xfree(new_tuple);
          continue;
        }

        for (uint8_t col_idx = 0; col_idx < fk->ref_column_count; col_idx++) {
          int schema_col_idx = find_column_index(schema, fk->ref_columns[col_idx]);
          bool will_update = false;
          ColumnValue new_value = row->values[schema_col_idx];

          for (int k = 0; k < cmd->value_counts[0]; ++k) {
            if (cmd->update_columns[k].index == schema_col_idx) {
              ColumnValue eval = evaluate_expression(cmd->values[0][k], &__tup(row), db, NULL, NULL);
              ColumnValue array_idx = evaluate_expression(cmd->update_columns->array_idx, &__tup(row), db, NULL, NULL);

              if (!infer_and_cast_value(&eval, &schema->columns[schema_col_idx])) {
                xfree(old_tuple);
                xfree(new_tuple);
                return (ExecutionResult){1, "Invalid type casting in FK update"};
              }

              if (!is_struct_zeroed(&array_idx, sizeof(ColumnValue))) {
                new_value.array.array_value[array_idx.int_value] = eval;
              } else {
                new_value = eval;
              }
              will_update = true;
              break;
            }
          }
          new_tuple[col_idx] = will_update ? new_value : row->values[schema_col_idx];
        }

        if (!store_fk_tuple(&old_fk[fk_idx], old_tuple, fk, schema) ||
            !store_fk_tuple(&new_fk[fk_idx], new_tuple, fk, schema)) {
          xfree(old_tuple);
          xfree(new_tuple);
          return (ExecutionResult){1, "Out of memory"};
        }

        xfree(old_tuple);
        xfree(new_tuple);
      }
    }
  }
  return (ExecutionResult){0, "Success"};
}

bool tuple_exists(FKConstraintValues* fk_constraint, ColumnValue* key_tuple, Constraint* fk, TableSchema* schema) {
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

bool store_fk_tuple(FKConstraintValues* fk_constraint, ColumnValue* key_tuple, Constraint* fk, TableSchema* schema) {
  if (tuple_exists(fk_constraint, key_tuple, fk, schema)) return true;
  if (!expand_fk_constraint(fk_constraint, fk->ref_column_count)) return false;

  ColumnValue* dest = &fk_constraint->values[fk_constraint->count * fk->ref_column_count];
  for (uint8_t col_idx = 0; col_idx < fk->ref_column_count; col_idx++) {
    dest[col_idx] = key_tuple[col_idx];
  }
  fk_constraint->count++;
  return true;
}

bool handle_on_delete_constraints(Database* db, Constraint* constraint, FKConstraintValues* fk_constraint) {
  switch (constraint->on_delete) {
    case FK_CASCADE:
      return cascade_delete(db, constraint->table_id, constraint->columns,
                            constraint->column_count, fk_constraint->values, fk_constraint->count);
    case FK_SET_NULL:
      return set_null(db, constraint->table_id, constraint->columns,
                                constraint->column_count, fk_constraint->values, fk_constraint->count);
    case FK_RESTRICT:
      return check_no_references(db, constraint->table_id, constraint->columns,
                                 constraint->column_count, fk_constraint->values, fk_constraint->count);
    case FK_NO_ACTION:
    default:
      return true;
  }
}

bool handle_on_update_constraints(Database* db, Constraint* constraint, FKConstraintValues* old_fk, FKConstraintValues* new_fk) {
  if (!db || !constraint || !old_fk || !new_fk) return false;

  switch (constraint->on_update) {
    case FK_CASCADE:
      return cascade_update(db, constraint->table_id, constraint->columns,
                            constraint->column_count, old_fk->values, new_fk->values,
                            old_fk->count);
    case FK_SET_NULL:
      return set_null(db, constraint->table_id, constraint->columns,
                                constraint->column_count, old_fk->values, old_fk->count);
    case FK_RESTRICT:
      return check_no_references(db, constraint->table_id, constraint->columns,
                                 constraint->column_count, old_fk->values, old_fk->count);
    case FK_NO_ACTION:
    default:
      return true;
  }
}

bool cascade_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    LOG_WARN("Referenced table schema not found for ID: %ld", referencing_table_id);
    return false;
  }

  for (int i = 0; i < value_count; i++) {
    char where_clause[512] = {0};
    for (int j = 0; j < ref_column_count; j++) {
      if (j > 0) strcat(where_clause, " AND ");
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[i * ref_column_count + j]);
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", ref_columns[j], value_str);
      strcat(where_clause, condition);
    }

    char query[1024];
    snprintf(query, sizeof(query), "DELETE FROM %s WHERE %s;", ref_schema->table_name, where_clause);
    ParserState state = parser_save_state(db->core->parser);
    Result res = process(db, query);
    parser_restore_state(db->core->parser, state);

    if (res.exec.code != 0) {
      LOG_ERROR("Cascade delete failed for table '%s': %s", ref_schema->table_name, query);
      free_result(&res);
      return false;
    }
    LOG_INFO("Cascade deleted %d rows from table '%s'", res.exec.row_count, ref_schema->table_name);
    free_result(&res);
  }
  return true;
}

bool cascade_update(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* old_values, ColumnValue* new_values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) {
    LOG_WARN("Referenced table schema not found for ID: %ld", referencing_table_id);
    return false;
  }

  for (int i = 0; i < value_count; i++) {
    char set_clause[512] = {0};
    for (int j = 0; j < ref_column_count; j++) {
      if (j > 0) strcat(set_clause, ", ");
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &new_values[i * ref_column_count + j]);
      char set_part[256];
      snprintf(set_part, sizeof(set_part), "%s = %s", ref_columns[j], value_str);
      strcat(set_clause, set_part);
    }

    char where_clause[512] = {0};
    for (int j = 0; j < ref_column_count; j++) {
      if (j > 0) strcat(where_clause, " AND ");
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &old_values[i * ref_column_count + j]);
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", ref_columns[j], value_str);
      strcat(where_clause, condition);
    }

    char query[2048];
    snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s;", ref_schema->table_name, set_clause, where_clause);
    ParserState state = parser_save_state(db->core->parser);
    Result res = process(db, query);
    parser_restore_state(db->core->parser, state);

    if (res.exec.code != 0) {
      LOG_ERROR("Cascade update failed for table '%s': %s", ref_schema->table_name, query);
      free_result(&res);
      return false;
    }
    LOG_INFO("Cascade updated %d rows in table '%s'", res.exec.row_count, ref_schema->table_name);
    free_result(&res);
  }
  return true;
}

bool set_null(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) return false;

  char set_clause[512] = {0};
  for (int i = 0; i < ref_column_count; i++) {
    if (i > 0) strcat(set_clause, ", ");
    char set_part[128];
    snprintf(set_part, sizeof(set_part), "%s = NULL", ref_columns[i]);
    strcat(set_clause, set_part);
  }

  for (int i = 0; i < value_count; i++) {
    char where_clause[1024] = {0};
    for (int j = 0; j < ref_column_count; j++) {
      if (j > 0) strcat(where_clause, " AND ");
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[i * ref_column_count + j]);
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", ref_columns[j], value_str);
      strcat(where_clause, condition);
    }

    char query[2048];
    snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s;", ref_schema->table_name, set_clause, where_clause);
    ParserState state = parser_save_state(db->core->parser);
    Result res = process_silent(db, query);
    parser_restore_state(db->core->parser, state);
    bool success = (res.exec.code == 0);
    free_result(&res);
    if (!success) return false;
  }
  return true;
}

bool set_default_on_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) return false;

  char set_clause[512] = {0};
  for (int i = 0; i < ref_column_count; i++) {
    if (i > 0) strcat(set_clause, ", ");
    int col_idx = find_column_index(ref_schema, ref_columns[i]);
    char set_part[128];
    if (col_idx >= 0 && ref_schema->columns[col_idx].default_value) {
      snprintf(set_part, sizeof(set_part), "%s = %s", ref_columns[i], str_column_value(ref_schema->columns[col_idx].default_value));
    } else {
      snprintf(set_part, sizeof(set_part), "%s = NULL", ref_columns[i]);
    }
    strcat(set_clause, set_part);
  }

  for (int i = 0; i < value_count; i++) {
    char where_clause[1024] = {0};
    for (int j = 0; j < ref_column_count; j++) {
      if (j > 0) strcat(where_clause, " AND ");
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[i * ref_column_count + j]);
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", ref_columns[j], value_str);
      strcat(where_clause, condition);
    }

    char query[2048];
    snprintf(query, sizeof(query), "UPDATE %s SET %s WHERE %s;", ref_schema->table_name, set_clause, where_clause);
    ParserState state = parser_save_state(db->core->parser);
    Result res = process_silent(db->core, query);
    parser_restore_state(db->core->parser, state);
    bool success = (res.exec.code == 0);
    free_result(&res);
    if (!success) return false;
  }
  return true;
}

bool check_no_references(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count) {
  TableSchema* ref_schema = get_table_schema_by_id(db, referencing_table_id);
  if (!ref_schema) return false;

  for (int i = 0; i < value_count; i++) {
    char where_clause[512] = {0};
    for (int j = 0; j < ref_column_count; j++) {
      if (j > 0) strcat(where_clause, " AND ");
      char value_str[256];
      format_column_value(value_str, sizeof(value_str), &values[i * ref_column_count + j]);
      char condition[512];
      snprintf(condition, sizeof(condition), "%s = %s", ref_columns[j], value_str);
      strcat(where_clause, condition);
    }

    char query[1024];
    snprintf(query, sizeof(query), "SELECT COUNT() FROM %s WHERE %s;", ref_schema->table_name, where_clause);
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

int64_t find_constraint_by_name(Database* db, int64_t table_id, const char* name) {
  if (!db || !db->constr_cache || !name) {
    LOG_ERROR("Invalid parameters to find_constraint_by_name");
    return -1;
  }

  Constraint* constraint = syscache_get_constraint_by_name(db->constr_cache, name);
  if (constraint && constraint->table_id == table_id) {
    return constraint->id;
  }

  return -1;
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
      if (row->values[k].is_toast && !toast_delete(db, row->values[k].toast_object)) {
        LOG_WARN("Unable to delete TOAST entries. Run 'fix'.");
      }
    }

    serialize_delete(pool, (RowID){page_idx, row_idx + 1});
    page->is_dirty = true;
    rows_deleted++;
  }

  return (ExecutionResult){0, "Delete executed successfully", .row_count = rows_deleted};
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
        .cols = xmalloc(sizeof(uint16_t) * max_updates),
        .old_vals = xmalloc(sizeof(ColumnValue) * max_updates),
        .new_vals = xmalloc(sizeof(ColumnValue) * max_updates),
        .count = 0};

    if (!upd.cols || !upd.old_vals || !upd.new_vals) {
      xfree(upd.cols);
      xfree(upd.old_vals);
      xfree(upd.new_vals);
      return (ExecutionResult){1, "Out of memory"};
    }

    for (int k = 0; k < max_updates; ++k) {
      int col_index = cmd->update_columns[k].index;
      ColumnValue eval = evaluate_expression(cmd->values[0][k], &__tup(row), db, NULL, NULL);
      ColumnValue array_idx = evaluate_expression(cmd->update_columns->array_idx, &__tup(row), db, NULL, NULL);

      if (!infer_and_cast_value(&eval, &schema->columns[col_index])) {
        xfree(upd.cols);
        xfree(upd.old_vals);
        xfree(upd.new_vals);
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
        row->null_bitmap = (uint8_t*)xmalloc(null_bitmap_size);
        xmemcpy(row->null_bitmap, cmd->bitmap, null_bitmap_size);
      } else {
        row->null_bitmap = (uint8_t*)xcalloc(null_bitmap_size, 1);
      }
      rows_updated++;
      page->is_dirty = true;
    }

    xfree(upd.cols);
    xfree(upd.old_vals);
    xfree(upd.new_vals);
  }

  return (ExecutionResult){0, "Update executed successfully", .row_count = rows_updated};
}


bool expand_row_set(RowSet* set) {
  if (set->count < set->capacity) return true;
  set->capacity <<= 1;
  RowID* new_rows = xrealloc(set->rows, sizeof(RowID) * set->capacity);
  if (!new_rows) return false;
  set->rows = new_rows;
  return true;
}

bool expand_fk_constraint(FKConstraintValues* fk_constraint, int ref_col_count) {
  if (fk_constraint->count < fk_constraint->capacity) return true;
  fk_constraint->capacity <<= 1;
  ColumnValue* new_vals = xrealloc(fk_constraint->values, sizeof(ColumnValue) * fk_constraint->capacity * ref_col_count);
  if (!new_vals) return false;
  fk_constraint->values = new_vals;
  return true;
}