#include "kernel/executor.h"

bool insert_constraint(Database* db, int table_id, const char* name, int constraint_type, const char** column_names, int column_count,
  const char* check_expr, const char* ref_table, const char** ref_columns, int ref_column_count, const char* on_delete, const char* on_update,
  bool is_deferrable, bool is_deferred) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_constraint");
    return false;
  }

  char* format_text_array(const char** items, int count) {
    if (count == 0 || !items) {
      return strdup("{}");
    }

    size_t buf_size = 2;
    for (int i = 0; i < count; i++) {
      buf_size += strlen(items[i]) + 1; 
    }

    char* result = malloc(buf_size);
    if (!result) return NULL;

    strcpy(result, "{");
    for (int i = 0; i < count; i++) {
      strcat(result, items[i]);
      if (i < count - 1) strcat(result, ",");
    }
    strcat(result, "}");
    return result;
  }

  char* col_names_str = format_text_array(column_names, column_count);
  char* ref_cols_str = format_text_array(ref_columns, ref_column_count);

  char query[2048];

  const char* check_expr_safe = check_expr ? check_expr : "";
  const char* ref_table_safe = ref_table ? ref_table : "";
  const char* on_delete_safe = on_delete ? on_delete : "";
  const char* on_update_safe = on_update ? on_update : "";

  snprintf(query, sizeof(query),
    "INSERT INTO jb_constraints "
    "(table_id, name, constraint_type, column_names, check_expr, ref_table, ref_columns, on_delete, on_update, is_deferrable, is_deferred, created_at) "
    "VALUES (%d, '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, NOW())",
    table_id,
    name,
    constraint_type,
    col_names_str ? col_names_str : "{}",
    check_expr_safe,
    ref_table_safe,
    ref_cols_str ? ref_cols_str : "{}",
    on_delete_safe,
    on_update_safe,
    is_deferrable ? "TRUE" : "FALSE",
    is_deferred ? "TRUE" : "FALSE");

  free(col_names_str);
  free(ref_cols_str);

  bool success = (process(db->core, query)).exec.code == 0;
  if (!success) {
    LOG_ERROR("Failed to insert constraint '%s'", name);
  }

  return success;
}

bool insert_default_value(Database* db, int table_id, const char* column_name, const char* default_expr) {
  if (!db->core || !column_name || !default_expr) {
    LOG_ERROR("Invalid parameters to insert_default_value");
    return false;
  }
  
  char query[1024];

  snprintf(query, sizeof(query),
    "INSERT INTO jb_attrdef "
    "(table_id, column_name, default_expr, created_at) "
    "VALUES (%d, '%s', '%s', NOW())",
    table_id,
    column_name,
    default_expr);

  bool success = (process(db->core, query)).exec.code == 0;
  if (!success) {
    LOG_ERROR("Failed to insert default value for column '%s'", column_name);
  }

  return success;
}

bool create_sequence_link(Database* db, ColumnDefinition* def, char* name, uint64_t min_value,
    uint64_t increment_by, uint64_t max_value, bool cycle) {
  if (!db || !db->core || !name) {
    LOG_ERROR("Invalid parameters to create_sequence_link");
    return false;
  }
  
  char query[1024];

  snprintf(query, sizeof(query),
    "INSERT INTO jb_sequences "
    "(name, current_value, increment_by, min_value, max_value, cycle) "
    "VALUES ('%s', %d, %d, %d, %d, %s)",
    name,
    min_value,
    increment_by,
    min_value,
    max_value,
    cycle ? "true" : "false"
  );

  bool success = (process(db->core, query)).exec.code == 0;

  return success;
}