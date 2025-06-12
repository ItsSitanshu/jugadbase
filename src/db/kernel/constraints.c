#include "kernel/kernel.h"

int64_t insert_default_constraint(Database* db, int64_t table_id, const char* column_name, const char* default_expr) {
  if (!db || !column_name || !default_expr) {
    LOG_ERROR("Invalid parameters to insert_default_constraint");
    return -1;
  }

  // char constraint_name[256];
  // snprintf(constraint_name, sizeof(constraint_name), "df_%s_%s", 
  //   db->tc[get_table_offset(db, column_name)].schema->table_name, column_name);

  // const char* col_names[] = { column_name };
  
  // return insert_constraint(db, table_id, constraint_name, 0, col_names, 1,
  //   default_expr, NULL, NULL, 0, 0, 0);
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

  char query[2048];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_constraints "
    "(table_id, columns, name, constraint_type, check_expr, ref_table, columns, "
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

  // LOG_DEBUG("[+] constraint: %s", query);

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

bool handle_on_update_constraints(Database* db, ColumnDefinition col) {

}

bool handle_on_delete_constraints(Database* db, ColumnDefinition def, ColumnValue val) {
  char query[1024];
  char value[300];

  format_column_value(value, sizeof(value), &val);

  switch (def.on_delete) {
    case FK_CASCADE: {
      snprintf(query, sizeof(query), "DELETE FROM %s WHERE %s = %s", def.foreign_table, def.foreign_column, value);
      
      Result res = process(db, query);
    
      return res.exec.code == 0;   
    }  
    case FK_SET_NULL: {
      snprintf(query, sizeof(query), "UPDATE %s SET %s = NULL WHERE %s = %s",
       def.foreign_table, def.foreign_column, def.foreign_column, value);
      
      Result res = process(db, query);
    
      return res.exec.code == 0;   
    }
    case FK_RESTRICT: {
      LOG_INFO("Did not delete row because of foreign constraint restriction with %s.%s", 
        def.foreign_table, def.foreign_column);

      return false;

    }  
    default:
      return true;
  }

  return true;
}