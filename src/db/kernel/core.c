#include "kernel/executor.h"

#include "utils/jb_core.h"

char* CONSTRAINT_FLAGS[N_CONSTRAINTS_TYPES][N_CONSTRAINTS_FLAGS] = {
  {"false", "false", "false", "true", "false"},
  {"false", "false", "true",  "false", "true"},
  {"false", "false", "true",  "false", "false"},
  {"false", "false", "true",  "false", "false"}
};

bool bootstrap_core_tables(Database* db) {
  if (!db) return false;

  TableSchema* schemas[4] = {
    jb_tables_schema(),
    jb_sequences_schema(),
    jb_attribute_schema(),
    jb_attrdef_schema() 
  };

  for (int i = 0; i < 4; i++) {
    ExecutionResult res = execute_create_table_internal(db, schemas[i], i);
    if (res.code != 0) {
      LOG_ERROR("Failed to bootstrap table '%s': %s", schemas[i]->table_name, res.message);
      return false;
    }
  }

  return true;
}

ExecutionResult execute_create_table_internal(Database* db, TableSchema* schema, int64_t table_id) {
  if (!db || !schema || !db->tc_appender) {
    return (ExecutionResult){1, "Invalid execution context or schema"};
  }

  FILE* tca_io = db->tc_appender;


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

    io_write(tca_io, &col->type_varchar, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_precision, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_scale, sizeof(uint8_t));

    io_write(tca_io, &col->is_array, sizeof(bool));
    io_write(tca_io, &col->is_index, sizeof(bool));
    io_write(tca_io, &col->is_foreign_key, sizeof(bool));
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

    return (ExecutionResult){1, "Table creation failed"};
  }

  char rows_file[MAX_PATH_LENGTH];
  int ret = snprintf(rows_file, MAX_PATH_LENGTH, "%s" SEP "rows.db", table_dir);
  if (ret >= MAX_PATH_LENGTH) {
    LOG_WARN("Rows file path too long, truncated");
    rows_file[MAX_PATH_LENGTH - 1] = '\0';
  }

  FILE* rows_fp = fopen(rows_file, "wb");
  if (!rows_fp) {
    LOG_ERROR("Failed to create rows file: %s", rows_file);
    rmdir(table_dir);
    return (ExecutionResult){1, "Failed to create rows file"};
  }

  uint64_t row_id = 0;
  fwrite(&row_id, sizeof(uint64_t), 1, rows_fp);
  fclose(rows_fp);

  io_flush(db->tc_writer);

  load_tc(db);
  unsigned int idx = hash_fnv1a(schema->table_name, MAX_TABLES);
  if (db->tc[idx].schema) {
    return (ExecutionResult){-1, "Conflict whilst creating internal schemas"};
  }

  db->tc[idx].schema = schema;

  return (ExecutionResult){0, "Table schema written successfully"};
}

int64_t find_table(Database* db, char* name) {
  if (strcmp(name, "jb_tables") == 0) {
    return 0;
  } else if (strcmp(name, "jb_attribute") == 0) {
    return 2;
  }

  if (is_struct_zeroed(name, 256)) return -1;
  if (!db) {
    LOG_ERROR("Invalid parameters to find_table");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[2048];
  snprintf(query, sizeof(query),
    "SELECT id FROM jb_tables "
    "WHERE name = '%s';",
    name
  );

  // LOG_DEBUG("> %s", query);

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  if (!success) {
    LOG_ERROR("Failed to find table '%s'", name);
    return -1;
  }

  int64_t value = res.exec.rows[0].values[0].int_value;
  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
}

int64_t insert_table(Database* db, char* name) {
  if (!db || !name) {
    LOG_ERROR("Invalid parameters to insert_table");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[2048];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_tables "
    "(name, database_name, owner, created_at) "
    "VALUES ('%s', '%s', 'sudo', NOW()) RETURNING id;",
    name,
    db->core->uuid
  );

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  if (!success) {
    LOG_ERROR("Failed to insert table '%s'", name);
    return -1;
  }

  int64_t value = res.exec.rows[0].values[0].int_value;
  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
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

  Result pres = process_silent(db->core, pquery);
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

  Result res = process_silent(db->core, query);
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

  Result res = process_silent(db->core, query);
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

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);
  char query[2048];

  snprintf(query, sizeof(query),
    "SELECT id FROM jb_sequences "
    "WHERE name = '%s';",
    name
  );

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;
  if (!success) {
    LOG_ERROR("Failed to find a valid sequence '%s'", name);
    return -1;
  }

  parser_restore_state(db->core->parser, state);
  return res.exec.rows[0].values[0].int_value;
}

int64_t insert_default_constraint(Database* db, int64_t table_id, const char* column_name, const char* default_expr) {
  if (!db || !column_name || !default_expr) {
    LOG_ERROR("Invalid parameters to insert_default_constraint");
    return -1;
  }

  char constraint_name[256];
  snprintf(constraint_name, sizeof(constraint_name), "df_%s_%s", 
    db->tc[get_table_offset(db, column_name)].schema->table_name, column_name);

  const char* col_names[] = { column_name };
  
  return insert_constraint(db, table_id, constraint_name, 0, col_names, 1,
    default_expr, NULL, NULL, 0, 0, 0);
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

int64_t insert_attribute(Database* db, int64_t table_id, const char* column_name, int data_type, int ordinal_position, bool is_nullable, bool has_default, bool has_constraints) {
  if (!db || !column_name) {
    LOG_ERROR("Invalid parameters to insert_attribute");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[1024];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_attribute "
    "(table_id, column_name, data_type, ordinal_position, is_nullable, has_default, has_constraints, created_at) "
    "VALUES (%ld, \"%s\", %d, %d, %s, %s, %s, NOW()) RETURNING id;",
    table_id,
    column_name,
    data_type,
    ordinal_position,
    is_nullable ? "true" : "false",
    has_default ? "true" : "false",
    has_constraints ? "true" : "false"    
  );

  // LOG_DEBUG("[+] attr: %s", column_name);

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  if (!success) {
    LOG_ERROR("Failed to insert attribute '%s'", column_name);
    return -1;
  }

  int64_t value = res.exec.rows[0].values[0].int_value;
  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return value;
}


int64_t insert_attr_default(Database* db, int64_t table_id, const char* column_name, const char* default_expr) {
  if (!db || !column_name || !default_expr) {
    LOG_ERROR("Invalid parameters to insert_attr_default");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[1024];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_attrdef "
    "(table_id, column_name, default_expr, created_at) "
    "VALUES (%ld, '%s', '%s', NOW()) RETURNING id;",
    table_id,
    column_name,
    default_expr
  );

  // LOG_DEBUG("[+] attr_default: %s", column_name);

  Result res = process_silent(db->core, query);
  bool success = res.exec.code == 0;

  if (!success) {
    LOG_ERROR("Failed to insert default for column '%s'", column_name);
    return -1;
  }

  int64_t value = res.exec.rows[0].values[0].int_value;
  parser_restore_state(db->core->parser, state);
  free_result(&res);

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
    "VALUES ('%s', %ld, %ld, %ld, %ld, %s)",
    name,
    min_value,
    increment_by,
    min_value,
    max_value,
    cycle ? "true" : "false"
  );

  bool success = (process_silent(db->core, query)).exec.code == 0;

  return success;
}


Attribute* load_attribute(Database* db, int64_t table_id, const char* column_name) {
  if (!db || !column_name) {
    LOG_ERROR("Invalid parameters to load_attribute");
    return NULL;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[512];
  snprintf(query, sizeof(query),
    "SELECT data_type, ordinal_position, is_nullable, has_default, has_constraints "
    "FROM jb_attribute "
    "WHERE table_id = %ld AND column_name = '%s';",
    table_id, column_name);

  // LOG_DEBUG("l[attribute]: %s", query);

  Result res = process_silent(db->core, query);
  if (res.exec.code != 0 || res.exec.row_count == 0) {
    LOG_ERROR("Failed to load attribute '%s' - %s", column_name, res.exec.message);
    parser_restore_state(db->core->parser, state);
    free_result(&res);
    return NULL;
  } else if (res.exec.row_count > 1) {
    LOG_ERROR("Interal: found %u conflicting attributes for '%s' - %s", res.exec.row_count, column_name, res.exec.message);
    parser_restore_state(db->core->parser, state);
    free_result(&res);
    return NULL;
  }

  Attribute* attr = calloc(1, sizeof(Attribute));
  Row result = res.exec.rows[0];
  attr->data_type = result.values[0].int_value;
  attr->ordinal_position = result.values[1].int_value;
  attr->is_nullable = result.values[2].bool_value;
  attr->has_default = result.values[3].bool_value;
  attr->has_constraints = result.values[4].bool_value;

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return attr;
}

ExprNode* load_attr_default(Database* db, int64_t table_id, char* column_name) {
  if (!db || !column_name) {
    LOG_ERROR("Invalid parameters to load_attr_default");
    return NULL;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[512];
  snprintf(query, sizeof(query),
    "SELECT default_expr FROM jb_attrdef "
    "WHERE table_id = %ld AND column_name = \"%s\";",
    table_id, column_name);

  // LOG_DEBUG("l(default_expr): %s", query);

  Result res = process_silent(db->core, query);
  if (res.exec.code != 0 || res.exec.row_count == 0) {
    LOG_ERROR("Failed to load default for column '%s'", column_name);
    parser_restore_state(db->core->parser, state);
    free_result(&res);
    return NULL;
  }

  char* default_expr_str = res.exec.rows[0].values[0].str_value;
  if (!default_expr_str) {
    LOG_ERROR("Default expression is NULL for column '%s'", column_name);
    parser_restore_state(db->core->parser, state);
    free_result(&res);
    return NULL;
  } 

  // LOG_DEBUG("Trying to load: %s", default_expr_str);

  lexer_set_buffer(db->core->lexer, default_expr_str);
  parser_reset(db->core->parser);

  ExprNode* expr_node = parser_parse_expression(db->core->parser, db->tc[table_id].schema);
  if (!expr_node) {
    LOG_ERROR("Failed to parse default expression for column '%s'", column_name);
  }

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return expr_node;
}

