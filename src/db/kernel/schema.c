#include "kernel/kernel.h"
#include "utils/jb_core.h"

char* CONSTRAINT_FLAGS[N_CONSTRAINTS_TYPES][N_CONSTRAINTS_FLAGS] = {
  {"false", "false", "false", "true", "false"},
  {"false", "false", "true",  "false", "true"},
  {"false", "false", "true",  "false", "false"},
  {"false", "false", "true",  "false", "false"}
};

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
  bool success = res.exec.code == 0 && res.exec.row_count > 0;

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
    "INSERT _unsafecon INTO jb_tables "
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

int64_t insert_attribute(Database* db, int64_t table_id, const char* column_name, 
                        int data_type, int ordinal_position, bool is_nullable, 
                        bool has_default, bool has_constraints, bool is_unsafe) {
  if (!db || !column_name) {
    LOG_ERROR("Invalid parameters to insert_attribute");
    return -1;
  }

  if (!db->core) db->core = db;

  // LOG_ERROR("in insert attribute: %s, cc %d", db->core->tc[130].schema->table_name, db->core->tc[130].schema->column_count);

  ParserState state = parser_save_state(db->core->parser);

  char query[1024];
  snprintf(query, sizeof(query),
    "%s jb_attribute "
    "(table_id, column_name, data_type, ordinal_position, is_nullable, has_default, has_constraints, created_at) "
    "VALUES (%ld, \"%s\", %d, %d, %s, %s, %s, NOW()) RETURNING id;",
    is_unsafe ? "INSERT _unsafecon INTO" : "INSERT INTO",
    table_id,
    column_name,
    data_type,
    ordinal_position,
    is_nullable ? "true" : "false",
    has_default ? "true" : "false",
    has_constraints ? "true" : "false"
  );

  // LOG_DEBUG("[+] attr: %s", query);

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
    LOG_ERROR("Internal: found %u conflicting attributes for '%s' - %s", res.exec.row_count, column_name, res.exec.message);
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

int64_t insert_attr_default(Database* db, int64_t table_id, const char* column_name, const char* default_expr, bool is_unsafe) {
  if (!db || !column_name || !default_expr) {
    LOG_ERROR("Invalid parameters to insert_attr_default");
    return -1;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[1024];
  snprintf(query, sizeof(query),
    "%s jb_attrdef "
    "(table_id, column_name, default_expr, created_at) "
    "VALUES (%ld, '%s', '%s', NOW()) RETURNING id;",
    is_unsafe ? "INSERT _unsafecon INTO" : "INSERT INTO",
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

void check_and_concat_toast(Database* db, ColumnValue* value) {
  char* result = toast_concat(db, value->toast_object);

  if (result) {
    value->str_value = strdup(result);
    value->is_toast = false;
  }
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
      LOG_FATAL("Failed to bootstrap table '%s': %s", schemas[i]->table_name, res.message);
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

    return (ExecutionResult){-1, "Table creation failed"};
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

  LOG_INFO("Created new schema entry in the in memory catalog at %d", idx);


  return (ExecutionResult){0, "Table schema written successfully"};
}

TableSchema* get_table_schema_by_id(Database* db, int64_t table_id) {
  if (!db) {
    LOG_ERROR("Invalid database parameter");
    return NULL;
  }

  if (!db->core) db->core = db;

  ParserState state = parser_save_state(db->core->parser);

  char query[256];
  snprintf(query, sizeof(query),
    "SELECT name FROM jb_tables WHERE id = %ld;",
    table_id
  );

  Result res = process_silent(db->core, query);
  
  if (res.exec.code != 0 || res.exec.row_count == 0) {
    parser_restore_state(db->core->parser, state);
    free_result(&res);
    return NULL;
  }

  char* table_name = res.exec.rows[0].values[0].str_value;
  TableSchema* schema = get_table_schema(db, table_name);

  parser_restore_state(db->core->parser, state);
  free_result(&res);

  return schema;
}