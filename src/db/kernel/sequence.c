#include "kernel/kernel.h"

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