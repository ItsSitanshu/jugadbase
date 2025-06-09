#ifndef JB_CORE_H
#define JB_CORE_H

#include "parser/parser.h"

TableSchema* jb_tables_schema() {
  TableSchema* schema = malloc(sizeof(TableSchema));
  strcpy(schema->table_name, "jb_tables");

  schema->column_count = 5;
  schema->not_null_count = 1;

  schema->columns = calloc(schema->column_count, sizeof(ColumnDefinition));

  ColumnDefinition* cols = schema->columns;

  strcpy(cols[0].name, "id");
  cols[0].type = TOK_T_SERIAL;
  cols[0].has_sequence = true;

  strcpy(cols[1].name, "name");
  cols[1].type = TOK_T_TEXT;
  cols[1].is_not_null = true;

  strcpy(cols[2].name, "database_name");
  cols[2].type = TOK_T_TEXT;

  strcpy(cols[3].name, "owner");
  cols[3].type = TOK_T_TEXT;
  cols[3].has_default = true;
  cols[3].default_value = calloc(1, sizeof(ColumnValue));
  cols[3].default_value->type = TOK_T_TEXT;
  cols[3].default_value->str_value = "sudo";

  strcpy(cols[4].name, "created_at");
  cols[4].type = TOK_T_TIMESTAMP;

  return schema;
}

TableSchema* jb_sequences_schema() {
  TableSchema* schema = malloc(sizeof(TableSchema));
  strcpy(schema->table_name, "jb_sequences");

  schema->column_count = 7;
  schema->not_null_count = 0;

  schema->columns = calloc(schema->column_count, sizeof(ColumnDefinition));
  ColumnDefinition* cols = schema->columns;

  strcpy(cols[0].name, "id");
  cols[0].type = TOK_T_SERIAL;
  cols[0].has_sequence = true;

  strcpy(cols[1].name, "name");
  cols[1].type = TOK_T_TEXT;

  strcpy(cols[2].name, "current_value");
  cols[2].type = TOK_T_INT;

  strcpy(cols[3].name, "increment_by");
  cols[3].type = TOK_T_INT;

  strcpy(cols[4].name, "min_value");
  cols[4].type = TOK_T_INT;

  strcpy(cols[5].name, "max_value");
  cols[5].type = TOK_T_INT;

  strcpy(cols[6].name, "cycle");
  cols[6].type = TOK_T_BOOL;

  return schema;
}

TableSchema* jb_attribute_schema() {
  TableSchema* schema = malloc(sizeof(TableSchema));
  strcpy(schema->table_name, "jb_attribute");

  schema->column_count = 9;
  schema->not_null_count = 0;

  schema->columns = calloc(schema->column_count, sizeof(ColumnDefinition));
  ColumnDefinition* cols = schema->columns;

  strcpy(cols[0].name, "id");
  cols[0].type = TOK_T_SERIAL;
  cols[0].has_sequence = true;

  strcpy(cols[1].name, "table_id");
  cols[1].type = TOK_T_INT;

  strcpy(cols[2].name, "column_name");
  cols[2].type = TOK_T_TEXT;

  strcpy(cols[3].name, "data_type");
  cols[3].type = TOK_T_INT;

  strcpy(cols[4].name, "ordinal_position");
  cols[4].type = TOK_T_INT;

  strcpy(cols[5].name, "is_nullable");
  cols[5].type = TOK_T_BOOL;

  strcpy(cols[6].name, "has_default");
  cols[6].type = TOK_T_BOOL;

  strcpy(cols[7].name, "has_constraints");
  cols[7].type = TOK_T_BOOL;

  strcpy(cols[8].name, "created_at");
  cols[8].type = TOK_T_TIMESTAMP;

  return schema;
}

TableSchema* jb_attrdef_schema() {
  TableSchema* schema = malloc(sizeof(TableSchema));
  strcpy(schema->table_name, "jb_attrdef");

  schema->column_count = 5;
  schema->not_null_count = 3;

  schema->columns = calloc(schema->column_count, sizeof(ColumnDefinition));
  ColumnDefinition* cols = schema->columns;

  strcpy(cols[0].name, "id");
  cols[0].type = TOK_T_SERIAL;
  cols[0].has_sequence = true;

  strcpy(cols[1].name, "table_id");
  cols[1].type = TOK_T_INT;
  cols[1].is_not_null = true;

  strcpy(cols[2].name, "column_name");
  cols[2].type = TOK_T_TEXT;
  cols[2].is_not_null = true;

  strcpy(cols[3].name, "default_expr");
  cols[3].type = TOK_T_TEXT;
  cols[3].is_not_null = true;

  strcpy(cols[4].name, "created_at");
  cols[4].type = TOK_T_TIMESTAMP;

  return schema;
}

bool load_jb_attributes_hardcoded(Database* db) {
  if (!db || !db->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  Attribute hc_attributes[] = {
    {TOK_T_SERIAL, 0, true, false, false},
    {TOK_T_INT, 1, true, false, false},
    {TOK_T_TEXT, 2, true, false, false},
    {TOK_T_INT, 3, true, false, false},
    {TOK_T_INT, 4, true, false, false},
    {TOK_T_BOOL, 5, true, false, false},
    {TOK_T_BOOL, 6, true, false, false},
    {TOK_T_BOOL, 7, true, false, false},
    {TOK_T_TIMESTAMP, 8, true, false, false},
  };

  FILE* io = db->tc_reader;
  io_seek(io, 0, SEEK_SET);

  const char* table_name = "jb_attribute";
  unsigned int idx = hash_fnv1a(table_name, MAX_TABLES);
  // if (db->tc[idx].schema) return true;  // Already loaded

  size_t index_table_offset = 2 * sizeof(uint32_t);

  uint32_t schema_offset;
  io_seek(io, index_table_offset + idx * sizeof(uint32_t), SEEK_SET);
  if (io_read(io, &schema_offset, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read schema offset.");
    return false;
  }

  io_seek(io, schema_offset, SEEK_SET);
  uint32_t schema_length;
  if (io_read(io, &schema_length, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read schema length.");
    return false;
  }

  TableSchema* schema = calloc(1, sizeof(TableSchema));
  if (!schema) {
    LOG_ERROR("Memory allocation failed for schema.");
    return false;
  }

  uint8_t table_name_length;
  if (io_read(io, &table_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read table name length.");
    free(schema);
    return false;
  }

  if (io_read(io, schema->table_name, table_name_length) != table_name_length) {
    LOG_ERROR("Failed to read table name.");
    free(schema);
    return false;
  }
  schema->table_name[table_name_length] = '\0';

  if (io_read(io, &schema->column_count, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read column count.");
    free(schema);
    return false;
  }

  schema->columns = calloc(schema->column_count, sizeof(ColumnDefinition));
  
  if (!schema->columns) {
    LOG_ERROR("Memory allocation failed for columns.");
    free(schema);
    return false;
  }

  for (uint8_t j = 0; j < schema->column_count; j++) {
    ColumnDefinition* col = &schema->columns[j];

    uint8_t col_name_length;
    if (io_read(io, &col_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read column name length.");
      goto cleanup;
    }

    if (io_read(io, col->name, col_name_length) != col_name_length) {
      LOG_ERROR("Failed to read column name.");
      goto cleanup;
    }
    col->name[col_name_length] = '\0';

    Attribute attr = hc_attributes[j];
    col->is_not_null = !attr.is_nullable;
    col->type = attr.data_type;
    col->has_default = attr.has_default;
    col->has_constraints = attr.has_constraints;

    io_read(io, &col->type_varchar, sizeof(uint8_t));
    io_read(io, &col->type_decimal_precision, sizeof(uint8_t));
    io_read(io, &col->type_decimal_scale, sizeof(uint8_t));

    io_read(io, &col->is_array, sizeof(bool));
    io_read(io, &col->is_index, sizeof(bool));
    io_read(io, &col->is_foreign_key, sizeof(bool));

    if (col->is_primary_key) schema->prim_column_count++;
    if (col->is_not_null) schema->not_null_count++;
  }

  FILE* file = NULL;
  char file_path[MAX_PATH_LENGTH];

  db->tc[idx].schema = schema;
  load_lake(db);

  return true;

cleanup:
  free(schema->columns);
  free(schema);
  return false;
}

bool load_jb_tables_hardcoded(Database* db) {
  if (!db || !db->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  Attribute hc_attributes[] = {
    {TOK_T_SERIAL, 0, false, false, false},   // id
    {TOK_T_TEXT, 1, false, false, false},     // name
    {TOK_T_TEXT, 2, false, false, false},     // database_name
    {TOK_T_INT, 3, false, true, false},      // owner
    {TOK_T_TIMESTAMP, 4, true, false, false} // created_at
  };

  FILE* io = db->tc_reader;
  io_seek(io, 0, SEEK_SET);

  const char* table_name = "jb_tables";
  unsigned int idx = hash_fnv1a(table_name, MAX_TABLES);

  size_t index_table_offset = 2 * sizeof(uint32_t);

  uint32_t schema_offset;
  io_seek(io, index_table_offset + idx * sizeof(uint32_t), SEEK_SET);
  if (io_read(io, &schema_offset, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read schema offset.");
    return false;
  }

  io_seek(io, schema_offset, SEEK_SET);
  uint32_t schema_length;
  if (io_read(io, &schema_length, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read schema length.");
    return false;
  }

  TableSchema* schema = calloc(1, sizeof(TableSchema));
  if (!schema) {
    LOG_ERROR("Memory allocation failed for schema.");
    return false;
  }

  uint8_t table_name_length;
  if (io_read(io, &table_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read table name length.");
    free(schema);
    return false;
  }

  if (io_read(io, schema->table_name, table_name_length) != table_name_length) {
    LOG_ERROR("Failed to read table name.");
    free(schema);
    return false;
  }
  schema->table_name[table_name_length] = '\0';

  if (io_read(io, &schema->column_count, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read column count.");
    free(schema);
    return false;
  }

  schema->columns = calloc(schema->column_count, sizeof(ColumnDefinition));
  if (!schema->columns) {
    LOG_ERROR("Memory allocation failed for columns.");
    free(schema);
    return false;
  }

  for (uint8_t j = 0; j < schema->column_count; j++) {
    ColumnDefinition* col = &schema->columns[j];

    uint8_t col_name_length;
    if (io_read(io, &col_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read column name length.");
      goto cleanup;
    }

    if (io_read(io, col->name, col_name_length) != col_name_length) {
      LOG_ERROR("Failed to read column name.");
      goto cleanup;
    }
    col->name[col_name_length] = '\0';

    Attribute attr = hc_attributes[j];
    col->is_not_null = !attr.is_nullable;
    col->type = attr.data_type;
    col->has_default = attr.has_default;
    col->has_constraints = attr.has_constraints;

    io_read(io, &col->type_varchar, sizeof(uint8_t));
    io_read(io, &col->type_decimal_precision, sizeof(uint8_t));
    io_read(io, &col->type_decimal_scale, sizeof(uint8_t));

    io_read(io, &col->is_array, sizeof(bool));
    io_read(io, &col->is_index, sizeof(bool));
    io_read(io, &col->is_foreign_key, sizeof(bool));

    if (col->is_primary_key) schema->prim_column_count++;
    if (col->is_not_null) schema->not_null_count++;
  }

  db->tc[idx].schema = schema;
  load_lake(db);

  return true;

cleanup:
  free(schema->columns);
  free(schema);
  return false;
}

#endif // JB_CORE_H