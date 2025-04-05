#include "context.h"
#include "executor.h"
#include "uuid.h"

#include "../utils/log.h"

Context* ctx_init() {
  Context* ctx = (Context*)malloc(sizeof(Context));
  if (!ctx) {
    LOG_FATAL("Failed to allocate memory for context.");
    return NULL;
  }

  memset(ctx, 0, sizeof(Context));

  ctx->lexer = lexer_init();
  if (!ctx->lexer) {
    LOG_FATAL("Failed to initialize lexer.");
    free(ctx);
    return NULL;
  }

  ctx->parser = parser_init(ctx->lexer);
  if (!ctx->parser) {
    LOG_FATAL("Failed to initialize parser.");
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->uuid = uuid();
  if (!ctx->uuid) {
    LOG_FATAL("Failed to generate UUID.");
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->fs = fs_init(DB_ROOT_DIRECTORY);
  if (!ctx->fs) {
    LOG_FATAL("Failed to initialize file system.");
    free(ctx->uuid);
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->schema.name = (char*)malloc(MAX_IDENTIFIER_LEN);
  if (!ctx->schema.name) {
    LOG_FATAL("Failed to allocate memory for schema name.");
    fs_free(ctx->fs);
    free(ctx->uuid);
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }
  memset(ctx->schema.name, 0, MAX_IDENTIFIER_LEN);

  ctx->schema.next_row_id = 0;
  ctx->schema.reader = NULL;
  ctx->schema.writer = NULL;
  ctx->schema.appender = NULL;

  ctx->idx.reader = NULL;
  ctx->idx.writer = NULL;
  ctx->idx.appender = NULL;

  ctx->table_count = 0;
  memset(ctx->tc, 0, sizeof(ctx->tc));

  ctx->tc_reader = NULL;
  ctx->tc_writer = NULL;
  ctx->tc_appender = NULL;

  ctx->tc_reader = io_init(ctx->fs->schema_file, IO_READ, 1024);
  ctx->tc_writer = io_init(ctx->fs->schema_file, IO_WRITE, 1024);
  ctx->tc_appender = io_init(ctx->fs->schema_file, IO_APPEND, 1024);

  ctx->db_file = NULL;

  memset(&ctx->current_page, 0, sizeof(Page));

  load_tc(ctx);
  if (!load_initial_schema(ctx)) {
    LOG_FATAL("Failed to read schema");
  }
  LOG_INFO("Successfully loaded %lu table(s) from catalog", ctx->table_count);

  return ctx;
}

void ctx_free(Context* ctx) {
  if (!ctx) return;

  parser_free(ctx->parser);
  fs_free(ctx->fs);
  free(ctx->uuid);

  if (ctx->schema.reader) io_close(ctx->schema.reader);
  if (ctx->schema.writer) io_close(ctx->schema.writer);
  if (ctx->schema.appender) io_close(ctx->schema.appender);
  free(ctx->schema.name);

  if (ctx->idx.reader) io_close(ctx->idx.reader);
  if (ctx->idx.writer) io_close(ctx->idx.writer);
  if (ctx->idx.appender) io_close(ctx->idx.appender);

  if (ctx->tc_reader) io_close(ctx->tc_reader);
  if (ctx->tc_writer) io_close(ctx->tc_writer);
  if (ctx->tc_appender) io_close(ctx->tc_appender);

  if (ctx->db_file) fclose(ctx->db_file);

  free(ctx);
}

bool process_dot_cmd(Context* ctx, char* input) {
  if (strcmp(input, ".help") == 0) {
    LOG_INFO("Available commands:\n"
      "  .tables       - List all tables\n"
      "  .quit         - Exit the program\n"
      "  .help         - Show this help message\n"
      "  .stats        - Show database statistics\n"
      "  .dump <file>  - Export database to a file");
    return true;
  } else if (strcmp(input, ".tables") == 0) {
    list_tables(ctx);
    return true;
  } else if (strcmp(input, ".stats") == 0) {
    // show_db_stats(ctx); backtrack transactions?
    return true;
  } else if (strcmp(input, ".quit") == 0) {
    LOG_INFO("Exiting...");
    ctx_free(ctx);
    exit(0);
  } else if (strcmp(input, ".clear") == 0) {
    clear_screen();
    return true;
  }

  return false;
}

void list_tables(Context* ctx) {
  if (!ctx || ctx->table_count == 0) {
    LOG_INFO("No tables found in the database.");
    return;
  }

  LOG_INFO("Tables in the database:");
  for (int i = 0; i < ctx->table_count; i++) {
    // LOG_INFO("  - %s (%zu rows)", ctx->tc[i].name, ctx->tc[i].row_count);
  }
}

void process_file(char* filename) {
  // TODO: Implement function
}

void switch_schema(Context* ctx, char* schema_name) {
  if (!ctx) return;

  if (strcmp(ctx->schema.name, schema_name) == 0) {
    LOG_DEBUG("Schema %s is already loaded.", schema_name);
    return;
  }

  load_tc(ctx);

  bool schema_found = false;
  TableCatalogEntry* schema_entry = NULL;

  for (size_t i = 0; i < ctx->table_count; i++) {
    if (strcmp(ctx->tc[i].name, schema_name) == 0) {
      schema_entry = &ctx->tc[i];
      schema_found = true;
      break;
    }
  }

  if (!schema_found) {
    LOG_ERROR("Schema %s not found in the table catalog.", schema_name);
    return;
  }

  char schema_path[MAX_PATH_LENGTH];
  snprintf(schema_path, sizeof(schema_path), "%s/%s/rows.db", ctx->fs->tables_dir, schema_name);

  struct stat buffer;
  int file_exists = (stat(schema_path, &buffer) == 0);

  if (!file_exists) {
    FILE* file = fopen(schema_path, "w");
    if (!file) {
      LOG_ERROR("Failed to create database file %s", schema_path);
      return;
    }

    uint32_t db_init = DB_INIT_MAGIC;
    uint32_t table_count = 0;
    fwrite(&db_init, sizeof(uint32_t), 1, file);
    fwrite(&table_count, sizeof(uint32_t), 1, file);
    fclose(file);
  }

  if (ctx->schema.reader) io_close(ctx->schema.reader);
  if (ctx->schema.writer) io_close(ctx->schema.writer);
  if (ctx->schema.appender) io_close(ctx->schema.appender);

  IO* reader = io_init(schema_path, IO_READ, 1024);
  IO* writer = io_init(schema_path, IO_WRITE, 1024);
  IO* appender = io_init(schema_path, IO_APPEND, 1024);

  if (!reader || !writer || !appender) {
    LOG_ERROR("Failed to initialize I/O for schema %s", schema_name);
    return;
  }

  ctx->schema.name = strdup(schema_name);
  ctx->schema.reader = reader;
  ctx->schema.writer = writer;
  ctx->schema.appender = appender;

  LOG_INFO("Successfully loaded schema %s and initialized I/O.", schema_name);
}

void load_tc(Context* ctx) {
  if (!ctx || !ctx->fs) return;

  load_table_schema(ctx);

  DIR* dir = opendir(ctx->fs->tables_dir);
  if (!dir) {
    LOG_ERROR("Failed to open schema directory: %s", ctx->fs->tables_dir);
    return;
  }

  ctx->tc_reader = io_init(ctx->fs->schema_file, IO_READ, 1024);
  ctx->tc_writer = io_init(ctx->fs->schema_file, IO_WRITE, 1024);
  ctx->tc_appender = io_init(ctx->fs->schema_file, IO_APPEND, 1024);

  struct dirent* entry;
  uint32_t tc = 0;

  while ((entry = readdir(dir)) != NULL) {
    if (entry->d_type == DT_DIR && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      bool found = false;

      for (int i = 0; i < ctx->table_count; i++) {
        if (strcmp(entry->d_name, ctx->tc[i].name) == 0) {
          found = true;
          break;
        }
      }

      if (!found) {
        continue; 
      }

      char table_path[MAX_PATH_LENGTH];
      snprintf(table_path, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "%s", ctx->fs->tables_dir, entry->d_name);

      char data_file[MAX_PATH_LENGTH];
      snprintf(data_file, MAX_PATH_LENGTH, "%.*s" PATH_SEPARATOR "rows.db",
        (int)(MAX_PATH_LENGTH - 10), table_path);

      if (access(data_file, F_OK) == 0) {
        tc += 1;  
      } else {
        LOG_WARN("Data file missing: %s", data_file);
      }
    }
  }


  if (tc < ctx->table_count) {
    LOG_WARN("Not all directories match the expected table names from the schema.");
    LOG_DEBUG("Table Catalog defines %ld whilst %d were read", ctx->table_count, tc);
  }

  closedir(dir);
}

void load_table_schema(Context* ctx) {
  if (!ctx || !ctx->fs) {
    LOG_ERROR("Invalid context or missing filesystem.");
    return;
  }
  
  io_seek(ctx->tc_reader, 0, SEEK_SET);

  uint32_t db_init;
  if (io_read(ctx->tc_reader, &db_init, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read database initialization magic number.");
    io_close(ctx->tc_reader);
    return;
  }

  if (db_init != DB_INIT_MAGIC) {
    LOG_ERROR("Invalid database file (wrong DB INIT magic number: 0x%X).", db_init);
    io_close(ctx->tc_reader);
    return;
  }

  if (io_read(ctx->tc_reader, &ctx->table_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read table count.");
    io_close(ctx->tc_reader);
    return;
  }

  if (ctx->table_count > MAX_TABLES) {
    LOG_ERROR("Table count exceeds maximum allowed tables.");
    io_close(ctx->tc_reader);
    return;
  }

  io_seek(ctx->tc_reader, sizeof(uint32_t) * MAX_TABLES, SEEK_CUR);

  for (uint32_t tc = 0; tc < ctx->table_count; tc++) {
    TableCatalogEntry* entry = &ctx->tc[tc];

    if (io_read(ctx->tc_reader, &entry->offset, sizeof(uint32_t)) != sizeof(uint32_t)) {
      LOG_ERROR("Failed to read table offset.");
      break;
    }

    if (io_read(ctx->tc_reader, &entry->name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read table name length.");
      break;
    }

    if (entry->name_length == 0 || entry->name_length >= sizeof(entry->name)) {
      LOG_ERROR("Invalid table name length (%u).", entry->name_length);
      break;
    }

    if (io_read(ctx->tc_reader, entry->name, entry->name_length) != entry->name_length) {
      LOG_ERROR("Failed to read table name.");
      break;
    }
    entry->name[entry->name_length] = '\0';

    long current_pos = io_tell(ctx->tc_reader);
    long next_offset = entry->offset - (entry->name_length + sizeof(uint32_t) + sizeof(uint8_t));

    if (next_offset > 0) {
      io_seek(ctx->tc_reader, next_offset, SEEK_CUR);
    }
  }

  io_close(ctx->tc_reader);
}

unsigned int hash_table_name(const char* table_name) {
  unsigned int hash = 0;
  while (*table_name) {
    hash = (hash << 5) + hash + *table_name++;
  }
  return hash % MAX_TABLES;
}

bool load_schema_tc(Context* ctx, char* table_name) {
  if (!ctx || !ctx->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  unsigned int idx = hash_table_name(table_name);
  if (ctx->tc[idx].schema) {
    if (strcmp(ctx->tc[idx].schema->table_name, table_name) == 0) {
      return true;
    }
  }
  
  uint32_t initial_offset = 0; 
  
  io_seek(ctx->tc_reader, (idx * sizeof(uint32_t)) + (2 * sizeof(uint32_t)), SEEK_SET);
  io_read(ctx->tc_reader, &initial_offset, sizeof(uint32_t));

  IO* io = ctx->tc_reader;
  TableSchema* schema = malloc(sizeof(TableSchema));
  if (!schema) {
    LOG_ERROR("Memory allocation failed for schema.");
    return false;
  }

  initial_offset += sizeof(uint32_t);
  // Hash-index*4B + Magic Identifier 4B +Table Count 4B  + Table Offset 4B (skipping for reading purpose) 
  io_seek(io, initial_offset, SEEK_SET);


  uint8_t table_name_length;
  if (io_read(io, &table_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read table name length.");
    free(schema);
    return false;
  }

  LOG_DEBUG("%u\n", table_name_length);

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

  schema->columns = malloc(sizeof(ColumnDefinition) * schema->column_count);
  if (!schema->columns) {
    LOG_ERROR("Memory allocation failed for columns.");
    free(schema);
    return false;
  }
  
  for (uint8_t i = 0; i < schema->column_count; i++) {
    ColumnDefinition* col = &schema->columns[i];

    uint8_t col_name_length;
    if (io_read(io, &col_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read column name length.");
      free(schema->columns);
      free(schema);
      return false;
    }

    if (io_read(io, col->name, col_name_length) != col_name_length) {
      LOG_ERROR("Failed to read column name.");
      free(schema->columns);
      free(schema);
      return false;
    }
    col->name[col_name_length] = '\0';

    io_read(io, &col->type, sizeof(uint32_t));
    io_read(io, &col->type_varchar, sizeof(uint8_t));
    io_read(io, &col->type_decimal_precision, sizeof(uint8_t));
    io_read(io, &col->type_decimal_scale, sizeof(uint8_t));

    io_read(io, &col->is_primary_key, sizeof(bool));
    io_read(io, &col->is_unique, sizeof(bool));
    io_read(io, &col->is_not_null, sizeof(bool));
    io_read(io, &col->is_index, sizeof(bool));
    io_read(io, &col->is_auto_increment, sizeof(bool));

    io_read(io, &col->has_default, sizeof(bool));
    if (col->has_default) {
      if (io_read(io, col->default_value, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
        LOG_ERROR("Failed to read default value.");
        free(schema->columns);
        free(schema);
        return false;
      }
    }

    io_read(io, &col->has_check, sizeof(bool));
    if (col->has_check) {
      if (io_read(io, col->check_expr, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
        LOG_ERROR("Failed to read check constraint.");
        free(schema->columns);
        free(schema);
        return false;
      }
    }

    io_read(io, &col->is_foreign_key, sizeof(bool));
    if (col->is_foreign_key) {
      if (io_read(io, col->foreign_table, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN ||
          io_read(io, col->foreign_column, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
        LOG_ERROR("Failed to read foreign key details.");
        free(schema->columns);
        free(schema);
        return false;
      }
    }
  }

  ctx->tc[idx].schema = schema;
  LOG_INFO("Created new schema entry in the in memory catalog at %d", idx);
  return true;
}

TableSchema* find_table_schema_tc(Context* ctx, const char* filename) {
  if (!ctx || !filename) {
    LOG_ERROR("Invalid context or filename provided.");
    return NULL;
  }

  unsigned int idx = hash_table_name(filename);
  LOG_DEBUG("Looking for table @ hash %d | %s == %s", idx, ctx->tc[idx].schema->table_name, filename);
  if (ctx->tc[idx].schema && strcmp(ctx->tc[idx].schema->table_name, filename) == 0) {
    return ctx->tc[idx].schema;
  }

  for (int i = 0; i < ctx->table_count; i++) {
    if (ctx->tc[i].schema && strcmp(ctx->tc[i].schema->table_name, filename) == 0) {
      return ctx->tc[i].schema;
    }
  }

  LOG_ERROR("Schema for filename '%s' not found.", filename);
  return NULL;
}

bool load_initial_schema(Context* ctx) {
  if (!ctx || !ctx->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  IO* io = ctx->tc_reader;

  for (size_t i = 0; i < ctx->table_count; i++) {
    const char* table_name = ctx->tc[i].name;
    unsigned int idx = hash_table_name(table_name);
    
    if (ctx->tc[idx].schema) continue;

    TableSchema* schema = malloc(sizeof(TableSchema));
    if (!schema) {
      LOG_ERROR("Memory allocation failed for schema.");
      return false;
    }

    io_seek(io, i * sizeof(TableSchema), SEEK_SET);

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

    schema->columns = malloc(sizeof(ColumnDefinition) * schema->column_count);
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
        free(schema->columns);
        free(schema);
        return false;
      }

      if (io_read(io, col->name, col_name_length) != col_name_length) {
        LOG_ERROR("Failed to read column name.");
        free(schema->columns);
        free(schema);
        return false;
      }
      col->name[col_name_length] = '\0';

      io_read(io, &col->type, sizeof(uint32_t));
      io_read(io, &col->type_varchar, sizeof(uint8_t));
      io_read(io, &col->type_decimal_precision, sizeof(uint8_t));
      io_read(io, &col->type_decimal_scale, sizeof(uint8_t));

      io_read(io, &col->is_primary_key, sizeof(bool));
      io_read(io, &col->is_unique, sizeof(bool));
      io_read(io, &col->is_not_null, sizeof(bool));
      io_read(io, &col->is_index, sizeof(bool));
      io_read(io, &col->is_auto_increment, sizeof(bool));

      io_read(io, &col->has_default, sizeof(bool));
      if (col->has_default) {
        if (io_read(io, col->default_value, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
          LOG_ERROR("Failed to read default value.");
          free(schema->columns);
          free(schema);
          return false;
        }
      }

      io_read(io, &col->has_check, sizeof(bool));
      if (col->has_check) {
        if (io_read(io, col->check_expr, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
          LOG_ERROR("Failed to read check constraint.");
          free(schema->columns);
          free(schema);
          return false;
        }
      }

      io_read(io, &col->is_foreign_key, sizeof(bool));
      if (col->is_foreign_key) {
        if (io_read(io, col->foreign_table, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN ||
            io_read(io, col->foreign_column, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
          LOG_ERROR("Failed to read foreign key details.");
          free(schema->columns);
          free(schema);
          return false;
        }
      }
    }

    ctx->tc[idx].schema = schema;
  }
  return true;
}