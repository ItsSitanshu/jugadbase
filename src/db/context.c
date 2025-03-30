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
  memset(ctx->table_catalog, 0, sizeof(ctx->table_catalog));

  ctx->tc_reader = NULL;
  ctx->tc_writer = NULL;
  ctx->tc_appender = NULL;

  ctx->db_file = NULL;

  memset(&ctx->current_page, 0, sizeof(Page));

  load_table_catalog(ctx);
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
  if (strncmp(input, ".schema", 7) == 0) {
    char* schema_name = input + 8; 
    while (*schema_name == ' ') schema_name++;

    if (*schema_name == '\0') {
      LOG_WARN("Usage: .schema <schemaname>");
    } else {
      LOG_INFO("Changing schema to: %s", schema_name);
      switch_schema(ctx, schema_name);
    }
    return true;
  } else if (strcmp(input, ".help") == 0) {
    LOG_INFO("Available commands:\n  .schema <schemaname>  - Show schema of the given name\n  .quit                 - Exit the program\n  .help                 - Show this help message");
    return true;
  } else if (strcmp(input, ".quit") == 0) {
    LOG_INFO("Exiting...");
    ctx_free(ctx);
    exit(0);
  }

  return false;
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

  load_table_catalog(ctx);

  bool schema_found = false;
  TableCatalogEntry* schema_entry = NULL;

  for (size_t i = 0; i < ctx->table_count; i++) {
    if (strcmp(ctx->table_catalog[i].name, schema_name) == 0) {
      schema_entry = &ctx->table_catalog[i];
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

void load_table_catalog(Context* ctx) {
  if (!ctx || !ctx->fs) return;

  load_table_schema(ctx);

  DIR* dir = opendir(ctx->fs->tables_dir);
  if (!dir) {
    LOG_ERROR("Failed to open schema directory: %s", ctx->fs->tables_dir);
    return;
  }

  ctx->tc_reader = io_init(ctx->fs->schema_file, IO_READ, 1024);
  ctx->tc_writer = io_init(ctx->fs->schema_file, IO_WRITE, 1024);;
  ctx->tc_appender = io_init(ctx->fs->schema_file, IO_APPEND, 1024);;

  struct dirent* entry;
  uint32_t tc = 0;

  while ((entry = readdir(dir)) != NULL) {
    if (entry->d_type == DT_DIR && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      if (tc < ctx->table_count && (strcmp(entry->d_name, ctx->table_catalog[tc].name) == 0)) {
        tc += 1;
      }

      if (tc < ctx->table_count && strcmp(entry->d_name, ctx->table_catalog[tc].name) == 0) {
        char table_path[MAX_PATH_LENGTH];

        int ret = snprintf(table_path, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "%s", ctx->fs->tables_dir, entry->d_name);
        if (ret >= MAX_PATH_LENGTH) {
          LOG_WARN("Table path is too long, truncating to fit buffer size.");
          table_path[MAX_PATH_LENGTH - 1] = '\0'; 
        }

        char index_file[MAX_PATH_LENGTH], data_file[MAX_PATH_LENGTH];
        
        ret = snprintf(index_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "xyz_row.idx", table_path);
        if (ret >= MAX_PATH_LENGTH) {
          LOG_WARN("Index file path is too long, truncating to fit buffer size.");
          index_file[MAX_PATH_LENGTH - 1] = '\0';  
        }

        ret = snprintf(data_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "rows.db", table_path);
        if (ret >= MAX_PATH_LENGTH) {
          LOG_WARN("Data file path is too long, truncating to fit buffer size.");
          data_file[MAX_PATH_LENGTH - 1] = '\0';
        }

        if (access(index_file, F_OK) == 0 && access(data_file, F_OK) == 0) {
          tc++;  
        }
      }
    }
  }

  closedir(dir);
}

void load_table_schema(Context* ctx) {
  if (!ctx || !ctx->fs) {
    LOG_ERROR("Invalid context or missing filesystem.");
    return;
  }

  char schema_path[MAX_PATH_LENGTH];
  snprintf(schema_path, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "schema", ctx->fs->tables_dir);

  FILE* file = fopen(schema_path, "rb");
  if (!file) {
    LOG_ERROR("Failed to open schema file: %s", schema_path);
    return;
  }

  uint32_t db_init;
  if (fread(&db_init, sizeof(uint32_t), 1, file) != 1) {
    LOG_ERROR("Failed to read database initialization magic number.");
    fclose(file);
    return;
  }

  if (db_init != DB_INIT_MAGIC) {
    LOG_ERROR("Invalid database file (wrong DB INIT magic number: 0x%X).", db_init);
    fclose(file);
    return;
  }

  if (fread(&ctx->table_count, sizeof(uint32_t), 1, file) != 1) {
    LOG_ERROR("Failed to read table count.");
    fclose(file);
    return;
  }

  if (ctx->table_count > MAX_TABLES) {
    LOG_ERROR("Table count exceeds maximum allowed tables.");
    fclose(file);
    return;
  }

  uint32_t tc = 0;
  while (tc < ctx->table_count) {
    TableCatalogEntry* entry = &ctx->table_catalog[tc];

    if (fread(&entry->offset, sizeof(uint32_t), 1, file) != 1) {
      LOG_ERROR("Failed to read table offset.");
      break;
    }

    if (fread(&entry->name_length, sizeof(uint8_t), 1, file) != 1) {
      LOG_ERROR("Failed to read table name length.");
      break;
    }

    if (entry->name_length == 0 || entry->name_length >= sizeof(entry->name)) {
      LOG_ERROR("Invalid table name length (%u).", entry->name_length);
      break;
    }

    if (fread(entry->name, sizeof(char), entry->name_length, file) != entry->name_length) {
      LOG_ERROR("Failed to read table name.");
      break;
    }
    entry->name[entry->name_length] = '\0';

    fseek(file, (entry->offset - (entry->name_length + sizeof(uint32_t) + sizeof(uint8_t))), SEEK_CUR);

    tc++;
  }

  fclose(file);
}