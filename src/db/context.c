#include "context.h"
#include "executor.h"

#include "uuid.h"

Context* ctx_init() {
  Context* ctx = malloc(sizeof(Context));
  if (!ctx) return NULL; 

  ctx->lexer = lexer_init();
  if (!ctx->lexer) {
    free(ctx);
    return NULL;
  }

  ctx->parser = parser_init(ctx->lexer);
  if (!ctx->parser) {
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->reader = NULL;
  ctx->writer = NULL;
  ctx->appender = NULL;
  ctx->filename = NULL;

  ctx->uuid = uuid();
  if (!ctx->uuid) { 
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->fs = fs_init(DB_ROOT_DIRECTORY);

  ctx->next_row_id = 0;
  ctx->db_file = NULL;

  return ctx;
}

void ctx_free(Context* ctx) {
  if (!ctx) return;

  parser_free(ctx->parser);
  fs_free(ctx->fs);
  free(ctx->uuid);
  
  if (ctx->filename) free(ctx->filename);

  if (ctx->reader) io_close(ctx->reader);
  if (ctx->writer) io_close(ctx->writer);
  if (ctx->appender) io_close(ctx->appender);

  free(ctx);
}

bool process_dot_cmd(Context* ctx, char* input) {
  if (strncmp(input, ".schema", 7) == 0) {
    char* schema_name = input + 8; 
    while (*schema_name == ' ') schema_name++;

    if (*schema_name == '\0') {
      printf("Usage: .schema <schemaname>\n");
    } else {
      printf("Changing schema to: %s\n", schema_name);
      switch_schema(ctx, schema_name);
    }
    return true;
  } else if (strcmp(input, ".help") == 0) {
    printf("Available commands:\n");
    printf("  .schema <schemaname>  - Show schema of the given name\n");
    printf("  .quit                 - Exit the program\n");
    printf("  .help                 - Show this help message\n");
    return true;
  } else if (strcmp(input, ".quit") == 0) {
    printf("Exiting...\n");
    ctx_free(ctx);
    exit(0);
  }

  return false;
}

void process_file(char* filename) {
  // TODO: Implement function
}

void switch_schema(Context* ctx, char* filename) {
  if (!ctx) return;

  if (ctx->filename && strcmp(ctx->filename, filename) == 0) {
    return;
  }

  if (ctx->filename) {
    free(ctx->filename);
  }

  ctx->filename = strdup(filename);
  if (!ctx->filename) {
    fprintf(stderr, "Error: Memory allocation failed for filename.\n");
    return;
  }

  if (ctx->reader) io_close(ctx->reader);
  if (ctx->writer) io_close(ctx->writer);
  if (ctx->appender) io_close(ctx->appender);

  struct stat buffer;
  int file_exists = (stat(ctx->filename, &buffer) == 0);

  if (!file_exists) {
    FILE* file = fopen(ctx->filename, "wb");
    if (!file) {
      fprintf(stderr, "Error: Failed to create database file %s\n", ctx->filename);
      return;
    }

    uint32_t db_init = DB_INIT_MAGIC;
    uint32_t table_count = 0;

    fwrite(&db_init, sizeof(uint32_t), 1, file);

    fwrite(&table_count, sizeof(uint32_t), 1, file);

    fclose(file);
  }

  ctx->appender = io_init(ctx->filename, IO_APPEND, 1024);
  if (!ctx->appender) {
    fprintf(stderr, "Error: Failed to initialize appender for %s\n", ctx->filename);
  }

  ctx->writer = io_init(ctx->filename, IO_WRITE, 1024);
  if (!ctx->writer) {
    fprintf(stderr, "Error: Failed to initialize writer for %s\n", ctx->filename);
  }

  ctx->reader = io_init(ctx->filename, IO_READ, 1024);
  if (!ctx->reader) {
    fprintf(stderr, "Error: Failed to initialize reader for %s\n", ctx->filename);
  }

  load_table_catalog(ctx);
  printf("Successfully loaded %lu table(s) from the catalog.\n", ctx->table_count);
}


void load_table_catalog(Context* ctx) {
  if (!ctx || !ctx->fs || !ctx->filename) return;

  char schema_path[MAX_PATH_LENGTH];
  snprintf(schema_path, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "%s", ctx->fs->tables_dir, ctx->filename);

  load_table_schema(ctx);

  DIR* dir = opendir(schema_path);
  if (!dir) {
    perror("Error: Failed to open schema directory");
    return;
  }

  struct dirent* entry;
  uint32_t tc = 0;

  while ((entry = readdir(dir)) != NULL) {
    if (entry->d_type == DT_DIR && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      while (tc < ctx->table_count && strcmp(entry->d_name, ctx->table_catalog[tc].name) != 0) {
        tc++;
      }

      if (tc < ctx->table_count && strcmp(entry->d_name, ctx->table_catalog[tc].name) == 0) {
        char table_path[MAX_PATH_LENGTH];

        int ret = snprintf(table_path, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "%s", schema_path, entry->d_name);
        if (ret >= MAX_PATH_LENGTH) {
          fprintf(stderr, "Warning: Table path is too long, truncating to fit buffer size.\n");
          table_path[MAX_PATH_LENGTH - 1] = '\0'; 
        }

        char index_file[MAX_PATH_LENGTH], data_file[MAX_PATH_LENGTH];
        
        ret = snprintf(index_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "xyz_row.idx", table_path);
        if (ret >= MAX_PATH_LENGTH) {
          fprintf(stderr, "Warning: Index file path is too long, truncating to fit buffer size.\n");
          index_file[MAX_PATH_LENGTH - 1] = '\0';  
        }

        ret = snprintf(data_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "rows.db", table_path);
        if (ret >= MAX_PATH_LENGTH) {
          fprintf(stderr, "Warning: Data file path is too long, truncating to fit buffer size.\n");
          data_file[MAX_PATH_LENGTH - 1] = '\0';
        }

        if (access(index_file, F_OK) == 0 && access(data_file, F_OK) == 0) {
          tc++;  
        }
      }
    }
  }

  if (tc < ctx->table_count) {
    fprintf(stderr, "Warning: Not all directories match the expected table names from the schema.\n");
  }

  closedir(dir);
}



void load_table_schema(Context* ctx) {
  if (!ctx || !ctx->fs) {
    fprintf(stderr, "Error: Invalid context or missing filesystem.\n");
    return;
  }

  char schema_path[MAX_PATH_LENGTH];
  snprintf(schema_path, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "schema", ctx->fs->tables_dir);

  printf("%s\n", schema_path);
  FILE* file = fopen(schema_path, "rb");
  if (!file) {
    perror("Error: Failed to open schema file");
    return;
  }

  uint32_t db_init;
  if (fread(&db_init, sizeof(uint32_t), 1, file) != 1) {
    fprintf(stderr, "Error: Failed to read database initialization magic number.\n");
    fclose(file);
    return;
  }

  if (db_init != DB_INIT_MAGIC) {
    fprintf(stderr, "Error: Invalid database file (wrong DB INIT magic number: 0x%X).\n", db_init);
    fclose(file);
    return;
  }

  if (fread(&ctx->table_count, sizeof(uint32_t), 1, file) != 1) {
    fprintf(stderr, "Error: Failed to read table count.\n");
    fclose(file);
    return;
  }

  if (ctx->table_count > MAX_TABLES) {
    fprintf(stderr, "Error: Table count exceeds maximum allowed tables.\n");
    fclose(file);
    return;
  }

  uint32_t tc = 0;
  while (tc < ctx->table_count) {
    TableCatalogEntry* entry = &ctx->table_catalog[tc];

    if (fread(&entry->offset, sizeof(uint32_t), 1, file) != 1) {
      fprintf(stderr, "Error: Failed to read table offset.\n");
      break;
    }

    if (fread(&entry->name_length, sizeof(uint8_t), 1, file) != 1) {
      fprintf(stderr, "Error: Failed to read table name length.\n");
      break;
    }

    if (entry->name_length == 0 || entry->name_length >= sizeof(entry->name)) {
      fprintf(stderr, "Error: Invalid table name length (%u).\n", entry->name_length);
      break;
    }

    if (fread(entry->name, sizeof(char), entry->name_length, file) != entry->name_length) {
      fprintf(stderr, "Error: Failed to read table name.\n");
      break;
    }
    entry->name[entry->name_length] = '\0';

    fseek(file, (entry->offset - (entry->name_length + sizeof(uint32_t) + sizeof(uint8_t))), SEEK_CUR);

    tc++;
  }

  fclose(file);
}