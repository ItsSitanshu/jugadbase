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

  ctx->btree = btree_create();
  if (!ctx->btree) {
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->next_row_id = 0;

  return ctx;
}

void ctx_free(Context* ctx) {
  if (!ctx) return;

  parser_free(ctx->parser);
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
    fwrite(&db_init, sizeof(uint32_t), 1, file);
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
}

void load_table_catalog(Context* ctx) {
  if (!ctx || !ctx->reader) return;

  IO* io = ctx->reader;
  uint32_t db_init;

  if (io_read(io, &db_init, sizeof(uint32_t)) != sizeof(uint32_t)) {
    printf("Error: Failed to read DB INIT\n");
    return;
  }

  if (db_init != DB_INIT_MAGIC) {
    printf("Error: Invalid database file (wrong DB INIT)\n");
    return;
  }

  ctx->table_count = 0;  

  while (io_read(io, &ctx->table_catalog[ctx->table_count].name_length, sizeof(uint8_t)) == sizeof(uint8_t)) {
    TableCatalogEntry* entry = &ctx->table_catalog[ctx->table_count];

    io_read(io, entry->name, entry->name_length);
    entry->name[entry->name_length] = '\0';

    io_read(io, &entry->offset, sizeof(uint32_t));
    ctx->table_count++;

    if (ctx->table_count >= MAX_TABLES) break;  // Prevent overflow
  }
}