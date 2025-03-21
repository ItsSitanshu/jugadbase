#ifndef CONTEXT_H
#define CONTEXT_H

#include "parser.h"
#include "io.h"
#include <sys/stat.h>

#define MAX_COMMANDS 1024
#define MAX_TABLES 256
#define DB_INIT_MAGIC 0x4A554741  // "JUGA" 

typedef struct Context {
  Lexer* lexer;
  Parser* parser;
  IO* reader;
  IO* writer;
  IO* appender;

  char* filename;
  char* uuid;

  BTree* btree;
  uint32_t next_row_id; 

  TableCatalogEntry table_catalog[MAX_TABLES];
  size_t table_count;
} Context;

Context* ctx_init();
void ctx_free(Context* ctx);

bool process_dot_cmd(Context* ctx, char* input);
void process_file(char* filename);

void load_table_catalog(Context* ctx);
void switch_schema(Context* ctx, char* filename);

#endif // CONTEXT_H