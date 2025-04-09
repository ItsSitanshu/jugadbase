#ifndef CONTEXT_H
#define CONTEXT_H

#include "fs.h"
#include "io.h"
#include "btree.h"
#include "parser.h"
#include "storage.h"

#define MAX_COMMANDS 1024
#define MAX_TABLES 256 // Prime to avoid hash collisons
#define DB_INIT_MAGIC 0x4A554741  // "JUGA" 

typedef struct Context {
  Lexer* lexer;
  Parser* parser;
  char* uuid;

  TableCatalogEntry tc[MAX_TABLES];
  BufferPool lake[MAX_TABLES];
  
  size_t table_count;
  uint8_t loaded_btree_clusters;
  uint8_t btree_idx_stack[BTREE_LIFETIME_THRESHOLD];

  FILE* tc_reader;
  FILE* tc_writer;
  FILE* tc_appender;

  FS* fs;
} Context;

Context* ctx_init();
void ctx_free(Context* ctx);

bool process_dot_cmd(Context* ctx, char* input);
void list_tables(Context* ctx);
void process_file(Context* ctx, char* filename);

void load_tc(Context* ctx);
void load_table_schema(Context* ctx);
void load_btree_cluster(Context* ctx, char* table_name);
void pop_btree_cluster(Context* ctx);

bool load_schema_tc(Context* ctx, char* table_name);
TableSchema* find_table_schema_tc(Context* ctx, const char* filename);
bool load_initial_schema(Context* ctx);

void load_lake(Context* ctx);
void flush_lake(Context* ctx);

#endif // CONTEXT_H