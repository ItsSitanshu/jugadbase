#ifndef CONTEXT_H
#define CONTEXT_H

#include "fs.h"
#include "io.h"
#include "parser.h"
#include "page.h"

#define MAX_COMMANDS 1024
#define MAX_TABLES 256
#define DB_INIT_MAGIC 0x4A554741  // "JUGA" 

typedef struct Context {
  Lexer* lexer;
  Parser* parser;
  char* uuid;

  TableCatalogEntry tc[MAX_TABLES];
  size_t table_count;

  FS* fs;
  FILE* db_file;

  IO* tc_reader;
  IO* tc_writer;
  IO* tc_appender;

  struct {
    IO* reader;
    IO* writer;
    IO* appender;
    uint32_t next_row_id; 
    char* name;
  } schema;

  struct {
    IO* reader;
    IO* writer;
    IO* appender;
  } idx;

  Page current_page;
} Context;

Context* ctx_init();
void ctx_free(Context* ctx);

bool process_dot_cmd(Context* ctx, char* input);
void list_tables(Context* ctx);
void process_file(char* filename);

void load_tc(Context* ctx);
void switch_schema(Context* ctx, char* schema_name);
void load_table_schema(Context* ctx);

unsigned int hash_table_name(const char* table_name);
bool load_schema_tc(Context* ctx, char* table_name);
TableSchema* find_table_schema_tc(Context* ctx, const char* filename);
bool load_initial_schema(Context* ctx);

#endif // CONTEXT_H