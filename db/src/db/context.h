#ifndef CONTEXT_H
#define CONTEXT_H

#include "parser.h"
#include "io.h"

#define MAX_COMMANDS 1024

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
} Context;


Context* ctx_init();
void ctx_free(Context* ctx);

bool process_dot_cmd(Context* ctx, char* input);
void process_file(char* filename);

void switch_schema(Context* ctx, char* filename);

#endif // CONTEXT_H