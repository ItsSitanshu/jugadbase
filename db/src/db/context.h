#ifndef CONTEXT_H
#define CONTEXT_H

#include "parser.h"
#include "executor.h"
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
} Context;

typedef struct {
  int status_code;
  char *message;
} ExecutionResult;


Context* ctx_init();
void ctx_free(Context* ctx);

ExecutionResult process(Context* ctx, char* buffer);
bool process_dot_cmd(Context* ctx, char* input);
void process_file(char* filename);

ExecutionOrder* generate_execution_plan(JQLCommand* command);
ExecutionResult execute_cmd(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd);
void switch_schema(Context* ctx, char* filename);

#endif // CONTEXT_H