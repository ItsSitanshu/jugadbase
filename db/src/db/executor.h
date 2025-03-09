#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "context.h"

typedef struct {
  int status_code;
  char *message;
} ExecutionResult;

ExecutionResult process(Context* ctx, char* buffer);

ExecutionResult execute_cmd(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd);

void read_table_schema(Context* ctx);

ExecutionOrder* generate_execution_plan(JQLCommand* command);

#endif // EXECUTOR_H
