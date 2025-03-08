#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "context.h"

typedef struct {
  int status_code;
  char *message;
} ExecutionResult;

ExecutionResult process(Context* ctx, char* buffer);
ExecutionOrder* generate_execution_plan(JQLCommand* command);
ExecutionResult execute_cmd(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd);

#endif // EXECUTOR_H
