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
ExecutionResult execute_insert(Context* ctx, JQLCommand* cmd);

void write_column_value(IO* io, ColumnValue* col_val, ColumnDefinition* col_def);

void* get_column_value_as_pointer(ColumnValue* col_val);
uint32_t get_table_offset(Context* ctx, const char* table_name);
bool parse_uuid_string(const char* uuid_str, uint8_t* output);

ExecutionOrder* generate_execution_plan(JQLCommand* command);

#endif // EXECUTOR_H
