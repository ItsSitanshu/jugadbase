#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "context.h"

typedef struct {
  int code;
  const char* message;
  Row* rows;
  uint32_t row_count;
  uint8_t owns_rows;
} ExecutionResult;

ExecutionResult process(Context* ctx, char* buffer);

ExecutionResult execute_cmd(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_insert(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_select(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_update(Context* ctx, JQLCommand* cmd);

void write_column_value(FILE* io, ColumnValue* col_val, ColumnDefinition* col_def);

void* get_column_value_as_pointer(ColumnValue* col_val);
size_t size_from_type(uint8_t column_type);
bool evaluate_condition(ConditionNode* cond, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx);
void print_column_value(ColumnValue* val);
uint32_t get_table_offset(Context* ctx, const char* table_name);
bool column_name_in_list(const char* name, char** list, uint8_t list_len);

ExecutionOrder* generate_execution_plan(JQLCommand* command);

#endif // EXECUTOR_H
