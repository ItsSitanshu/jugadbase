#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "context.h"
#include "functions.h"

#include <stdarg.h>

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
bool execute_row_insert(ExprNode** src, Context* ctx, uint8_t schema_idx, 
  ColumnDefinition* primary_key_cols, ColumnValue* primary_key_vals, TableSchema* schema, uint8_t column_count);
ExecutionResult execute_select(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_update(Context* ctx, JQLCommand* cmd);
ExecutionResult execute_delete(Context* ctx, JQLCommand* cmd);

ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx, uint8_t* out_type);
ColumnValue evaluate_expression(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx);
bool evaluate_condition(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx);

bool match_char_class(char** pattern_ptr, char* str);
bool like_match(char* str, char* pattern);
void* get_column_value_as_pointer(ColumnValue* col_val);
size_t size_from_type(uint8_t column_type);
uint32_t get_table_offset(Context* ctx, const char* table_name);
bool column_name_in_list(const char* name, char** list, uint8_t list_len);

ExecutionOrder* generate_execution_plan(JQLCommand* command);

#endif // EXECUTOR_H
