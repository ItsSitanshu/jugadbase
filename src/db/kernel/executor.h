#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "storage/database.h"
#include "internal/functions.h"

#include "utils/log.h"
#include "utils/security.h"

#include <stdarg.h>

typedef struct {
  int code;
  const char* message;

  Row* rows;
  uint32_t row_count;

  char** aliases;
  size_t alias_limit;

  uint8_t owns_rows;
} ExecutionResult;

typedef struct Result {
  ExecutionResult exec;
  JQLCommand* cmd;
} Result;

Result process(Database* db, char* buffer);
Result process_core(Database* db, char* buffer);

Result execute_cmd(Database* db, JQLCommand* cmd);
ExecutionResult execute_create_table(Database* db, JQLCommand* cmd);
ExecutionResult execute_insert(Database* db, JQLCommand* cmd);
Row* execute_row_insert(ExprNode** src, Database* db, uint8_t schema_idx, 
  ColumnDefinition* primary_key_cols, ColumnValue* primary_key_vals, 
  TableSchema* schema, uint8_t column_count,
  char** columns, uint8_t up_col_count, bool specified_order);
ExecutionResult execute_select(Database* db, JQLCommand* cmd);
ExecutionResult execute_update(Database* db, JQLCommand* cmd);
ExecutionResult execute_delete(Database* db, JQLCommand* cmd);

ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx, ColumnDefinition* out);

ColumnValue evaluate_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_literal_expression(ExprNode* expr, Database* db);
ColumnValue evaluate_column_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db);
ColumnValue evaluate_array_access_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_unary_op_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_binary_op_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_comparison_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_like_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_between_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_in_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_logical_and_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_logical_or_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_logical_not_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue evaluate_datetime_binary_op(ColumnValue left, ColumnValue right, int op);

bool evaluate_condition(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);

void swap_rows(Row* r1, Row* r2);
int compare_rows(const Row* r1, const Row* r2, JQLCommand* cmd, TableSchema* schema);
int partition_rows(Row rows[], int low, int high, JQLCommand *cmd, TableSchema *schema);
void quick_sort_rows(Row rows[], int low, int high, JQLCommand *cmd, TableSchema *schema);

bool match_char_class(char** pattern_ptr, char* str);
bool like_match(char* str, char* pattern);
void* get_column_value_as_pointer(ColumnValue* col_val);
size_t size_from_type(ColumnDefinition* fallback);
size_t size_from_value(ColumnValue* val, ColumnDefinition* fallback);
uint32_t get_table_offset(Database* db, const char* table_name);
bool column_name_in_list(const char* name, char** list, uint8_t list_len);

void check_and_concat_toast(Database* db, ColumnValue* value);
bool check_foreign_key(Database* db, ColumnDefinition def, ColumnValue val);
bool handle_on_delete_constraints(Database* db, ColumnDefinition def, ColumnValue val);

// WAL
void write_update_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  uint16_t* col_indices, ColumnValue* old_values, ColumnValue* new_values, 
  uint16_t num_columns, TableSchema* schema);
void write_delete_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  Row* row, TableSchema* schema);

bool insert_table(Database* db, char* name);
bool insert_constraint(Database* db, int table_id, const char* name, int constraint_type, const char** column_names, int column_count,
  const char* check_expr, const char* ref_table, const char** ref_columns, int ref_column_count, const char* on_delete, const char* on_update,
  bool is_deferrable, bool is_deferred);
bool insert_default_value(Database* db, int table_id, const char* column_name, const char* default_expr);

int64_t sequence_next_val(Database* db, char* name);
int64_t create_default_squence(Database* db, char* name);
int64_t find_sequence(Database* db, char* name);

void free_row(Row* row);
void free_execution_result(ExecutionResult* result);
void free_result(Result* result);

#endif // EXECUTOR_H
