#ifndef KERNEL_H
#define KERNEL_H

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
Result process_silent(Database* db, char* buffer);

void free_row(Row* row);
void free_execution_result(ExecutionResult* result);
void free_result(Result* result);

#endif

#ifndef KERNEL_COMMANDS_H
#define KERNEL_COMMANDS_H

Result execute_cmd(Database* db, JQLCommand* cmd, bool show);

ExecutionResult execute_create_table(Database* db, JQLCommand* cmd);
ExecutionResult execute_alter_table(Database* db, JQLCommand* cmd);

ExecutionResult execute_insert(Database* db, JQLCommand* cmd);
ExecutionResult execute_select(Database* db, JQLCommand* cmd);
ExecutionResult execute_update(Database* db, JQLCommand* cmd);
ExecutionResult execute_delete(Database* db, JQLCommand* cmd);

Row* execute_row_insert(ExprNode** src, Database* db, uint8_t schema_idx, 
  ColumnDefinition* primary_key_cols, ColumnValue* primary_key_vals, 
  TableSchema* schema, uint8_t column_count,
  char** columns, uint8_t up_col_count, bool specified_order, int64_t table_id, bool is_unsafe);

#endif

#ifndef KERNEL_EXPRESSION_H
#define KERNEL_EXPRESSION_H


ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx, ColumnDefinition* out);
ColumnValue evaluate_expression(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
bool evaluate_condition(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);

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

#endif

#ifndef KERNEL_SCHEMA_H
#define KERNEL_SCHEMA_H

typedef struct Attribute {
  int data_type;
  int ordinal_position;
  bool is_nullable;
  bool has_default;
  bool has_constraints;
} Attribute;


int64_t find_table(Database* db, char* name);
int64_t insert_table(Database* db, char* name);

int64_t insert_attribute(Database* db, int64_t table_id, const char* column_name, 
                        int data_type, int ordinal_position, bool is_nullable, 
                        bool has_default, bool has_constraints, bool is_unsafe);
Attribute* load_attribute(Database* db, int64_t table_id, const char* column_name);

int64_t insert_attr_default(Database* db, int64_t table_id, const char* column_name, const char* default_expr, bool is_unsafe);
ExprNode* load_attr_default(Database* db, int64_t table_id, char* column_name);

bool bootstrap_core_tables(Database* db);
bool load_schema_for_table(Database* db, size_t idx, const char* table_name);
ExecutionResult execute_create_table_internal(Database* db, TableSchema* schema, int64_t table_id);

TableSchema* get_table_schema_by_id(Database* db, int64_t table_id);

#endif

#ifndef KERNEL_CONSTRAINTS_H
#define KERNEL_CONSTRAINTS_H

typedef enum ConstraintType {
  CONSTRAINT_PRIMARY_KEY = 1,
  CONSTRAINT_UNIQUE = 2,
  CONSTRAINT_FOREIGN_KEY = 3,
  CONSTRAINT_CHECK = 4,
} ConstraintType;

typedef struct {
  int64_t id;
  int64_t table_id;
  char** columns;
  int column_count;
  char* name;
  ConstraintType constraint_type;
  char* check_expr;
  int64_t ref_table_id;
  char** ref_columns;
  int ref_column_count;
  FKAction on_delete;
  FKAction on_update;
  bool is_deferrable;
  bool is_deferred;
  bool is_nullable;
  bool is_primary;
  bool is_unique;
} Constraint;

int64_t insert_default_constraint(Database* db, int64_t table_id, const char* column_name, const char* default_expr);
int64_t find_default_constraint(Database* db, int64_t table_id, const char* column_name);
int64_t insert_constraint(Database* db, int64_t table_id, char* name, 
                          int constraint_type, char (*columns)[MAX_IDENTIFIER_LEN], int col_count,
                          char* check_expr, int ref_table, 
                          char (*ref_columns)[MAX_IDENTIFIER_LEN], int ref_col_count,
                          int on_delete, int on_update);

int64_t find_constraint_by_name(Database* db, int64_t table_id, const char* name);
bool delete_constraint(Database* db, int64_t constraint_id);
bool update_constraint_name(Database* db, int64_t constraint_id, const char* new_name);
int64_t insert_single_column_constraint(Database* db, int64_t table_id, int64_t column_id, 
                                       const char* name, uint32_t constraint_type, bool is_nullable,
                                       bool is_unique, bool is_primary);
bool check_foreign_key(Database* db, ColumnDefinition def, ColumnValue val);

char** parse_text_array(const char* text_array_str, int* count);
Constraint parse_constraint_from_row(Row* row);
void free_constraint(Constraint* constraint);
Result get_table_constraints(Database* db, int64_t table_id);

bool validate_not_null_constraint(Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count);
bool validate_unique_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count);
bool validate_primary_key_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count);
bool validate_foreign_key_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count);
bool validate_check_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count);
bool validate_constraint(Database* db, Constraint* constraint, TableSchema* schema, ColumnValue* values, int value_count);

bool cascade_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count);

bool set_null_on_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count);
bool set_default_on_delete(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count);

bool check_no_references(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* values, int value_count);
bool cascade_update(Database* db, int64_t referencing_table_id, char** ref_columns, int ref_column_count, ColumnValue* old_values, ColumnValue* new_values, int value_count);

bool handle_single_on_delete_constraint(Database* db, Constraint* constraint, ColumnValue* values, int value_count);
bool handle_single_on_update_constraint(Database* db, Constraint* constraint, ColumnValue* old_values, ColumnValue* new_values, int value_count);
bool handle_on_delete_constraints(Database* db, int64_t table_id, ColumnValue* values, int value_count);
bool handle_on_update_constraints(Database* db, int64_t table_id, ColumnValue* old_values, ColumnValue* new_values, int value_count);

bool validate_all_constraints(Database* db, int64_t table_id, ColumnValue* values, int value_count);

#endif

#ifndef KERNEL_WAL_H
#define KERNEL_WAL_H

void write_update_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  uint16_t* col_indices, ColumnValue* old_values, ColumnValue* new_values, 
  uint16_t num_columns, TableSchema* schema);

void write_delete_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  Row* row, TableSchema* schema);

#endif

#ifndef KERNEL_SEQUENCE_H
#define KERNEL_SEQUENCE_H

int64_t sequence_next_val(Database* db, char* name);
int64_t create_default_sequence(Database* db, char* name, bool is_unsafe);
int64_t find_sequence(Database* db, char* name);

#endif

#ifndef KERNEL_UTILS_H
#define KERNEL_UTILS_H

void swap_rows(Row* r1, Row* r2);
int compare_rows(const Row* r1, const Row* r2, JQLCommand* cmd, TableSchema* schema);

int partition_rows(Row rows[], int low, int high, JQLCommand *cmd, TableSchema *schema);
void quick_sort_rows(Row rows[], int low, int high, JQLCommand *cmd, TableSchema *schema);

char* process_str_arg(const char* check_expr);

bool match_char_class(char** pattern_ptr, char* str);
bool like_match(char* str, char* pattern);

void* get_column_value_as_pointer(ColumnValue* col_val);
size_t size_from_type(ColumnDefinition* fallback);
size_t size_from_value(ColumnValue* val, ColumnDefinition* fallback);

#endif