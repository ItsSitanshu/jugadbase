#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "parser.h"
#include "storage.h"

#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdio.h>

#define MAX_FUNCTIONS 64

typedef ColumnValue (*BuiltinFunction)(
  ExprNode** args, uint8_t arg_count,
  Row* row, TableSchema* schema
);

typedef struct {
  char* name;
  BuiltinFunction func;
} FunctionEntry;

typedef struct {
  FunctionEntry* entries;
  size_t count;
  size_t capacity;
} FunctionRegistry;



extern FunctionRegistry global_function_registry;
void register_function(const char* name, BuiltinFunction func);
BuiltinFunction find_function(const char* name);
void free_function_registry();

void register_builtin_functions();
ColumnValue evaluate_function(const char* name, ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue evaluate_expression(ExprNode* expr, Row* row, TableSchema* schema, Context* ctx, uint8_t schema_idx);

ColumnValue fn_abs(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_round(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_now(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_sin(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_cos(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_tan(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_log(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_pow(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_concat(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_substring(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_length(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_lower(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_upper(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_trim(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_replace(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_coalesce(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_cast(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_time(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_ifnull(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_greatest(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_least(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_rand(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_floor(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_ceiling(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_pi(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_degrees(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_radians(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_extract(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);
ColumnValue fn_str_to_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema);

#endif // FUNCTIONS_H