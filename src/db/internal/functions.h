#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <time.h>

#include "parser/parser.h"
#include "storage/storage.h"

typedef struct Database Database;

typedef ColumnValue (*BuiltinFunction)(
  ExprNode** args, 
  uint8_t arg_count, 
  Row* row, 
  TableSchema* schema,
  Database* db,
  uint8_t schema_idx
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
ColumnValue resolve_expr_value(ExprNode* expr, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx, ColumnDefinition* out);

void register_function(const char* name, BuiltinFunction func);
BuiltinFunction find_function(const char* name);
void free_function_registry();
void register_builtin_functions();

ColumnValue evaluate_function(
  const char* name, 
  ExprNode** args, 
  uint8_t arg_count, 
  Row* row, 
  TableSchema* schema,
  Database* db,
  uint8_t schema_idx
);

ColumnValue evaluate_aggregate(
  ExprNode* expr,
  Row* rows,
  uint32_t row_count,
  TableSchema* schema,
  Database* db,
  uint8_t schema_idx
);


ColumnValue evaluate_expression(
  ExprNode* expr, 
  Row* row, 
  TableSchema* schema,
  Database* db,
  uint8_t schema_idx
);

ColumnValue fn_abs(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_round(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_now(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_sin(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_cos(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_tan(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_log(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_pow(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_concat(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_substring(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_length(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_lower(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_upper(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_trim(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_replace(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_coalesce(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_cast(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_time(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_ifnull(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_greatest(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_least(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_rand(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_floor(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_ceiling(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_pi(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_degrees(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_radians(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_extract(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);
ColumnValue fn_str_to_date(ExprNode** args, uint8_t arg_count, Row* row, TableSchema* schema, Database* db, uint8_t schema_idx);

#endif /* FUNCTIONS_H */