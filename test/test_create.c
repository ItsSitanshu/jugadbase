#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"

START_TEST(test_read_table_schema) {
  Context* ctx = ctx_init();
  ck_assert_ptr_nonnull(ctx);
  
  char* create_query = "CREATE TABLE users ("
                       "id SERIAL PRIMKEY, "
                       "name VARCHAR(50) NOT NULL UNIQUE, "
                       "age INT CHECK(age > 0), "
                       "email VARCHAR(100) DEFAULT \"unknown\", "
                       "role_id INT FRNKEY REF roles(id)"
                       ");";
  ExecutionResult res = process(ctx, create_query);
  ck_assert_int_eq(res.status_code, 0);

  TableSchema* schema = find_table_schema_tc(ctx, "users");

  ck_assert_str_eq(schema->table_name, "users");
  ck_assert_int_eq(schema->column_count, 5);

  ck_assert_str_eq(schema->columns[0].name, "id");
  ck_assert_int_eq(schema->columns[0].type, TOK_T_SERIAL);
  ck_assert_int_eq(schema->columns[0].is_primary_key, 1);
  ck_assert_int_eq(schema->columns[0].is_auto_increment, 1);
  ck_assert_int_eq(schema->columns[0].is_unique, 1);  // Primary keys are unique
  ck_assert_int_eq(schema->columns[0].is_not_null, 1);  // Primary keys are NOT NULL

  ck_assert_str_eq(schema->columns[1].name, "name");
  ck_assert_int_eq(schema->columns[1].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema->columns[1].type_varchar, 50);
  ck_assert_int_eq(schema->columns[1].is_not_null, 1);
  ck_assert_int_eq(schema->columns[1].is_unique, 1);
  ck_assert_int_eq(schema->columns[1].is_primary_key, 0);
  ck_assert_int_eq(schema->columns[1].is_auto_increment, 0);

  ck_assert_str_eq(schema->columns[2].name, "age");
  ck_assert_int_eq(schema->columns[2].type, TOK_T_INT);
  ck_assert_int_eq(schema->columns[2].has_check, 1);
  ck_assert_str_eq(schema->columns[2].check_expr, "age>0");
  ck_assert_int_eq(schema->columns[2].is_not_null, 0);
  ck_assert_int_eq(schema->columns[2].is_unique, 0);
  ck_assert_int_eq(schema->columns[2].is_primary_key, 0);
  ck_assert_int_eq(schema->columns[2].is_auto_increment, 0);

  ck_assert_str_eq(schema->columns[3].name, "email");
  ck_assert_int_eq(schema->columns[3].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema->columns[3].type_varchar, 100);
  ck_assert_int_eq(schema->columns[3].has_default, 1);
  ck_assert_str_eq(schema->columns[3].default_value, "unknown");
  ck_assert_int_eq(schema->columns[3].is_not_null, 0);
  ck_assert_int_eq(schema->columns[3].is_unique, 0);
  ck_assert_int_eq(schema->columns[3].is_primary_key, 0);
  ck_assert_int_eq(schema->columns[3].is_auto_increment, 0);

  ck_assert_str_eq(schema->columns[4].name, "role_id");
  ck_assert_int_eq(schema->columns[4].type, TOK_T_INT);
  ck_assert_int_eq(schema->columns[4].is_foreign_key, 1);
  ck_assert_str_eq(schema->columns[4].foreign_table, "roles");
  ck_assert_str_eq(schema->columns[4].foreign_column, "id");
  ck_assert_int_eq(schema->columns[4].is_not_null, 0);
  ck_assert_int_eq(schema->columns[4].is_unique, 0);
  ck_assert_int_eq(schema->columns[4].is_primary_key, 0);
  ck_assert_int_eq(schema->columns[4].is_auto_increment, 0);

  free(schema->columns);
  ctx_free(ctx);
}
END_TEST

Suite* schema_suite(void) {
  Suite* s = suite_create("Schema");
  TCase* tc_core = tcase_create("Core");

  tcase_add_test(tc_core, test_read_table_schema);
  suite_add_tcase(s, tc_core);

  return s;
}

int main(void) {
  int no_failed;
  Suite* s = schema_suite();
  SRunner* sr = srunner_create(s);

  srunner_run_all(sr, CK_NORMAL);
  no_failed = srunner_ntests_failed(sr);
  srunner_free(sr);

  return (no_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
