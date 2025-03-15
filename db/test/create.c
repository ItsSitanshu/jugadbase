#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"

#define TEST_SCHEMA_FILE "test.jdb"

START_TEST(test_read_table_schema) {
  unlink(TEST_SCHEMA_FILE);

  Context* ctx = ctx_init();
  ck_assert_ptr_nonnull(ctx);

  switch_schema(ctx, TEST_SCHEMA_FILE);

  char* create_query = "CREATE TABLE users (id INT, name VARCHAR(50));";
  ExecutionResult res = process(ctx, create_query);
  ck_assert_int_eq(res.status_code, 0);

  TableSchema schema = read_table_schema(ctx);
  ck_assert_str_eq(schema.table_name, "users");
  ck_assert_int_eq(schema.column_count, 2);

  ck_assert_str_eq(schema.columns[0].name, "id");
  ck_assert_int_eq(schema.columns[0].type, TOK_T_INT);

  ck_assert_str_eq(schema.columns[1].name, "name");
  ck_assert_int_eq(schema.columns[1].type, TOK_T_VARCHAR);

  free(schema.columns);
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
