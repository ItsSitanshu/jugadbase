#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/kernel.h"
#include "utils/testing.h"

START_TEST(test_select_with_limit) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);
  
  struct {
    char* query;
    int expected_rows;
  } limit_test_cases[] = {
    { "SELECT * FROM employees LIM 3;", 3 },
    { "SELECT * FROM employees LIM 10;", 10 },
    { "SELECT * FROM employees WHERE salary > 70000 LIM 2;", 2 },
    { "SELECT * FROM employees WHERE age < 30 LIM 1;", 1 },
    { "SELECT * FROM employees LIM 0;", 0 }
  };

  for (int i = 0; i < sizeof(limit_test_cases) / sizeof(limit_test_cases[0]); i++) {
    printf("Executing LIM test case #%d: %s\n", i + 1, limit_test_cases[i].query);
    ExecutionResult res = process(db, limit_test_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == limit_test_cases[i].expected_rows,
      "LIM test case #%d failed: expected %d rows, got %d",
      i + 1, limit_test_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* limit_suite(void) {
  Suite* s = suite_create("SelectWithLimit");
  
  TCase* tc_limit = tcase_create("SelectWithLimit");
  tcase_add_test(tc_limit, test_select_with_limit);
  suite_add_tcase(s, tc_limit);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(limit_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}