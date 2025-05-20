#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/executor.h"
#include "utils/testing.h"

START_TEST(test_select_with_in_between) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);
  
  struct {
    char* query;
    int expected_rows;
  } in_between_test_cases[] = {
    { "SELECT * FROM employees WHERE department IN (\"Engineering\", \"Finance\");", 9 },
    { "SELECT * FROM employees WHERE id IN (1, 3, 5);", 3 },
    { "SELECT * FROM employees WHERE age BETWEEN 25 AND 35;", 12 },
    { "SELECT * FROM employees WHERE salary BETWEEN 60000 AND 80000;", 10 },
    { "SELECT * FROM employees WHERE department IN (\"HR\", \"Marketing\") AND age BETWEEN 20 AND 30;", 5 },
    { "SELECT * FROM employees WHERE salary BETWEEN 70000 AND 90000 AND department IN (\"Engineering\", \"Marketing\");", 7 }
  };

  for (int i = 0; i < sizeof(in_between_test_cases) / sizeof(in_between_test_cases[0]); i++) {
    printf("Executing IN/BETWEEN test case #%d: %s\n", i + 1, in_between_test_cases[i].query);
    ExecutionResult res = process(db, in_between_test_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == in_between_test_cases[i].expected_rows,
      "IN/BETWEEN test case #%d failed: expected %d rows, got %d",
      i + 1, in_between_test_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* in_between_suite(void) {
  Suite* s = suite_create("SelectWithInBetween");
  
  TCase* tc_in_between = tcase_create("SelectWithInBetween");
  tcase_add_test(tc_in_between, test_select_with_in_between);
  suite_add_tcase(s, tc_in_between);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(in_between_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}