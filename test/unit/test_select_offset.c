#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/executor.h"
#include "utils/testing.h"

START_TEST(test_select_with_offset) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);
  
  struct {
    char* query;
    int expected_rows;
  } offset_test_cases[] = {
    { "SELECT * FROM employees OFFSET 2;", 13 },
    { "SELECT * FROM employees OFFSET 14;", 1 },
    { "SELECT * FROM employees OFFSET 15;", 0 },
    { "SELECT * FROM employees WHERE department = \"Engineering\" OFFSET 1;", 5 },
    { "SELECT * FROM employees WHERE salary > 60000 OFFSET 2;", 12 }
  };

  for (int i = 0; i < sizeof(offset_test_cases) / sizeof(offset_test_cases[0]); i++) {
    printf("Executing OFFSET test case #%d: %s\n", i + 1, offset_test_cases[i].query);
    ExecutionResult res = process(db, offset_test_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == offset_test_cases[i].expected_rows,
      "OFFSET test case #%d failed: expected %d rows, got %d",
      i + 1, offset_test_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* offset_suite(void) {
  Suite* s = suite_create("SelectWithOffset");
  
  TCase* tc_offset = tcase_create("SelectWithOffset");
  tcase_add_test(tc_offset, test_select_with_offset);
  suite_add_tcase(s, tc_offset);
  
  return s;
}


int main(void) {
  SRunner* sr = srunner_create(offset_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}