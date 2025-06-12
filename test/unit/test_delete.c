#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/kernel.h"
#include "utils/testing.h"

START_TEST(test_delete) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);

  struct {
    char* delete_query;
    char* verify_query;
    int expected_deleted_rows;
    int expected_verify_rows;
  } delete_test_cases[] = {
    {
      "DELETE FROM employees WHERE id = 1;",
      "SELECT * FROM employees WHERE id = 1;",
      1, 0
    },
    {
      "DELETE FROM employees WHERE department = 'HR';",
      "SELECT * FROM employees WHERE department = 'HR';",
      3, 0  
    },
    {
      "DELETE FROM employees WHERE age < 18;",
      "SELECT * FROM employees WHERE age < 18;",
      0, 0
    },
    {
      "DELETE FROM employees WHERE is_active = false AND salary < 70000;",
      "SELECT * FROM employees WHERE is_active = false AND salary < 70000;",
      2, 0  
    },
    {
      "DELETE FROM employees;",
      "SELECT * FROM employees;",
      9, 0 
    },
  };

  int n_cases = sizeof(delete_test_cases) / sizeof(delete_test_cases[0]);
  for (int i = 0; i < n_cases; i++) {
    printf("Executing DELETE test case #%d: %s\n",
           i + 1, delete_test_cases[i].delete_query);

    ExecutionResult del_res = process(db, delete_test_cases[i].delete_query).exec;
    ck_assert_int_eq(del_res.code, 0);

    /* verify number of rows actually deleted if your engine returns that */
    /* ck_assert_int_eq(del_res.row_count, delete_test_cases[i].expected_deleted_rows); */

    ExecutionResult verify_res = process(db, delete_test_cases[i].verify_query).exec;
    ck_assert_int_eq(verify_res.code, 0);
    ck_assert_msg(verify_res.row_count == delete_test_cases[i].expected_verify_rows,
      "DELETE test case #%d verification failed: expected %d rows, got %d",
      i + 1,
      delete_test_cases[i].expected_verify_rows,
      verify_res.row_count);

    if (verify_res.owns_rows) {
      free(verify_res.rows);
    }
  }

  db_free(db);
}
END_TEST
Suite* delete_suite(void) {
  Suite* s = suite_create("Delete");
  
  TCase* tc_limit = tcase_create("Delete");
  tcase_add_test(tc_limit, test_delete);
  suite_add_tcase(s, tc_limit);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(delete_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}