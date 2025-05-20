#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/executor.h"
#include "utils/testing.h"

START_TEST(test_update_operations) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);

  struct {
    char* update_query;
    char* verify_query;
    int expected_updated_rows;
    int expected_verify_rows;
  } update_test_cases[] = {
    { 
      "UPDATE employees SET salary = 80000 WHERE id = 1;",
      "SELECT * FROM employees WHERE id = 1 AND salary = 80000;", 
      1, 1 
    },
    
    { 
      "UPDATE employees SET is_active = true WHERE department = \"Marketing\" OR department = \"Finance\";",
      "SELECT * FROM employees WHERE is_active = true;", 
      6, 14 
    },
    
    { 
      "UPDATE employees SET age = age + 1, salary = salary * 1.1 WHERE age < 30 AND is_active = true;",
      "SELECT * FROM employees WHERE age = 23 AND salary = 60500;", 
      6, 1 
    },
    
    { 
      "UPDATE employees SET last_login_date = \"2025-04-20\";",
      "SELECT * FROM employees WHERE last_login_date = \"2025-04-20\";", 
      15, 15 
    },
    
    { 
      "UPDATE employees SET department = \"Data Science\", salary = 90000 WHERE department = \"Engineering\" AND salary > 80000;",
      "SELECT * FROM employees WHERE department = \"Data Science\";", 
      3, 3 
    }
  };

  for (int i = 0; i < sizeof(update_test_cases) / sizeof(update_test_cases[0]); i++) {
    printf("Executing UPDATE test case #%d: %s\n", i + 1, update_test_cases[i].update_query);
    
    ExecutionResult update_res = process(db, update_test_cases[i].update_query).exec;
    ck_assert_int_eq(update_res.code, 0);
    
    ExecutionResult verify_res = process(db, update_test_cases[i].verify_query).exec;
    ck_assert_int_eq(verify_res.code, 0);
    ck_assert_msg(verify_res.row_count == update_test_cases[i].expected_verify_rows,
      "UPDATE test case #%d verification failed: expected %d rows, got %d",
      i + 1, update_test_cases[i].expected_verify_rows, verify_res.row_count);
  
    if (verify_res.owns_rows) {
      free(verify_res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* update_suite(void) {
  Suite* s = suite_create("Update");
  
  TCase* tc_limit = tcase_create("Update");
  tcase_add_test(tc_limit, test_update_operations);
  suite_add_tcase(s, tc_limit);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(update_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}