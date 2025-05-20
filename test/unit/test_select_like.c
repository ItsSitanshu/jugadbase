#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/executor.h"
#include "utils/testing.h"

START_TEST(test_select_with_like) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);
  
  struct {
    char* query;
    int expected_rows;
  } like_test_cases[] = {
    { "SELECT * FROM employees WHERE name LIKE '%Smith%';", 1 },          // Contains "Smith"
    { "SELECT * FROM employees WHERE email LIKE '%@example.com';", 15 },  // Ends with "@example.com"
    { "SELECT * FROM employees WHERE name LIKE 'A%';", 1 },               // Starts with "A"
    { "SELECT * FROM employees WHERE department LIKE '%ing';", 9 },       // Ends with "ing"
    { "SELECT * FROM employees WHERE last_login_date LIKE '2025-04-%';", 7 }, // Dates in April
    { "SELECT * FROM employees WHERE name LIKE '_o%';", 1 }               // Second character is "o"
  };

  for (int i = 0; i < sizeof(like_test_cases) / sizeof(like_test_cases[0]); i++) {
    printf("Executing LIKE test case #%d: %s\n", i + 1, like_test_cases[i].query);
    ExecutionResult res = process(db, like_test_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == like_test_cases[i].expected_rows,
      "LIKE test case #%d failed: expected %d rows, got %d",
      i + 1, like_test_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* like_suite(void) {
  Suite* s = suite_create("SelectWithLike");
  
  TCase* tc_like = tcase_create("SelectWithLike");
  tcase_add_test(tc_like, test_select_with_like);
  suite_add_tcase(s, tc_like);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(like_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}