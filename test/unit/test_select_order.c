#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"
#include "testing.h"

START_TEST(test_select_with_order_by) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);
  
  struct {
    char* query;
    int expected_first_id;
    int expected_last_id;
  } order_by_test_cases[] = {
    { "SELECT * FROM employees ORDER BY age ASC;", 4, 5 },      // Youngest to oldest
    { "SELECT * FROM employees ORDER BY age DESC;", 5, 4 },     // Oldest to youngest
    { "SELECT * FROM employees ORDER BY salary ASC;", 4, 5 },   // Lowest to highest salary
    { "SELECT * FROM employees ORDER BY salary DESC;", 5, 4 },  // Highest to lowest salary
    { "SELECT * FROM employees ORDER BY name ASC;", 1, 15 },    // Alphabetical by name
    { "SELECT * FROM employees ORDER BY name DESC;", 15, 1 }    // Reverse alphabetical by name
  };

  for (int i = 0; i < sizeof(order_by_test_cases) / sizeof(order_by_test_cases[0]); i++) {
    printf("Executing ORDER BY test case #%d: %s\n", i + 1, order_by_test_cases[i].query);
    ExecutionResult res = process(db, order_by_test_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_int_eq(res.row_count, 15);
    
    ck_assert_int_eq(res.rows[0].id.row_id, order_by_test_cases[i].expected_first_id);
    ck_assert_int_eq(res.rows[14].id.row_id, order_by_test_cases[i].expected_last_id);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* order_by_suite(void) {
  Suite* s = suite_create("SelectWithOrderBy");
  
  TCase* tc_order_by = tcase_create("SelectWithOrderBy");
  tcase_add_test(tc_order_by, test_select_with_order_by);
  suite_add_tcase(s, tc_order_by);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(order_by_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}