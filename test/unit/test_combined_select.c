#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"
#include "testing.h"

START_TEST(test_combined_select_features) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);
  
  struct {
    char* query;
    int expected_rows;
    int expected_first_id;  // For queries that use ORDER BY
  } combined_test_cases[] = {
    // Combining WHERE, ORDER BY, and LIM
    { "SELECT * FROM employees WHERE department IN (\"Engineering\", \"Marketing\") ORDER BY salary DESC LIM 3;", 3, 11 },
    
    { "SELECT * FROM employees WHERE name LIKE \"%a%\" AND age BETWEEN 25 AND 35 ORDER BY age ASC OFFSET 1;", 7, 8 },
    
    { "SELECT * FROM employees WHERE (department IN (\"Engineering\", \"Finance\") OR salary BETWEEN 60000 AND 70000) AND name LIKE \"%e%\" ORDER BY salary ASC LIM 4 OFFSET 1;", 4, 10 },
    
    { "SELECT * FROM employees WHERE (age BETWEEN 25 AND 35) AND (department IN (\"Engineering\", \"Marketing\")) AND (name LIKE \"%a%\") ORDER BY salary DESC LIM 2 OFFSET 1;", 2, 9 },
    
    { "SELECT * FROM employees WHERE ((department = \"Engineering\" AND age > 30) OR (department = \"Marketing\" AND salary > 70000)) AND is_active = true ORDER BY id ASC LIM 3;", 3, 3 }
  };

  for (int i = 0; i < sizeof(combined_test_cases) / sizeof(combined_test_cases[0]); i++) {
    printf("Executing combined test case #%d: %s\n", i + 1, combined_test_cases[i].query);
    ExecutionResult res = process(db, combined_test_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == combined_test_cases[i].expected_rows,
      "Combined test case #%d failed: expected %d rows, got %d",
      i + 1, combined_test_cases[i].expected_rows, res.row_count);
    
    if (combined_test_cases[i].expected_first_id > 0 && res.row_count > 0) {
      ck_assert_int_eq(res.rows[0].id.row_id, combined_test_cases[i].expected_first_id);
    }
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }

  db_free(db);
}
END_TEST

Suite* combined_select_suite(void) {
  Suite* s = suite_create("CombinedSelectFeatures");
  
  TCase* tc_combined = tcase_create("CombinedSelectFeatures");
  tcase_add_test(tc_combined, test_combined_select_features);
  suite_add_tcase(s, tc_combined);
  
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(combined_select_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}