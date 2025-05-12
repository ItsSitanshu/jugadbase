#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"
#include "testing.h"

START_TEST(test_simple_select) {
  INIT_TEST(db);
  setup_test_data(db, employees_setup_queries);

  struct {
    char* query;
    int expected_rows;
  } complex_where_cases[] = {
    { "SELECT * FROM employees WHERE ((age > 30 AND salary > 80000) OR (department = 'Engineering' AND NOT is_active = false)) AND (age < 35 OR salary > 85000);", 6 },
    { "SELECT * FROM employees WHERE (((age >= 30 AND age <= 35) OR (salary >= 75000 AND salary <= 85000)) AND is_active = true) OR (department = 'Finance' AND age < 30);", 6 },
    { "SELECT * FROM employees WHERE (department = 'Engineering' OR department = 'Marketing' OR department = 'Finance') AND (age < 30 OR age > 35) AND is_active = true;", 3 },
    { "SELECT * FROM employees WHERE (department = 'Engineering' AND salary > 80000 AND is_active = true) OR (department = 'Finance' AND age > 30 AND is_active = false) OR (department = 'Marketing' AND age < 30 AND salary < 70000);", 7 },
    { "SELECT * FROM employees WHERE NOT ((department = 'Engineering' AND salary < 80000) OR (is_active = false AND age < 30));", 9 },
    { "SELECT * FROM employees WHERE ((age > 25 AND (department = 'Engineering' OR department = 'Finance')) OR (salary > 70000 AND (is_active = true OR age >= 35))) AND NOT (department = 'HR' AND salary < 60000);", 11 },
    { "SELECT * FROM employees WHERE (age >= 30 AND age <= 35 AND salary >= 75000) OR (age < 30 AND salary > 65000 AND is_active = true);", 8 },
    { "SELECT * FROM employees WHERE (((age > 30 AND department = 'Engineering') OR (salary > 75000 AND is_active = true)) AND (age < 40 OR department != 'HR')) OR ((department = 'Marketing' OR department = 'Finance') AND NOT (salary < 60000 OR age > 35));", 9 },
    { "SELECT * FROM employees WHERE NOT (department = 'HR') AND NOT (salary < 70000 AND is_active = false) AND NOT (age > 35 OR department = 'Finance');", 7 },    
  };

  for (int i = 0; i < sizeof(complex_where_cases) / sizeof(complex_where_cases[0]); i++) {
    printf("Executing complex WHERE query #%d: %s\n", i + 1, complex_where_cases[i].query);
    ExecutionResult res = process(db, complex_where_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == complex_where_cases[i].expected_rows,
      "Complex WHERE query #%d failed: expected %d rows, got %d",
      i + 1, complex_where_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }  

  db_free(db);
}
END_TEST


Suite* simple_select_suite(void) {
  Suite* s = suite_create("SimpleSelect");
  TCase* tc = tcase_create("SimpleSelect");

  tcase_add_test(tc, test_simple_select);
  suite_add_tcase(s, tc);
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(simple_select_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}