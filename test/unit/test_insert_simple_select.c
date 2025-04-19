#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"
#include "testing.h"

START_TEST(test_insert_simple_select) {
  INIT_TEST(ctx);

  char* setup_queries[] = {
    "CREATE TABLE employees ("
    "id SERIAL PRIMKEY, "
    "email VARCHAR(255), "
    "name VARCHAR(100), "
    "age INT, "
    "salary FLOAT, "
    "department VARCHAR(100), "
    "is_active BOOL, "
    "last_login_date VARCHAR(50)"
    ");",

    "INSERT INTO employees VALUES (1, \"alice@example.com\", \"Alice Smith\", 30, 75000, \"Engineering\", true, \"2025-04-01\");",
    "INSERT INTO employees VALUES (2, \"bob@example.com\", \"Bob Johnson\", 25, 65000, \"Marketing\", false, \"2025-03-15\");",
    "INSERT INTO employees VALUES (3, \"charlie@example.com\", \"Charlie Davis\", 35, 85000, \"Engineering\", true, \"2025-04-10\");",
    "INSERT INTO employees VALUES (4, \"daisy@example.com\", \"Daisy Brown\", 22, 55000, \"HR\", true, \"2025-04-05\");",
    "INSERT INTO employees VALUES (5, \"eve@example.com\", \"Eve Wilson\", 40, 95000, \"Finance\", false, \"2025-03-01\");",
    "INSERT INTO employees VALUES (6, \"frank@example.com\", \"Frank Miller\", 29, 70000, \"Engineering\", true, \"2025-04-12\");",
    "INSERT INTO employees VALUES (7, \"grace@example.com\", \"Grace Taylor\", 31, 80000, \"Marketing\", true, \"2025-03-28\");",
    "INSERT INTO employees VALUES (8, \"heidi@example.com\", \"Heidi Garcia\", 27, 63000, \"HR\", false, \"2025-02-20\");",
    "INSERT INTO employees VALUES (9, \"ivan@example.com\", \"Ivan Chen\", 33, 82000, \"Engineering\", true, \"2025-04-08\");",
    "INSERT INTO employees VALUES (10, \"judy@example.com\", \"Judy Lopez\", 28, 68000, \"Finance\", false, \"2025-03-10\");",
    "INSERT INTO employees VALUES (11, \"kevin@example.com\", \"Kevin Adams\", 38, 88000, \"Engineering\", true, \"2025-04-11\");",
    "INSERT INTO employees VALUES (12, \"lisa@example.com\", \"Lisa Wright\", 26, 61000, \"Marketing\", true, \"2025-03-25\");",
    "INSERT INTO employees VALUES (13, \"mike@example.com\", \"Mike Rodriguez\", 34, 79000, \"Finance\", false, \"2025-02-15\");",
    "INSERT INTO employees VALUES (14, \"nancy@example.com\", \"Nancy Lee\", 32, 76000, \"Engineering\", true, \"2025-04-07\");",
    "INSERT INTO employees VALUES (15, \"oscar@example.com\", \"Oscar Kim\", 29, 71000, \"HR\", true, \"2025-03-20\");",
  };


  struct {
    char* query;
    int expected_rows;
  } complex_where_cases[] = {
    { "SELECT * FROM employees WHERE ((age > 30 AND salary > 80000) OR (department = \"Engineering\" AND NOT is_active = false)) AND (age < 35 OR salary > 85000);", 6 },
    { "SELECT * FROM employees WHERE (((age >= 30 AND age <= 35) OR (salary >= 75000 AND salary <= 85000)) AND is_active = true) OR (department = \"Finance\" AND age < 30);", 6 },
    { "SELECT * FROM employees WHERE (department = \"Engineering\" OR department = \"Marketing\" OR department = \"Finance\") AND (age < 30 OR age > 35) AND is_active = true;", 3 },
    { "SELECT * FROM employees WHERE (department = \"Engineering\" AND salary > 80000 AND is_active = true) OR (department = \"Finance\" AND age > 30 AND is_active = false) OR (department = \"Marketing\" AND age < 30 AND salary < 70000);", 7 },
    { "SELECT * FROM employees WHERE NOT ((department = \"Engineering\" AND salary < 80000) OR (is_active = false AND age < 30));", 9 },
    { "SELECT * FROM employees WHERE ((age > 25 AND (department = \"Engineering\" OR department = \"Finance\")) OR (salary > 70000 AND (is_active = true OR age >= 35))) AND NOT (department = \"HR\" AND salary < 60000);", 11 },
    { "SELECT * FROM employees WHERE (age >= 30 AND age <= 35 AND salary >= 75000) OR (age < 30 AND salary > 65000 AND is_active = true);", 8 },
    { "SELECT * FROM employees WHERE (((age > 30 AND department = \"Engineering\") OR (salary > 75000 AND is_active = true)) AND (age < 40 OR department != \"HR\")) OR ((department = \"Marketing\" OR department = \"Finance\") AND NOT (salary < 60000 OR age > 35));", 9 },
    { "SELECT * FROM employees WHERE NOT (department = \"HR\") AND NOT (salary < 70000 AND is_active = false) AND NOT (age > 35 OR department = \"Finance\");", 7 },    
  };
  
  for (int i = 0; i < sizeof(setup_queries) / sizeof(setup_queries[0]); i++) {
    printf("Executing setup query #%d: %s\n", i + 1, setup_queries[i]);
    ExecutionResult res = process(ctx, setup_queries[i]).exec;
    ck_assert_int_eq(res.code, 0);
  }

  for (int i = 0; i < sizeof(complex_where_cases) / sizeof(complex_where_cases[0]); i++) {
    printf("Executing complex WHERE query #%d: %s\n", i + 1, complex_where_cases[i].query);
    ExecutionResult res = process(ctx, complex_where_cases[i].query).exec;
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == complex_where_cases[i].expected_rows,
      "Complex WHERE query #%d failed: expected %d rows, got %d",
      i + 1, complex_where_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }  

  ctx_free(ctx);
}
END_TEST


Suite* complex_where_suite(void) {
  Suite* s = suite_create("ComplexWhere");
  TCase* tc = tcase_create("ComplexWhereTests");

  tcase_add_test(tc, test_insert_simple_select);
  suite_add_tcase(s, tc);
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(complex_where_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}