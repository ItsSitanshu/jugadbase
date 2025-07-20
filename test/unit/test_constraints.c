#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kernel/kernel.h"
#include "utils/testing.h"

START_TEST(test_constraints) {
  INIT_TEST(db);

  const char* setup_queries[] = {
    "CREATE TABLE dpt (id SERIAL, dptname VARCHAR(100) UNIQUE, number_of_staff INT);",
    "CREATE TABLE employees (id SERIAL PRIMKEY, email VARCHAR(255), name VARCHAR(100), age INT, salary FLOAT, department VARCHAR(100) "
    "FRNKEY REFERENCES dpt (dptname) ON DELETE CASCADE ON UPDATE SET NULL, is_active BOOL, last_login_date DATE);",
    NULL
  };

  for (int i = 0; setup_queries[i] != NULL; i++) {
    ExecutionResult res = process(db, setup_queries[i]).exec;
    ck_assert_int_eq(res.code, 0);
  }

  ExecutionResult dep_insert = process(db,
    "INSERT INTO dpt (dptname, number_of_staff) VALUES "
    "('Engineering', 6), ('Marketing', 3), ('HR', 3), ('Finance', 3), ('Accounts', 1);").exec;
  ck_assert_int_eq(dep_insert.code, 0);

  ExecutionResult emp_insert = process(db,
    "INSERT INTO employees VALUES "
    "(1, 'alice@example.com', 'Alice Smith', 30, 75000, 'Engineering', true, '2025-04-01'),"
    "(2, 'bob@example.com', 'Bob Johnson', 25, 65000, 'Marketing', false, '2025-03-15'),"
    "(3, 'charlie@example.com', 'Charlie Davis', 35, 85000, 'Engineering', true, '2025-04-10'),"
    "(4, 'daisy@example.com', 'Daisy Brown', 22, 55000, 'HR', true, '2025-04-05'),"
    "(5, 'eve@example.com', 'Eve Wilson', 40, 95000, 'Finance', false, '2025-03-01');").exec;
  ck_assert_int_eq(emp_insert.code, 0);

  // === Test 1: ON DELETE SET NULL ===
  ExecutionResult del_res = process(db, "DELETE FROM dpt WHERE dptname = 'HR';").exec;
  ck_assert_int_eq(del_res.code, 0);

  ExecutionResult null_check = process(db,
    "SELECT department FROM employees WHERE department = 'HR';").exec;
  ck_assert_int_eq(null_check.code, 0);
  ck_assert_int_eq(null_check.row_count, 0);
  if (null_check.owns_rows) free(null_check.rows);

  // === Test 2: ON UPDATE SET NULL ===
  ExecutionResult upd_res = process(db, "UPDATE dpt SET dptname = 'Finances' WHERE dptname = 'Finance';").exec;
  ck_assert_int_eq(upd_res.code, 0);

  ExecutionResult null_check2 = process(db,
    "SELECT department FROM employees WHERE id = 5;").exec;
  ck_assert_int_eq(null_check2.code, 0);
  ck_assert_int_eq(null_check2.row_count, 1);
  ck_assert_ptr_nonnull(null_check2.rows);
  ck_assert_ptr_nonnull(null_check2.rows[0].values);
  ck_assert_int_eq(null_check2.rows[0].values[0].is_null, 1);

  if (null_check2.owns_rows) free(null_check2.rows);

  // === Test 3: UNIQUE constraint violation ===
  ExecutionResult dup_dept = process(db, "INSERT INTO dpt (dptname, number_of_staff) VALUES ('Engineering', 1);").exec;
  ck_assert_int_ne(dup_dept.code, 0); 

  // === Test 4: PRIMARY KEY violation ===
  ExecutionResult dup_pk = process(db,
    "INSERT INTO employees VALUES (1, 'duplicate@example.com', 'Dup', 22, 42000, 'Marketing', true, '2025-03-22');").exec;
  ck_assert_int_ne(dup_pk.code, 0);

  // === Test 5: Invalid FK insert ===
  ExecutionResult bad_fk = process(db,
    "INSERT INTO employees VALUES (30, 'ghost@domain.com', 'Ghost', 50, 90000, 'GhostDept', true, '2025-04-01');").exec;
  ck_assert_int_ne(bad_fk.code, 0);

  db_free(db);
}
END_TEST

Suite* constraints_suite(void) {
  Suite* s = suite_create("Constraints");

  TCase* tc = tcase_create("Constraint Tests");
  tcase_add_test(tc, test_constraints);
  suite_add_tcase(s, tc);

  return s;
}

int main(void) {
  SRunner* sr = srunner_create(constraints_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}
