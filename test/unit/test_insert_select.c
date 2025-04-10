#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "executor.h"

START_TEST(test_create_users_and_insert_data) {
  char path[MAX_PATH_LENGTH];
  sprintf(path, "%s" SEP "test_insert_select", DB_ROOT_DIRECTORY);

  Context* ctx = ctx_init(path);
  ck_assert_ptr_nonnull(ctx);

  char* setup_queries[] = {
    "CREATE TABLE users ("
    "id SERIAL PRIMKEY, "
    "email VARCHAR(255), "
    "age INT, "
    "is_active BOOL"
    ");",

    "INSERT INTO users VALUES (1, \"alice@example.com\", 30, true);",
    "INSERT INTO users VALUES (2, \"bob@example.com\", 25, false);",
    "INSERT INTO users VALUES (3, \"charlie@example.com\", 35, true);",
    "INSERT INTO users VALUES (4, \"daisy@example.com\", 22, true);",
    "INSERT INTO users VALUES (5, \"eve@example.com\", 40, false);",
    "INSERT INTO users VALUES (6, \"frank@example.com\", 29, true);",
    "INSERT INTO users VALUES (7, \"grace@example.com\", 31, true);",
    "INSERT INTO users VALUES (8, \"heidi@example.com\", 27, false);",
    "INSERT INTO users VALUES (9, \"ivan@example.com\", 33, true);",
    "INSERT INTO users VALUES (10, \"judy@example.com\", 28, false);",
  };


  struct {
    char* query;
    int expected_rows;
  } select_cases[] = {
    { "SELECT * FROM users WHERE id = 1;", 1 },
    { "SELECT * FROM users WHERE age != 30;", 9 },
    { "SELECT * FROM users WHERE age > 30;", 4 },
    { "SELECT * FROM users WHERE age >= 30;", 5 },
    { "SELECT * FROM users WHERE age < 30;", 5 },
    { "SELECT * FROM users WHERE age <= 30;", 6 },
    { "SELECT * FROM users WHERE age > 25 AND is_active = true;", 5 },
    { "SELECT * FROM users WHERE age < 25 OR is_active = false;", 5 },
    { "SELECT * FROM users WHERE NOT is_active = true;", 4 },
    { "SELECT * FROM users WHERE (age > 25 AND age < 35);", 6 },
    { "SELECT * FROM users WHERE (is_active = true OR age < 25);", 6 },
    { "SELECT * FROM users WHERE (age > 25 AND (is_active = true OR age = 27));", 6 }
  };
  
  for (int i = 0; i < sizeof(setup_queries) / sizeof(setup_queries[0]); i++) {
    printf("Executing query #%d: %s\n", i + 1, setup_queries[i]);
    ExecutionResult res = process(ctx, setup_queries[i]);
    ck_assert_int_eq(res.code, 0);
  }

  for (int i = 0; i < sizeof(select_cases) / sizeof(select_cases[0]); i++) {
    printf("Executing SELECT #%d: %s\n", i + 1, select_cases[i].query);
    ExecutionResult res = process(ctx, select_cases[i].query);
  
    ck_assert_int_eq(res.code, 0);
    ck_assert_msg(res.row_count == select_cases[i].expected_rows,
      "Query #%d failed: expected %d rows, got %d",
      i + 1, select_cases[i].expected_rows, res.row_count);
  
    if (res.owns_rows) {
      free(res.rows);
    }
  }  

  ctx_free(ctx);
}
END_TEST


Suite* insert_select_suite(void) {
  Suite* s = suite_create("Insert+Select");
  TCase* tc = tcase_create("InsertSelectTests");

  tcase_add_test(tc, test_create_users_and_insert_data);
  suite_add_tcase(s, tc);
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(insert_select_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}

