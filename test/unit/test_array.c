#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>

#include "kernel/executor.h"
#include "utils/testing.h"

START_TEST(test_array_column_access_and_update) {
  INIT_TEST(db);

  ExecutionResult create_res = process(db,
    "CREATE TABLE user_profiles ("
    "id SERIAL, "
    "name VARCHAR(50), "
    "favorite_numbers INT[], "
    "visited_countries TEXT[], "
    "activity_streaks BOOL[], "
    "preferences TEXT[], "
    "notes TEXT[]);").exec;
  ck_assert_int_eq(create_res.code, 0);

  process(db,
    "INSERT INTO user_profiles (id, name, favorite_numbers, visited_countries, activity_streaks, preferences) "
    "VALUES (0, 'Alice', '{3, 7, 21}', '{\"Nepal\", \"Japan\", \"Germany\"}', '{true, false, true, true}', "
    "'{\"Loves hiking\", \"Prefers dark mode\"}');");

  process(db,
    "INSERT INTO user_profiles (id, name, favorite_numbers, visited_countries, activity_streaks, preferences) "
    "VALUES (1, 'Bob', '{8, 13}', '{\"India\", \"Sri Lanka\"}', '{false, false, false, false}', "
    "'{\"Testing the app\"}');");

  ExecutionResult update_res1 = process(db,
    "UPDATE user_profiles SET favorite_numbers[1] = 99 WHERE id = 0;").exec;
  ck_assert_int_eq(update_res1.code, 0);

  ExecutionResult update_res2 = process(db,
    "UPDATE user_profiles SET visited_countries[0] = 'Bhutan' WHERE id = 1;").exec;
  ck_assert_int_eq(update_res2.code, 0);

  ExecutionResult update_res3 = process(db,
    "UPDATE user_profiles SET activity_streaks[2] = true WHERE id = 1;").exec;
  ck_assert_int_eq(update_res3.code, 0);

  struct {
    char* query;
    int expected_type;
    union {
      int int_val;
      char* text_val;
      bool bool_val;
    } expected;
  } tests[] = {
    { "SELECT favorite_numbers[1] FROM user_profiles WHERE id = 0;", TOK_T_INT, .expected.int_val = 99 },
    { "SELECT visited_countries[0] FROM user_profiles WHERE id = 1;", TOK_T_TEXT, .expected.text_val = "Bhutan" },
    { "SELECT activity_streaks[2] FROM user_profiles WHERE id = 1;", TOK_T_BOOL, .expected.bool_val = true },
  };

  for (int i = 0; i < sizeof(tests)/sizeof(tests[0]); i++) {
    printf("Running test %d: %s\n", i + 1, tests[i].query);
    ExecutionResult res = process(db, tests[i].query).exec;
    ck_assert_int_eq(res.code, 0);
    ck_assert_int_eq(res.row_count, 1);

    ColumnValue val = res.rows[0].values[0];

    switch (tests[i].expected_type) {
      case TOK_T_INT:
        ck_assert_int_eq(val.int_value, tests[i].expected.int_val);
        break;
      case TOK_T_TEXT:
        ck_assert_str_eq(val.str_value, tests[i].expected.text_val);
        break;
      case TOK_T_BOOL:
        ck_assert(val.bool_value == tests[i].expected.bool_val);
        break;
      default:
        ck_abort_msg("Unsupported value type");
    }

    if (res.owns_rows) free(res.rows);
  }

  db_free(db);
}
END_TEST

Suite* array_access_suite(void) {
  Suite* s = suite_create("ArrayAccessAndUpdate");

  TCase* tc = tcase_create("ArrayOps");
  tcase_add_test(tc, test_array_column_access_and_update);
  suite_add_tcase(s, tc);

  return s;
}

int main(void) {
  SRunner* sr = srunner_create(array_access_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}