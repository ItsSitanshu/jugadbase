#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "kernel/executor.h"
#include "utils/testing.h"

START_TEST(test_read_table_schema) {
  INIT_TEST(db);

  char* create_queries[] = {
    "CREATE TABLE users ("
    "id SERIAL PRIMKEY, "
    "name VARCHAR(50) NOT NULL UNIQUE, "
    "age INT CHECK(age > 0), "
    "email VARCHAR(100) DEFAULT \"unknown\", "
    "role_id INT FRNKEY REF roles(id)"
    ");",

    "CREATE TABLE categories ("
    "category_id SERIAL PRIMKEY, "
    "category_name VARCHAR(50) NOT NULL"
    ");",

    "CREATE TABLE products ("
    "product_id SERIAL PRIMKEY, "
    "product_name VARCHAR(100) NOT NULL, "
    "price DECIMAL(10, 2) CHECK(price > 0), "
    "quantity INT DEFAULT 0, "
    "category_id INT FRNKEY REF categories(id)"
    ");",

    "CREATE TABLE orders ("
    "order_id SERIAL PRIMKEY, "
    "order_date DATE NOT NULL, "
    "user_id INT FRNKEY REF users(id), "
    "total_amount DECIMAL(10, 2) CHECK(total_amount >= 0)"
    ");",


    "CREATE TABLE   payments ("
    "payment_id SERIAL PRIMKEY, "
    "order_id INT FRNKEY REF orders(order_id), "
    "payment_date DATE NOT NULL, "
    "payment_method VARCHAR(20) NOT NULL"
    ");",
  };

  for (int i = 0; i < sizeof(create_queries) / sizeof(create_queries[0]); i++) {
    printf("Executing query #%d: %s\n", i + 1, create_queries[i]);
    ExecutionResult res = process(db, create_queries[i]).exec;
    ck_assert_int_eq(res.code, 0);
  }

  TableSchema* schema = find_table_schema_tc(db, "users");
  ck_assert_str_eq(schema->table_name, "users");
  ck_assert_int_eq(schema->column_count, 5);
  ck_assert_str_eq(schema->columns[0].name, "id");
  ck_assert_int_eq(schema->columns[0].type, TOK_T_SERIAL);
  ck_assert_int_eq(schema->columns[0].is_primary_key, 1);
  ck_assert_int_eq(schema->columns[0].has_sequence, 1);
  ck_assert_int_eq(schema->columns[0].is_unique, 1);
  ck_assert_int_eq(schema->columns[0].is_not_null, 1);

  ck_assert_str_eq(schema->columns[1].name, "name");
  ck_assert_int_eq(schema->columns[1].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema->columns[1].type_varchar, 50);
  ck_assert_int_eq(schema->columns[1].is_not_null, 1);
  ck_assert_int_eq(schema->columns[1].is_unique, 1);

  ck_assert_str_eq(schema->columns[2].name, "age");
  ck_assert_int_eq(schema->columns[2].type, TOK_T_INT);
  ck_assert_int_eq(schema->columns[2].has_check, 1);
  ck_assert_str_eq(schema->columns[2].check_expr, "age>0");

  ck_assert_str_eq(schema->columns[3].name, "email");
  ck_assert_int_eq(schema->columns[3].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema->columns[3].type_varchar, 100);
  ck_assert_int_eq(schema->columns[3].has_default, 1);
  // ck_assert_str_eq(schema->columns[3].default_value, "unknown");

  ck_assert_str_eq(schema->columns[4].name, "role_id");
  ck_assert_int_eq(schema->columns[4].type, TOK_T_INT);
  ck_assert_int_eq(schema->columns[4].is_foreign_key, 1);
  ck_assert_str_eq(schema->columns[4].foreign_table, "roles");
  ck_assert_str_eq(schema->columns[4].foreign_column, "id");

  // --- Validation for 'categories' table ---

  TableSchema* schema_categories = find_table_schema_tc(db, "categories");
  ck_assert_str_eq(schema_categories->table_name, "categories");
  ck_assert_int_eq(schema_categories->column_count, 2);

  ck_assert_str_eq(schema_categories->columns[0].name, "category_id");
  ck_assert_int_eq(schema_categories->columns[0].type, TOK_T_SERIAL);
  ck_assert_int_eq(schema_categories->columns[0].is_primary_key, 1);
  ck_assert_int_eq(schema_categories->columns[0].has_sequence, 1);

  ck_assert_str_eq(schema_categories->columns[1].name, "category_name");
  ck_assert_int_eq(schema_categories->columns[1].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema_categories->columns[1].type_varchar, 50);
  ck_assert_int_eq(schema_categories->columns[1].is_not_null, 1);

  // --- Validation for 'products' table ---
  TableSchema* schema_products = find_table_schema_tc(db, "products");
  ck_assert_str_eq(schema_products->table_name, "products");
  ck_assert_int_eq(schema_products->column_count, 5);
  ck_assert_str_eq(schema_products->columns[0].name, "product_id");
  ck_assert_int_eq(schema_products->columns[0].type, TOK_T_SERIAL);
  ck_assert_int_eq(schema_products->columns[0].is_primary_key, 1);
  ck_assert_int_eq(schema_products->columns[0].has_sequence, 1);

  ck_assert_str_eq(schema_products->columns[1].name, "product_name");
  ck_assert_int_eq(schema_products->columns[1].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema_products->columns[1].type_varchar, 100);
  ck_assert_int_eq(schema_products->columns[1].is_not_null, 1);

  ck_assert_str_eq(schema_products->columns[2].name, "price");
  ck_assert_int_eq(schema_products->columns[2].type, TOK_T_DECIMAL);
  ck_assert_str_eq(schema_products->columns[2].check_expr, "price>0");

  ck_assert_str_eq(schema_products->columns[3].name, "quantity");
  ck_assert_int_eq(schema_products->columns[3].type, TOK_T_INT);
  // ck_assert_str_eq(schema_products->columns[3].default_value, "0");

  ck_assert_str_eq(schema_products->columns[4].name, "category_id");
  ck_assert_int_eq(schema_products->columns[4].type, TOK_T_INT);
  ck_assert_int_eq(schema_products->columns[4].is_foreign_key, 1);
  ck_assert_str_eq(schema_products->columns[4].foreign_table, "categories");
  ck_assert_str_eq(schema_products->columns[4].foreign_column, "id");

  // --- Validation for 'orders' table ---
  TableSchema* schema_orders = find_table_schema_tc(db, "orders");
  ck_assert_str_eq(schema_orders->table_name, "orders");
  ck_assert_int_eq(schema_orders->column_count, 4);

  ck_assert_str_eq(schema_orders->columns[0].name, "order_id");
  ck_assert_int_eq(schema_orders->columns[0].type, TOK_T_SERIAL);
  ck_assert_int_eq(schema_orders->columns[0].is_primary_key, 1);
  ck_assert_int_eq(schema_orders->columns[0].has_sequence, 1);

  ck_assert_str_eq(schema_orders->columns[1].name, "order_date");
  ck_assert_int_eq(schema_orders->columns[1].type, TOK_T_DATE);
  ck_assert_int_eq(schema_orders->columns[1].is_not_null, 1);

  ck_assert_str_eq(schema_orders->columns[2].name, "user_id");
  ck_assert_int_eq(schema_orders->columns[2].type, TOK_T_INT);
  ck_assert_int_eq(schema_orders->columns[2].is_foreign_key, 1);
  ck_assert_str_eq(schema_orders->columns[2].foreign_table, "users");
  ck_assert_str_eq(schema_orders->columns[2].foreign_column, "id");

  ck_assert_str_eq(schema_orders->columns[3].name, "total_amount");
  ck_assert_int_eq(schema_orders->columns[3].type, TOK_T_DECIMAL);
  ck_assert_str_eq(schema_orders->columns[3].check_expr, "total_amount>=0");

  // --- Validation for 'payments' table ---
  TableSchema* schema_payments = find_table_schema_tc(db, "payments");
  ck_assert_str_eq(schema_payments->table_name, "payments");
  ck_assert_int_eq(schema_payments->column_count, 4);

  ck_assert_str_eq(schema_payments->columns[0].name, "payment_id");
  ck_assert_int_eq(schema_payments->columns[0].type, TOK_T_SERIAL);
  ck_assert_int_eq(schema_payments->columns[0].is_primary_key, 1);
  ck_assert_int_eq(schema_payments->columns[0].has_sequence, 1);

  ck_assert_str_eq(schema_payments->columns[1].name, "order_id");
  ck_assert_int_eq(schema_payments->columns[1].type, TOK_T_INT);
  ck_assert_int_eq(schema_payments->columns[1].is_foreign_key, 1);
  ck_assert_str_eq(schema_payments->columns[1].foreign_table, "orders");
  ck_assert_str_eq(schema_payments->columns[1].foreign_column, "order_id");

  ck_assert_str_eq(schema_payments->columns[2].name, "payment_date");
  ck_assert_int_eq(schema_payments->columns[2].type, TOK_T_DATE);
  ck_assert_int_eq(schema_payments->columns[2].is_not_null, 1);

  ck_assert_str_eq(schema_payments->columns[3].name, "payment_method");
  ck_assert_int_eq(schema_payments->columns[3].type, TOK_T_VARCHAR);
  ck_assert_int_eq(schema_payments->columns[3].type_varchar, 20);
  ck_assert_int_eq(schema_payments->columns[3].is_not_null, 1);

  db_free(db);
}
END_TEST


Suite* create_table_suite(void) {
  Suite* s = suite_create("Create");
  TCase* tc = tcase_create("CreateTests");

  tcase_add_test(tc, test_read_table_schema);
  suite_add_tcase(s, tc);
  return s;
}

int main(void) {
  SRunner* sr = srunner_create(create_table_suite());
  srunner_run_all(sr, CK_NORMAL);
  int failures = srunner_ntests_failed(sr);
  srunner_free(sr);
  return (failures == 0) ? 0 : 1;
}