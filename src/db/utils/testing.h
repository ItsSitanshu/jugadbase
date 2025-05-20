#include <string.h>
#include "storage/database.h"


#define INIT_TEST(db_var)                                                  \
  char path[MAX_PATH_LENGTH];                                                \
  do {                                                                      \
    *verbosity_level = 2;                                                    \
    const char* __file = __FILE__;                                           \
    const char* __filename = strrchr(__file, '/');                           \
    __filename = (__filename) ? __filename + 1 : __file;                     \
    char __basename[128];                                                   \
    strncpy(__basename, __filename, sizeof(__basename));                    \
    __basename[sizeof(__basename) - 1] = '\0';                               \
    char* __dot = strrchr(__basename, '.');                                  \
    if (__dot) {                                                             \
      *__dot = '\0';                                                         \
    }                                                                        \
    snprintf(path, sizeof(path), "%s" SEP "%s", DB_ROOT_DIRECTORY, __basename); \
  } while (0);                                                               \
  Database* db_var = db_init(path);                                         \
  ck_assert_ptr_nonnull(db_var);                                    
  
#define ck_assert_ptr_nonnull(X) _ck_assert_ptr_null(X, !=)

static char* employees_setup_queries[] = {
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

  "INSERT INTO employees VALUES (1, 'alice@example.com', 'Alice Smith', 30, 75000, 'Engineering', true, '2025-04-01');",
  "INSERT INTO employees VALUES (2, 'bob@example.com', 'Bob Johnson', 25, 65000, 'Marketing', false, '2025-03-15');",
  "INSERT INTO employees VALUES (3, 'charlie@example.com', 'Charlie Davis', 35, 85000, 'Engineering', true, '2025-04-10');",
  "INSERT INTO employees VALUES (4, 'daisy@example.com', 'Daisy Brown', 22, 55000, 'HR', true, '2025-04-05');",
  "INSERT INTO employees VALUES (5, 'eve@example.com', 'Eve Wilson', 40, 95000, 'Finance', false, '2025-03-01');",
  "INSERT INTO employees VALUES (6, 'frank@example.com', 'Frank Miller', 29, 70000, 'Engineering', true, '2025-04-12');",
  "INSERT INTO employees VALUES (7, 'grace@example.com', 'Grace Taylor', 31, 80000, 'Marketing', true, '2025-03-28');",
  "INSERT INTO employees VALUES (8, 'heidi@example.com', 'Heidi Garcia', 27, 63000, 'HR', false, '2025-02-20');",
  "INSERT INTO employees VALUES (9, 'ivan@example.com', 'Ivan Chen', 33, 82000, 'Engineering', true, '2025-04-08');",
  "INSERT INTO employees VALUES (10, 'judy@example.com', 'Judy Lopez', 28, 68000, 'Finance', false, '2025-03-10');",
  "INSERT INTO employees VALUES (11, 'kevin@example.com', 'Kevin Adams', 38, 88000, 'Engineering', true, '2025-04-11');",
  "INSERT INTO employees VALUES (12, 'lisa@example.com', 'Lisa Wright', 26, 61000, 'Marketing', true, '2025-03-25');",
  "INSERT INTO employees VALUES (13, 'mike@example.com', 'Mike Rodriguez', 34, 79000, 'Finance', false, '2025-02-15');",
  "INSERT INTO employees VALUES (14, 'nancy@example.com', 'Nancy Lee', 32, 76000, 'Engineering', true, '2025-04-07');",
  "INSERT INTO employees VALUES (15, 'oscar@example.com', 'Oscar Kim', 29, 71000, 'HR', true, '2025-03-20');"
};

static void setup_test_data(Database* db, char* setup_queries[]) {
  for (int i = 0; i < 16; i++) {
    printf("Executing setup query #%d: %s\n", i + 1, setup_queries[i]);
    ExecutionResult res = process(db, setup_queries[i]).exec;
    ck_assert_int_eq(res.code, 0);
  }
}