#include <string.h>
#include <context.h>


#define INIT_TEST(ctx_var)                                                  \
  char path[MAX_PATH_LENGTH];                                                \
  do {                                                                       \
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
  Context* ctx_var = ctx_init(path);                                         \
  ck_assert_ptr_nonnull(ctx_var);                                    
  
#define ck_assert_ptr_nonnull(X) _ck_assert_ptr_null(X, !=)
