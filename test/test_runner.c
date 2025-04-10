#include <check.h>
#include <stdlib.h>

int main(void) {
  int failures = 0;

  Suite* suites[] = {
  };

  for (size_t i = 0; i < sizeof(suites)/sizeof(suites[0]); i++) {
    SRunner* sr = srunner_create(suites[i]);
    srunner_run_all(sr, CK_NORMAL);
    failures += srunner_ntests_failed(sr);
    srunner_free(sr);
  }

  return (failures == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
