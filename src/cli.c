#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "executor.h"
#include "utils/cli.h"
#include "utils/log.h"

int main(int argc, char* argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--verbose") == 0 && i + 1 < argc) {
      *verbosity_level = atoi(argv[++i]); 
    }
  }

  if (*verbosity_level == 1) {
    LOG_WARN("Invalid verbosity level: %d. Defaulting to WARN.", *verbosity_level);
  }

  Context* ctx = ctx_init();

  if (!ctx) {
    fprintf(stderr, "Failed to initialize context\n");
    return 1;
  }

  char input[1024];
  while (1) {
    char short_cwd[256];
    get_short_cwd(short_cwd, sizeof(short_cwd));

    printf(COLOR_RED "/%s " COLOR_MAGENTA "[jugad-cli]" COLOR_RESET "> " , short_cwd);
    fflush(stdout);

    if (!fgets(input, sizeof(input), stdin)) {
      printf("\nError reading input. Exiting...\n");
      break;
    }

    input[strcspn(input, "\n")] = 0;

    if (strcmp(input, ".quit") == 0) {
      LOG_INFO("Exiting...");
      break;
    }

    if (!process_dot_cmd(ctx, input)) {
      ExecutionResult result = process(ctx, input);
      printf("Result: %s sc: %d\n", result.message, result.status_code);
    }
  }

  ctx_free(ctx);
  return 0;
}
