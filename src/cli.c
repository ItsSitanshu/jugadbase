#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "executor.h"
#include "utils/cli.h"

int main(int argc, char* argv[]) {
  Context* ctx = ctx_init();

  if (!ctx) {
    fprintf(stderr, "Failed to initialize context\n");
    return 1;
  }

  char* schema_name = (argc > 1) ? argv[1] : "default.jdb";
  if (argc > 1) {
    switch_schema(ctx, argv[1]);
  } else {
    switch_schema(ctx, schema_name);
  }

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--verbose") == 0 && i + 1 < argc) {
      verbosity_level = atoi(argv[++i]); 
    }
  }

  if (verbosity_level == 1) {
    LOG_WARN("Invalid verbosity level: %d. Defaulting to WARN.", verbosity_level);
  }

  char input[1024];
  while (1) {
    char short_cwd[256];
    get_short_cwd(short_cwd, sizeof(short_cwd));

    printf(CYAN "/%s " GREEN "[%s]" RESET "> " , short_cwd, ctx->filename);
    fflush(stdout);

    if (!fgets(input, sizeof(input), stdin)) {
      printf("\nError reading input. Exiting...\n");
      break;
    }

    input[strcspn(input, "\n")] = 0;

    if (strcmp(input, ".quit") == 0) {
      printf(GREEN "Exiting...\n" RESET);
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
