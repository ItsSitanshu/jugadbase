#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "utils/cli.h"
#include "utils/log.h"
#include "utils/jugadline.h"

#include "executor.h"

int main(int argc, char* argv[]) {
  const char* output_filename = NULL;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--verbose") == 0 && i + 1 < argc) {
      *verbosity_level = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
      output_filename = argv[++i];
    }
  }

  if (*verbosity_level == 1) {
    LOG_WARN("Invalid verbosity level: %d. Defaulting to WARN.", *verbosity_level);
  }

  Database* db = db_init(DB_ROOT_DIRECTORY);

  if (!db) {
    fprintf(stderr, "Failed to initialize database\n");
    return 1;
  }

  char* input = NULL;

  CommandHistory history = { .current = 1, .size = 0 };

  char short_cwd[256];
  char prompt[512];
  get_short_cwd(short_cwd, sizeof(short_cwd));

  while (1) {
    snprintf(prompt, sizeof(prompt), COLOR_RED "/%s " COLOR_MAGENTA "[jugad-cli]" COLOR_RESET "> ", short_cwd);    
    
    input = jugadline(&history, prompt);

    if (!process_dot_cmd(db, input)) {
      Result result = process(db, input);
      if (output_filename) {
        print_text_table_to_file(result.exec, result.cmd, output_filename);
      }
    }
  }

  db_free(db);
  for (int i = 0; i < history.size; i++) {
    free(history.history[i]);
  }
  return 0;
}
