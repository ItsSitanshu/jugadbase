#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/time.h>
#endif

extern int* verbosity_level;

#define COLOR_RESET   "\x1b[0m"
#define COLOR_RED     "\x1b[38;5;204m"  /* Flamingo (#F2CDCD) */
#define COLOR_YELLOW  "\x1b[38;5;180m"  /* Peach (#FAB387) */
#define COLOR_GREEN   "\x1b[38;5;114m"  /* Green (#A6E3A1) */
#define COLOR_BLUE    "\x1b[38;5;110m"  /* Sapphire (#74C7EC) */
#define COLOR_CYAN    "\x1b[38;5;117m"  /* Sky (#89DCEB) */
#define COLOR_MAGENTA "\x1b[38;5;176m"  /* Mauve (#CBA6F7) */

#define MAX_LOG_PATH 256

static inline int is_god_mode(void) {
  static int cached = -1;
  if (cached == -1) {
    const char* val = getenv("JUGADBASE_GOD_MODE");
    cached = (val != NULL && strcmp(val, "1") == 0);
  }
  return cached;
}

static inline int get_verbosity() {
  return *verbosity_level;
}

static const char* token_type_strings[] = {
  "i", "vch", "ch", "tex",
  "b", "f", "d", "dec",
  "dt", "tm", "tmtz", "dtm",
  "dttz", "ts", "tstz", "tint",
  "blb", "jsn", "uid", "ser",
  "u", "str"
};

void get_current_time_with_ms(char* time_str, size_t size);
int is_new_log_cycle(const char* current_file, const char* new_file);
void log_transaction(const char* filename, const char* fmt, ...);
char* tolower_copy(const char* s);
const char* get_token_type(int type);

#define LOG_MESSAGE(level, color, level_threshold, fmt, ...) \
  do { \
    if (get_verbosity() >= level_threshold) { \
      char time_str[30]; \
      get_current_time_with_ms(time_str, sizeof(time_str)); \
      fprintf(stderr, "<%s>%s[%s] " fmt COLOR_RESET "\n", \
        time_str, color, #level, ##__VA_ARGS__); \
    } \
  } while (0)

#define LOG_FATAL(fmt, ...) \
  do { \
    LOG_MESSAGE(FATAL, COLOR_MAGENTA, 3, fmt, ##__VA_ARGS__); \
    exit(EXIT_FAILURE); \
  } while (0)

#define LOG_DEBUG(fmt, ...) LOG_MESSAGE(DEBUG, COLOR_GREEN,  3, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  LOG_MESSAGE(INFO,  COLOR_BLUE,   2, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  LOG_MESSAGE(WARN,  COLOR_YELLOW, 1, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) LOG_MESSAGE(ERROR, COLOR_RED,    0, fmt, ##__VA_ARGS__)

#define LOG_TRANS(filename, fmt, ...) log_transaction(filename, fmt, ##__VA_ARGS__)

#endif /* LOG_H */