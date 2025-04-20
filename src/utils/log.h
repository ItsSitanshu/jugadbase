#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <stdarg.h>

#ifdef _WIN32
  #include <windows.h>
#else
  #include <sys/time.h>
#endif

extern int* verbosity_level;

#define COLOR_RESET   "\x1b[0m"
#define COLOR_RED     "\x1b[38;5;204m"  // Flamingo (#F2CDCD)
#define COLOR_YELLOW  "\x1b[38;5;180m"  // Peach (#FAB387)
#define COLOR_GREEN   "\x1b[38;5;114m"  // Green (#A6E3A1)
#define COLOR_BLUE    "\x1b[38;5;110m"  // Sapphire (#74C7EC)
#define COLOR_CYAN    "\x1b[38;5;117m"  // Sky (#89DCEB)
#define COLOR_MAGENTA "\x1b[38;5;176m"  // Mauve (#CBA6F7)

#define MAX_LOG_PATH 256

static inline int get_verbosity() { return *verbosity_level; }

static void get_current_time_with_ms(char* time_str, size_t size) {
  struct timeval tv;
  struct tm* tm_info;

  gettimeofday(&tv, NULL); 
  tm_info = localtime(&tv.tv_sec); 

  strftime(time_str, size, "%Y-%m-%d %H:%M:%S", tm_info);
  snprintf(time_str + strlen(time_str), size - strlen(time_str), ".%03ld", (long)(tv.tv_usec / 1000));  // Append milliseconds
}

static int is_new_log_cycle(const char* current_file, const char* new_file) {
  struct tm current_tm, new_tm;
  sscanf(current_file, "%4d-%2d-%2d-%2d%*s", &current_tm.tm_year, &current_tm.tm_mon, &current_tm.tm_mday, &current_tm.tm_hour);
  current_tm.tm_year -= 1900;
  current_tm.tm_mon -= 1;  

  sscanf(new_file, "%4d-%2d-%2d-%2d%*s", &new_tm.tm_year, &new_tm.tm_mon, &new_tm.tm_mday, &new_tm.tm_hour);
  new_tm.tm_year -= 1900;
  new_tm.tm_mon -= 1; 

  if (current_tm.tm_hour != new_tm.tm_hour) {
    return 1;
  }

  return 0; 
}

static void log_transaction(const char* filename, const char* fmt, ...) {
  struct timeval tv;

  char time_str[30];
  char log_file_path[MAX_LOG_PATH];
  get_current_time_with_ms(time_str, sizeof(time_str));

  struct tm tm_info;
  gettimeofday(&tv, NULL);
  tm_info = *localtime(&tv.tv_sec);

  int hour = tm_info.tm_hour;
  if (hour < 12) {
    snprintf(log_file_path, sizeof(log_file_path), "%s%02d-%02d-%02d-0h.log", filename, tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday);
  } else {
    snprintf(log_file_path, sizeof(log_file_path), "%s%02d-%02d-%02d-12h.log", filename, tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday);
  }

  FILE* log_file = fopen(log_file_path, "a");
  if (log_file == NULL) {
    fprintf(stderr, "Error opening log file: %s\n", log_file_path);
    return;
  }

  va_list args;
  va_start(args, fmt);
  vfprintf(log_file, fmt, args);
  va_end(args);

  fclose(log_file);
}

static char* tolower_copy(const char* s) {
  size_t len = strlen(s);
  char* lower = malloc(len + 1);
  if (!lower) return NULL;
  for (size_t i = 0; i < len; ++i) {
    lower[i] = tolower((unsigned char)s[i]);
  }
  lower[len] = '\0';
  return lower;
}

static const char* token_type_strings[] = {
  "i",   "vch",  "ch",   "tex",
  "b",   "f",   "d",   "dec",
  "dt",  "tm", "tmtz", "dtm",
  "dttz", "ts", "tstz", "tint",
  "blb", "jsn",  "uid",  "ser",
  "u",   "str"
};

static const char* get_token_type(int type) {
  if (type >= 0 && type < 18) {
    return token_type_strings[type];
  } else {
    return "UNKNOWN";
  }
}

#define LOG_MESSAGE(level, color, level_threshold, fmt, ...) \
  do { \
    if (get_verbosity() >= level_threshold) { \
      char time_str[30];  \
      get_current_time_with_ms(time_str, sizeof(time_str)); \
      fprintf(stderr, "<%s>%s[%s] " fmt COLOR_RESET "\n", time_str, color, #level, ##__VA_ARGS__ ); \
    } \
  } while (0)

#define LOG_FATAL(fmt, ...) \
  do { \
    LOG_MESSAGE(FATAL, COLOR_MAGENTA,    3, fmt, ##__VA_ARGS__); \
    exit(EXIT_FAILURE); \
  } while (0)

#define LOG_DEBUG(fmt, ...) LOG_MESSAGE(DEBUG, COLOR_GREEN,    3, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  LOG_MESSAGE(INFO,  COLOR_BLUE,   2, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  LOG_MESSAGE(WARN,  COLOR_YELLOW,  1, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) LOG_MESSAGE(ERROR, COLOR_RED,     0, fmt, ##__VA_ARGS__)

#define LOG_TRANS(filename, fmt, ...) log_transaction(filename, fmt, ##__VA_ARGS__)

#endif
