#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <time.h>

extern int verbosity_level; 

#define COLOR_RESET   "\x1b[0m"
#define COLOR_RED     "\x1b[31m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_CYAN    "\x1b[36m"
#define COLOR_MAGENTA "\x1b[35m"

static inline int get_verbosity() { return verbosity_level; } 

#define LOG_MESSAGE(level, color, level_threshold, fmt, ...) \
  do { \
    if (get_verbosity() >= level_threshold) { \
      time_t now = time(NULL); \
      struct tm *tm_info = localtime(&now); \
      char time_str[10]; \
      strftime(time_str, sizeof(time_str), "%H:%M:%S", tm_info); \
      fprintf(stderr, "%s[%s] [%s] " fmt COLOR_RESET "\n", color, time_str, #level, ##__VA_ARGS__ ); \
    } \
  } while (0)

#define LOG_DEBUG(fmt, ...) LOG_MESSAGE(DEBUG, COLOR_BLUE,    3, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  LOG_MESSAGE(INFO,  COLOR_GREEN,   2, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  LOG_MESSAGE(WARN,  COLOR_YELLOW,  1, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) LOG_MESSAGE(ERROR, COLOR_RED,     0, fmt, ##__VA_ARGS__)
#define LOG_FATAL(fmt, ...) \
  do { \
    LOG_MESSAGE(FATAL, COLOR_MAGENTA, 0, fmt, ##__VA_ARGS__); \
    exit(EXIT_FAILURE); \
  } while (0)

#endif
