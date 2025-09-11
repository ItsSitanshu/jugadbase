#include "log.h"
#include "xmem.h"

#include <string.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>


int actual_verbosity_level = 3;
int* verbosity_level = &actual_verbosity_level; 


void get_current_time_with_ms(char* time_str, size_t size) {
  struct timeval tv;
  struct tm* tm_info;

  gettimeofday(&tv, NULL);
  tm_info = localtime(&tv.tv_sec);

  strftime(time_str, size, "%Y-%m-%d %H:%M:%S", tm_info);
  snprintf(time_str + strlen(time_str),
           size - strlen(time_str),
           ".%03ld",
           (long)(tv.tv_usec / 1000));
}

int is_new_log_cycle(const char* current_file, const char* new_file) {
  struct tm current_tm, new_tm;
  sscanf(current_file, "%4d-%2d-%2d-%2d%*s",
         &current_tm.tm_year, &current_tm.tm_mon,
         &current_tm.tm_mday, &current_tm.tm_hour);
  current_tm.tm_year -= 1900;
  current_tm.tm_mon -= 1;

  sscanf(new_file, "%4d-%2d-%2d-%2d%*s",
         &new_tm.tm_year, &new_tm.tm_mon,
         &new_tm.tm_mday, &new_tm.tm_hour);
  new_tm.tm_year -= 1900;
  new_tm.tm_mon -= 1;

  return current_tm.tm_hour != new_tm.tm_hour;
}

void log_transaction(const char* filename, const char* fmt, ...) {
  struct timeval tv;
  char time_str[30];
  char log_file_path[MAX_LOG_PATH];

  get_current_time_with_ms(time_str, sizeof(time_str));

  struct tm tm_info;
  gettimeofday(&tv, NULL);
  tm_info = *localtime(&tv.tv_sec);

  int hour = tm_info.tm_hour;
  if (hour < 12) {
    snprintf(log_file_path, sizeof(log_file_path),
             "%s%02d-%02d-%02d-0h.log",
             filename, tm_info.tm_year + 1900,
             tm_info.tm_mon + 1, tm_info.tm_mday);
  } else {
    snprintf(log_file_path, sizeof(log_file_path),
             "%s%02d-%02d-%02d-12h.log",
             filename, tm_info.tm_year + 1900,
             tm_info.tm_mon + 1, tm_info.tm_mday);
  }

  FILE* log_file = fopen(log_file_path, "a");
  if (!log_file) {
    fprintf(stderr, "Error opening log file: %s\n", log_file_path);
    return;
  }

  va_list args;
  va_start(args, fmt);
  vfprintf(log_file, fmt, args);
  va_end(args);

  fclose(log_file);
}

char* tolower_copy(const char* s) {
  size_t len = strlen(s);
  char* lower = xmalloc(len + 1);
  if (!lower) return NULL;

  for (size_t i = 0; i < len; ++i) {
    lower[i] = tolower((unsigned char)s[i]);
  }
  lower[len] = '\0';
  return lower;
}

const char* get_token_type(int type) {
  if (type >= 0 && type < 22) {
    return token_type_strings[type];
  }
  return "UNKNOWN";
}
