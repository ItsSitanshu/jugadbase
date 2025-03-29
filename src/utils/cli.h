#ifndef CLI_H
#define CLI_H


#define GREEN   "\033[1;32m"
#define CYAN    "\033[1;36m"
#define RESET   "\033[0m"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "../utils/log.h"

void get_short_cwd(char* buffer, size_t size) {
  char cwd[1024];
  if (getcwd(cwd, sizeof(cwd)) != NULL) {
    char* last_slash = strrchr(cwd, '/');
    if (last_slash && last_slash != cwd) {
      *last_slash = '\0';
      char* second_last_slash = strrchr(cwd, '/');
      snprintf(buffer, size, "%s/%s", 
        second_last_slash ? second_last_slash + 1 : cwd, 
        last_slash + 1);
    } else {
      snprintf(buffer, size, "%s", last_slash ? last_slash + 1 : cwd);
    }
  } else {
    snprintf(buffer, size, "unknown");
  }
}


#endif // CLI_H 