#ifndef FS_H
#define FS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
  
#include "../utils/log.h"

#if defined(_WIN32) || defined(_WIN64)
  #include <direct.h> 
  #define PATH_SEPARATOR "\\"
#else
  #include <sys/stat.h>
  #define PATH_SEPARATOR "/"
#endif

#ifndef LOGS_VERBOSE
  #define LOGS_VERBOSE 0
#endif

#define MAX_PATH_LENGTH 512

#ifndef DB_ROOT_DIRECTORY
  #ifdef _WIN32
    #ifndef DB_ROOT_DIRECTORY
      #define DB_ROOT_DIRECTORY "C:\\db_root_directory"
    #endif
  #elif defined(__linux__)
    #ifndef DB_ROOT_DIRECTORY
      #include <stdlib.h>  
      #define DB_ROOT_DIRECTORY_PATH getenv("HOME") ? getenv("HOME") : "/home/user" 
      #define DB_ROOT_DIRECTORY DB_ROOT_DIRECTORY_PATH "/.jugad"  
    #endif
  #else
    #error "Unsupported operating system"
  #endif
#endif

static int create_directory(const char* path) {
  #if defined(_WIN32) || defined(_WIN64)
    struct _stat st = {0};
    if (_stat(path, &st) == -1) {
      return _mkdir(path);  
    }
  #else
    struct stat st = {0};
    if (stat(path, &st) == -1) {
      return mkdir(path, 0700); 
    }
  #endif
  return 0;
}

static void create_file(const char* file_path) {
  FILE *file = fopen(file_path, "r");
  if (file) {
    fclose(file);
  } else {
    file = fopen(file_path, "w");
    if (file) {
      fclose(file); 
    } else {
      LOG_ERROR("Failed to create file \n\t > %s\n", file_path);
    }
  }
}

// Directory structure for storing database files:
// /db_root_directory
//    ├── /tables/ <table_name> -> contains data_file, index files, metadata, transaction logs
//    ├── /logs/ -> contains global_transaction_log for durability
//    ├── /backups/ -> contains database/table backups
//    └── /config/ -> contains global configuration files (db_config.json, logging_config.json)

typedef struct {
  char* root_dir;
  char* tables_dir;
  char* logs_dir;
  char* backups_dir;
  char* config_dir;
  char* global_transaction_log;
  char* db_config_file;
  char* schema_file;
  char* logging_config_file;
} FS;

FS* fs_init(const char* root_directory);
void fs_free(FS* fs);

#endif // FS_H