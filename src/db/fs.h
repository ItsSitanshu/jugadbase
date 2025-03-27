#ifndef FS_H
#define FS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32) || defined(_WIN64)
  #include <direct.h> 
  #define PATH_SEPARATOR "\\"
#else
  #include <sys/stat.h>
  #define PATH_SEPARATOR "/"
#endif

#define MAX_PATH_LENGTH 512

#ifndef DB_ROOT_DIRECTORY
  #define DB_ROOT_DIRECTORY "C:\\db_root_directory"  
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
  char* logging_config_file;
} FS;

FS* fs_init(const char* root_directory);
void fs_free(FS* fs);

#endif // FS_H