#include "fs.h"

#include "../utils/log.h"

int directory_exists(const char* dir_path) {
  struct stat st = {0};
  return (stat(dir_path, &st) == 0 && S_ISDIR(st.st_mode));
}

void log_directory_status(const char* dir_path, const char* dir_name, int* any_directories_created) {
  if (!directory_exists(dir_path)) {
    create_directory(dir_path);
    LOG_INFO("Created %s\n", dir_name);
  }
  
  *any_directories_created += 1; 
}

void log_file_status(const char* file_path, const char* file_name) {
  FILE *file = fopen(file_path, "r");
  if (file) {
    fclose(file);
    LOG_INFO("Required file %s exists.\n", file_name);
  } else {
    LOG_INFO("Created %s\n", file_name);
    create_file(file_path);
  }
}

FS* fs_init(const char* root_directory) {
  FS* fs = malloc(sizeof(FS));

  fs->root_dir = strdup(root_directory);
  fs->tables_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->tables_dir, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "tables", root_directory);

  fs->logs_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->logs_dir, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "logs", root_directory);

  fs->backups_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->backups_dir, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "backups", root_directory);

  fs->config_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->config_dir, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "config", root_directory);

  fs->global_transaction_log = malloc(MAX_PATH_LENGTH);
  snprintf(fs->global_transaction_log, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "logs" PATH_SEPARATOR "global_transaction_log", root_directory);

  fs->db_config_file = malloc(MAX_PATH_LENGTH);
  snprintf(fs->db_config_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "config" PATH_SEPARATOR "db_config.json", root_directory);

  fs->logging_config_file = malloc(MAX_PATH_LENGTH);
  snprintf(fs->logging_config_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "config" PATH_SEPARATOR "logging_config.json", root_directory);

  int any_directory_created = 0;
  
  log_directory_status(fs->tables_dir, "tables", &any_directory_created);
  log_directory_status(fs->logs_dir, "logs", &any_directory_created);
  log_directory_status(fs->backups_dir, "backups", &any_directory_created);
  log_directory_status(fs->config_dir, "config", &any_directory_created);

  char schema_dir[MAX_PATH_LENGTH];
  snprintf(schema_dir, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "schema", fs->tables_dir);
  
  log_file_status(fs->global_transaction_log, "global_transaction_log");
  log_file_status(fs->global_transaction_log, "global_transaction_log");
  log_file_status(fs->db_config_file, "db_config.json");
  log_file_status(fs->logging_config_file, "logging_config.json");

  return fs;
}

void fs_free(FS* fs) {
  if (fs) {
    free(fs->root_dir);
    free(fs->tables_dir);
    free(fs->logs_dir);
    free(fs->backups_dir);
    free(fs->config_dir);
    free(fs->global_transaction_log);
    free(fs->db_config_file);
    free(fs->logging_config_file);
    free(fs);
  }
}