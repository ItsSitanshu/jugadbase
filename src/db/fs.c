#include "fs.h"

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

  create_directory(fs->tables_dir);
  create_directory(fs->logs_dir);
  create_directory(fs->backups_dir);
  create_directory(fs->config_dir);

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