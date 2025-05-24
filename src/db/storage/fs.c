#include "storage/fs.h"
#include "storage/database.h"

#include "../utils/log.h"

void log_directory_status(char* dir_path, const char* dir_name, int* any_directories_created) {
  if (!directory_exists(dir_path)) {
    create_directory(dir_path);
    LOG_INFO("Created %s/", dir_name);
  }
  
  *any_directories_created += 1; 
}

void log_schema_file_status(char* path) {
  FILE *file = fopen(path, "rb");

  long size = 0;

  if (file) {
    fseek(file, 0, SEEK_END);
    size = ftell(file);
    fclose(file);
  }

  if (size == 0) {
    LOG_INFO("Schema file is empty, initializing...");

    file = fopen(path, "w");
    if (!file) {
      LOG_FATAL("Failed to open schema file for writing");
    }

    uint32_t magic = DB_INIT_MAGIC;
    uint32_t zero = 0;
    uint32_t placeholders[MAX_TABLES] = {0};
    
    fwrite(&magic, sizeof(uint32_t), 1, file);
    fwrite(&zero, sizeof(uint32_t), 1, file);
    fwrite(placeholders, sizeof(uint32_t), MAX_TABLES, file);

    fclose(file);

    LOG_INFO("Initialized schema file with metadata");
  } else {
    LOG_DEBUG("Schema file is not empty");
  }
}

void log_file_status(char* file_path, const char* file_name) {
  FILE *file = fopen(file_path, "r");
  if (file) {
    fclose(file);
    LOG_INFO("Required file %s exists.", file_name);
  } else {
    create_file(file_path);
    LOG_INFO("Created %s", file_name);
  }
}

FS* fs_init(char* root_directory) {
  FS* fs = calloc(1, sizeof(FS));

  fs->root_dir = strdup(root_directory);
  fs->tables_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->tables_dir, MAX_PATH_LENGTH, "%s" SEP "tables", root_directory);

  fs->logs_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->logs_dir, MAX_PATH_LENGTH, "%s" SEP "logs", root_directory);

  fs->backups_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->backups_dir, MAX_PATH_LENGTH, "%s" SEP "backups", root_directory);

  fs->config_dir = malloc(MAX_PATH_LENGTH);
  snprintf(fs->config_dir, MAX_PATH_LENGTH, "%s" SEP "config", root_directory);

  fs->global_transaction_log = malloc(MAX_PATH_LENGTH);
  snprintf(fs->global_transaction_log, MAX_PATH_LENGTH, "%s" SEP "logs" SEP "global_transaction_log", root_directory);

  fs->db_config_file = malloc(MAX_PATH_LENGTH);
  snprintf(fs->db_config_file, MAX_PATH_LENGTH, "%s" SEP "config" SEP "db_config.json", root_directory);

  fs->logging_config_file = malloc(MAX_PATH_LENGTH);
  snprintf(fs->logging_config_file, MAX_PATH_LENGTH, "%s" SEP "config" SEP "logging_config.json", root_directory);

  fs->wal_file = malloc(MAX_PATH_LENGTH);
  snprintf(fs->wal_file, MAX_PATH_LENGTH, "%s" SEP "db.wal", root_directory);

  int any_directory_created = 0;
  
  log_directory_status(fs->root_dir, "<root>", &any_directory_created);
  log_directory_status(fs->tables_dir, "tables", &any_directory_created);
  log_directory_status(fs->logs_dir, "logs", &any_directory_created);
  log_directory_status(fs->backups_dir, "backups", &any_directory_created);
  log_directory_status(fs->config_dir, "config", &any_directory_created);

  char schema_dir[MAX_PATH_LENGTH];
  fs->schema_file = malloc(MAX_PATH_LENGTH);
  snprintf(fs->schema_file, MAX_PATH_LENGTH, "%s" SEP "schema", fs->tables_dir);
  
  log_file_status(fs->global_transaction_log, "global_transaction_log");
  log_file_status(fs->global_transaction_log, "global_transaction_log");
  log_file_status(fs->db_config_file, "db_config.json");
  log_file_status(fs->logging_config_file, "logging_config.json");
  log_file_status(fs->wal_file, "db.wal");

  log_schema_file_status(fs->schema_file);

  return fs;
}

void fs_free(FS* fs) {
  if (!fs) return;

  free(fs->root_dir);
  free(fs->tables_dir);
  free(fs->logs_dir);
  free(fs->backups_dir);
  free(fs->config_dir);
  free(fs->global_transaction_log);
  free(fs->db_config_file);
  free(fs->logging_config_file);
  free(fs->wal_file);
  free(fs->schema_file);

  free(fs);
}
