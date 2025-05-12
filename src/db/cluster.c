#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster.h"
#include "../utils/log.h"


ClusterManager* cluster_manager_init(char* root_dir) {
  struct stat st = {0};
  char config_path[512] = {0};
  FILE* config_file = NULL;
  

  if (create_directory(root_dir) != 0) {  
    LOG_ERROR("Failed to create ROOT directory for clusters");
    return NULL;
  }
  
  ClusterManager* manager = (ClusterManager*)malloc(sizeof(ClusterManager));
  if (!manager) {
    LOG_ERROR("Failed to allocate memory for cluster manager");
    return NULL;
  }
  
  memset(manager, 0, sizeof(ClusterManager));
  manager->cluster_count = 0;
  manager->active_cluster = -1;
  manager->initialized = true;
  sprintf(manager->path, "%s", root_dir);
  
  snprintf(config_path, sizeof(config_path), "%s/%s", root_dir, CONFIG_FILE);
  
  if (access(config_path, F_OK) != -1) {
    config_file = fopen(config_path, "rb");
    if (config_file) {
      if (fread(manager, sizeof(ClusterManager), 1, config_file) != 1) {
        LOG_ERROR("Failed to read cluster configuration");
        free(manager);
        fclose(config_file);
        return NULL;
      }
      
      for (int i = 0; i < manager->cluster_count; i++) {
        DbCluster* cluster = &manager->clusters[i];
        for (int j = 0; j < cluster->db_count; j++) {
          cluster->databases[j] = db_init(cluster->db_paths[j]);
          if (!cluster->databases[j]) {
            LOG_ERROR("Failed to initialize database at '%s'", cluster->db_paths[j]);
          }
        }
      }
      
      fclose(config_file);
      LOG_INFO("Loaded cluster configuration from %s", config_path);
    } else {
      LOG_ERROR("Failed to open cluster configuration file");
    }

    cluster_switch(manager, 0);
    cluster_switch_db(manager, 1);

    LOG_DEBUG("WORKS!");
  } else {
    LOG_INFO("No cluster configuration found, initialized empty manager");
  }
  
  return manager;
}

bool cluster_manager_save(ClusterManager* manager) {
  if (!manager) return false;
  
  char config_path[512] = {0};
  snprintf(config_path, sizeof(config_path), "%s/%s", manager->path, CONFIG_FILE);
  
  ClusterManager temp_manager;
  memcpy(&temp_manager, manager, sizeof(ClusterManager));
  
  for (int i = 0; i < temp_manager.cluster_count; i++) {
    DbCluster* cluster = &temp_manager.clusters[i];
    for (int j = 0; j < cluster->db_count; j++) {
      cluster->databases[j] = NULL;
    }
  }
  
  FILE* config_file = fopen(config_path, "wb");
  if (!config_file) {
    LOG_ERROR("Failed to open cluster configuration file for writing");
    return false;
  }
  
  if (fwrite(&temp_manager, sizeof(ClusterManager), 1, config_file) != 1) {
    LOG_ERROR("Failed to write cluster configuration");
    fclose(config_file);
    return false;
  }
  
  fclose(config_file);
  LOG_INFO("Saved cluster configuration to %s", config_path);
  return true;
}

void cluster_manager_free(ClusterManager* manager) {
  if (!manager) return;
  
  cluster_manager_save(manager);
  
  for (int i = 0; i < manager->cluster_count; i++) {
    DbCluster* cluster = &manager->clusters[i];
    for (int j = 0; j < cluster->db_count; j++) {
      if (cluster->databases[j]) {
        db_free(cluster->databases[j]);
        cluster->databases[j] = NULL;
      }
    }
  }
  
  free(manager);
  LOG_INFO("Cluster manager freed");
}

bool cluster_create(ClusterManager* manager, char* name) {
  if (!manager || !name) {
    LOG_ERROR("Invalid parameters for cluster creation");
    return false;
  }
  
  if (manager->cluster_count >= MAX_CLUSTERS) {
    LOG_ERROR("Maximum number of clusters reached (%d)", MAX_CLUSTERS);
    return false;
  }
  
  for (int i = 0; i < manager->cluster_count; i++) {
    if (strcmp(manager->clusters[i].name, name) == 0) {
      LOG_ERROR("Cluster with name '%s' already exists", name);
      return false;
    }
  }
  
  char cluster_path[512] = {0};
  snprintf(cluster_path, sizeof(cluster_path), "%s/%s", manager->path, name);

  if (create_directory(cluster_path) != 0) {
    LOG_ERROR("Failed to create directory for cluster '%s'", name);
    return false;
  }
  
  int idx = manager->cluster_count;
  DbCluster* cluster = &manager->clusters[idx];
  
  strncpy(cluster->name, name, MAX_CLUSTER_NAME - 1);
  cluster->name[MAX_CLUSTER_NAME - 1] = '\0';
  cluster->db_count = 0;
  cluster->active_db = -1;
  cluster->initialized = true;
  
  manager->cluster_count++;
  
  if (manager->active_cluster == -1) {
    manager->active_cluster = 0;
  }
  
  cluster_manager_save(manager);
  
  LOG_INFO("Created cluster '%s' (index: %d)", name, idx);
  Result cluster_info = cluster_list(manager);
    
  if (!cluster_add_db(manager, 0, "core")) {
    fprintf(stderr, "Failed to add jb.core database to cluster\n");
    cluster_manager_free(manager);
    return 1;
  }

  cluster_switch(manager, 0);
  Database* db = cluster_get_active_db(manager);
  process_file(db, CORE_JDL_PATH);

  return true;
}

bool cluster_add_db(ClusterManager* manager, int cluster_idx, char* db_path) {
  if (!manager || !db_path || cluster_idx < 0 || cluster_idx >= (manager->cluster_count + 1)) {
    LOG_ERROR("Invalid parameters for adding database to cluster");
    return false;
  }
  
  DbCluster* cluster = &manager->clusters[cluster_idx];
  
  if (cluster->db_count >= MAX_DBS_PER_CLUSTER) {
    LOG_ERROR("Maximum number of databases per cluster reached (%d)", MAX_DBS_PER_CLUSTER);
    return false;
  }
  
  for (int i = 0; i < cluster->db_count; i++) {
    if (strcmp(cluster->db_paths[i], db_path) == 0) {
      LOG_ERROR("Database with path '%s' already exists in cluster '%s'", db_path, cluster->name);
      return false;
    }
  }
  
  char full_path[512] = {0};
  if (db_path[0] == '/') {
    strncpy(full_path, db_path, sizeof(full_path) - 1);
  } else {
    snprintf(full_path, sizeof(full_path), "%s/%s/%s", manager->path, cluster->name, db_path);
  }
  
  Database* db = db_init(full_path);
  if (!db) {
    LOG_ERROR("Failed to initialize database at '%s'", full_path);
    return false;
  }
  
  int db_idx = cluster->db_count;
  cluster->databases[db_idx] = db;
  strncpy(cluster->db_paths[db_idx], full_path, 255);
  cluster->db_paths[db_idx][255] = '\0';
  cluster->db_count++;
  
  cluster->active_db = db_idx;
  
  cluster_manager_save(manager);
  
  LOG_INFO("Added database '%s' to cluster '%s' (index: %d)", full_path, cluster->name, db_idx);
  return true;
}

bool cluster_switch(ClusterManager* manager, int cluster_idx) {
  if (!manager || cluster_idx < 0 || cluster_idx >= manager->cluster_count) {
    LOG_ERROR("Invalid cluster index for switching: %d", cluster_idx);
    return false;
  }
  
  manager->active_cluster = cluster_idx;
  
  cluster_manager_save(manager);
  
  LOG_INFO("Switched to cluster '%s'", manager->clusters[cluster_idx].name);
  return true;
}

bool cluster_switch_db(ClusterManager* manager, int db_idx) {
  if (!manager || manager->active_cluster < 0) {
    LOG_ERROR("No active cluster for switching databases");
    return false;
  }
  
  DbCluster* cluster = &manager->clusters[manager->active_cluster];
  
  if (db_idx < 0 || db_idx >= cluster->db_count) {
    LOG_ERROR("Invalid database index for switching: %d", db_idx);
    return false;
  }
  
  cluster->active_db = db_idx;
  
  cluster_manager_save(manager);
  
  LOG_INFO("Switched to database '%s' in cluster '%s'", 
            cluster->db_paths[db_idx], cluster->name);
  return true;
}

Database* cluster_get_active_db(ClusterManager* manager) {
  if (!manager || manager->active_cluster < 0) {
    return NULL;
  }
  
  DbCluster* cluster = &manager->clusters[manager->active_cluster];
  if (cluster->active_db < 0 || cluster->active_db >= cluster->db_count) {
    return NULL;
  }
  
  return cluster->databases[cluster->active_db];
}

Result cluster_execute_all(ClusterManager* manager, char* cmd) {
  ExecutionResult result = {0};
  
  if (!manager || manager->active_cluster < 0 || !cmd) {
    result.code = -1;
    return (Result){result, NULL};
  }
  
  DbCluster* cluster = &manager->clusters[manager->active_cluster];
  
  if (cluster->db_count == 0) {
    result.code = -1;
    return (Result){result, NULL};
  }
  
  int original_active_db = cluster->active_db;
  
  for (int i = 0; i < cluster->db_count; i++) {
    cluster->active_db = i;
    
    Result db_result = process(cluster->databases[i], cmd);
  }
  
  cluster->active_db = original_active_db;
  
  return (Result){result, NULL};
}

Result cluster_list(ClusterManager* manager) {
  ExecutionResult result = {0};
  
  if (!manager) {
    result.code = -1;
    return (Result){result, NULL};
  }
  
  if (manager->cluster_count == 0) {
    result.code = -1;
    return (Result){result, NULL};
  }
  
  char buffer[4096] = {0};
  int offset = 0;
  
  offset += snprintf(buffer + offset, sizeof(buffer) - offset, 
                    "Clusters (%d):\n", manager->cluster_count);
  
  for (int i = 0; i < manager->cluster_count; i++) {
    DbCluster* cluster = &manager->clusters[i];
    
    char active_marker = (i == manager->active_cluster) ? '*' : ' ';
    
    offset += snprintf(buffer + offset, sizeof(buffer) - offset, 
                      "%c [%d] %s (%d databases)\n", 
                      active_marker, i, cluster->name, cluster->db_count);
    
    for (int j = 0; j < cluster->db_count; j++) {
      char db_active_marker = (i == manager->active_cluster && j == cluster->active_db) ? '*' : ' ';
      
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, 
                        "  %c [%d] %s\n", 
                        db_active_marker, j, cluster->db_paths[j]);
    }
  }
  
  result.code = 0;
  result.message = strdup(buffer);

  return (Result){result, NULL};
}

static bool parse_cluster_cmd(char* input, char* cmd, char* arg1, char* arg2) {
  if (!input || !cmd || !arg1 || !arg2) return false;
  
  char* ptr = input;
  while (*ptr && isspace(*ptr)) ptr++;
  
  if (strncmp(ptr, "~ ", 2) == 0) {
    ptr += 2;
  } else {
    return false;
  }
  
  while (*ptr && isspace(*ptr)) ptr++;
  
  int i = 0;
  while (*ptr && !isspace(*ptr) && i < 31) {
      cmd[i++] = *ptr++;
  }
  cmd[i] = '\0';
  
  while (*ptr && isspace(*ptr)) ptr++;
  
  i = 0;
  while (*ptr && !isspace(*ptr) && i < 255) {
    arg1[i++] = *ptr++;
  }
  arg1[i] = '\0';
  
  while (*ptr && isspace(*ptr)) ptr++;
  
  i = 0;
  while (*ptr && i < 255) {
    arg2[i++] = *ptr++;
  }
  arg2[i] = '\0';
  
  return true;
}

bool process_cluster_cmd(ClusterManager* manager, Database** current_db, char* input) {
  if (!manager || !input || !current_db) return false;
  
  char cmd[32] = {0};
  char arg1[256] = {0};
  char arg2[256] = {0};
  
  if (!parse_cluster_cmd(input, cmd, arg1, arg2)) {
    LOG_ERROR("Failed to parse cluster command: %s", input);
    return false;
  }
  
  if (strcmp(cmd, "create") == 0 || strcmp(cmd, "new") == 0) {
    if (arg1[0] == '\0') {
      LOG_ERROR("Cluster name required");
      return true; 
    }
    
    if (cluster_create(manager, arg1)) {
      printf("Created cluster '%s'\n", arg1);
    } else {
      printf("Failed to create cluster '%s'\n", arg1);
    }
    return true;
  } else if (strcmp(cmd, "add") == 0) {
    if (arg1[0] == '\0' || arg2[0] == '\0') {
      LOG_ERROR("Cluster index and database path required");
      return true;
    }
    
    int cluster_idx = atoi(arg1);
    if (cluster_add_db(manager, cluster_idx, arg2)) {
      printf("Added database '%s' to cluster %d\n", arg2, cluster_idx);
    } else {
      printf("Failed to add database '%s' to cluster %d\n", arg2, cluster_idx);
    }
    return true;
  } else if (strcmp(cmd, "switch") == 0 || strcmp(cmd, "use") == 0) {
    if (arg1[0] == '\0') {
      LOG_ERROR("Cluster index required");
      return true;
    }
    
    int cluster_idx = atoi(arg1);
    if (cluster_switch(manager, cluster_idx)) {
      printf("Switched to cluster %d\n", cluster_idx);
      
      if (arg2[0] != '\0') {
        int db_idx = atoi(arg2);
        if (cluster_switch_db(manager, db_idx)) {
          printf("Switched to database %d\n", db_idx);
        } else {
          printf("Failed to switch to database %d\n", db_idx);
        }
      }
      
      *current_db = cluster_get_active_db(manager);
    } else {
      printf("Failed to switch to cluster %d\n", cluster_idx);
    }
    return true;
  }
  else if (strcmp(cmd, "list") == 0 || strcmp(cmd, "ls") == 0) {
    Result result = cluster_list(manager);
    if (result.exec.code == 0) {
      printf("%s", result.exec.message);
    } else {
      printf("Failed to list clusters: %s\n", result.exec.message);
    }
    return true;
  }
  else if (strcmp(cmd, "exec") == 0 || strcmp(cmd, "all") == 0) {
    if (arg1[0] == '\0') {
      LOG_ERROR("Command to execute required");
      return true;
    }
    
    char full_cmd[512] = {0};
    if (arg2[0] != '\0') {
      snprintf(full_cmd, sizeof(full_cmd), "%s %s", arg1, arg2);
    } else {
      strncpy(full_cmd, arg1, sizeof(full_cmd) - 1);
    }
    
    Result result = cluster_execute_all(manager, full_cmd);
    if (result.exec.code == 0) {
      printf("Command executed on all databases in active cluster\n");
      printf("%s", result.exec.message);
    } else {
      printf("Failed to execute command on all databases: %s\n", result.exec.message);
    }
    return true;
  }
  
  LOG_ERROR("Unknown cluster command: %s", cmd);
  return true;  
}

bool is_cluster_cmd(char* input) {
  if (!input) return false;
  
  while (*input && isspace(*input)) input++;
  
  return (strncmp(input, "~ ", 2) == 0);
}