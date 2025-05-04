#ifndef DB_CLUSTER_H
#define DB_CLUSTER_H

#include <stdbool.h>
#include "executor.h" 

#define MAX_DBS_PER_CLUSTER 256
#define MAX_CLUSTERS 8
#define MAX_CLUSTER_NAME 64

typedef struct {
  char name[MAX_CLUSTER_NAME];
  Database* databases[MAX_DBS_PER_CLUSTER];
  char db_paths[MAX_DBS_PER_CLUSTER][256];
  int db_count;
  int active_db;  
  bool initialized;
} DbCluster;

typedef struct {
  DbCluster clusters[MAX_CLUSTERS];
  int cluster_count;
  int active_cluster;  
  bool initialized;

  char path[MAX_PATH_LENGTH];
} ClusterManager;

ClusterManager* cluster_manager_init(char* root_dir);

void cluster_manager_free(ClusterManager* manager);
bool cluster_create(ClusterManager* manager, const char* name);
bool cluster_add_db(ClusterManager* manager, int cluster_idx, const char* db_path);
bool cluster_switch(ClusterManager* manager, int cluster_idx);
bool cluster_switch_db(ClusterManager* manager, int db_idx);
Database* cluster_get_active_db(ClusterManager* manager);
Result cluster_execute_all(ClusterManager* manager, const char* cmd);
Result cluster_list(ClusterManager* manager);
bool process_cluster_cmd(ClusterManager* manager, Database** current_db, const char* input);
bool is_cluster_cmd(const char* input);

#endif /* CLUSTER_H */