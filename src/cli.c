#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "utils/cli.h"
#include "utils/log.h"
#include "utils/jugadline.h"

#include "executor.h"
#include "cluster.h"

#define ROOT_DIR "./data/clusters"

int main(int argc, char* argv[]) {
  const char* output_filename = NULL;
  const char* default_db = "default";
  const char* cluster_name = "default";
  bool create_default = true;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--verbose") == 0 && i + 1 < argc) {
      *verbosity_level = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
      output_filename = argv[++i];
    } else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
      default_db = argv[++i];
    } else if (strcmp(argv[i], "--cluster") == 0 && i + 1 < argc) {
      cluster_name = argv[++i];
    } else if (strcmp(argv[i], "--no-default") == 0) {
      create_default = false;
    }
  }

  if (*verbosity_level == 1) {
    LOG_WARN("Invalid verbosity level: %d. Defaulting to WARN.", *verbosity_level);
  }

  ClusterManager* cluster_manager = cluster_manager_init(DB_ROOT_DIRECTORY);
  if (!cluster_manager) {
    fprintf(stderr, "Failed to initialize cluster manager\n");
    return 1;
  }

  Database* db = NULL;
  
  if (cluster_manager->cluster_count == 0 && create_default) {
    if (!cluster_create(cluster_manager, cluster_name)) {
      fprintf(stderr, "Failed to create default cluster\n");
      cluster_manager_free(cluster_manager);
      return 1;
    }

    if (!cluster_add_db(cluster_manager, 0, default_db)) {
      fprintf(stderr, "Failed to add default database to cluster\n");
      cluster_manager_free(cluster_manager);
      return 1;
    }

    db = cluster_get_active_db(cluster_manager);
    if (!db && cluster_manager->cluster_count > 0) {
      cluster_switch(cluster_manager, 1);
      if (cluster_manager->clusters[1].db_count > 0) {
        cluster_switch_db(cluster_manager, 1);
        db = cluster_get_active_db(cluster_manager);
      }
    }
  }
  
  if (!db) {
    fprintf(stderr, "No active database available. Use cluster commands to create or switch to a cluster and database.\n");
    LOG_WARN("Starting with no active database");
  }

  char* input = NULL;
  CommandHistory history = { .current = 1, .size = 0 };

  char short_cwd[256];
  char prompt[512];
  get_short_cwd(short_cwd, sizeof(short_cwd));

  while (1) {
    const char* db_path = "none";
    DbCluster* active_cluster = NULL;
    
    if (cluster_manager->active_cluster >= 0 && 
        cluster_manager->active_cluster < cluster_manager->cluster_count) {
      active_cluster = &cluster_manager->clusters[cluster_manager->active_cluster];
      
      if (active_cluster->active_db >= 0 && 
          active_cluster->active_db < active_cluster->db_count) {
        db_path = active_cluster->db_paths[active_cluster->active_db];
      }
    }
    
    const char* db_name = strrchr(db_path, '/');
    if (db_name) {
      db_name++;  
    } else {
      db_name = db_path;
    }

    snprintf(prompt, sizeof(prompt), 
             COLOR_RED "/%s<%s> " COLOR_MAGENTA "[%s~jugad-cli]" COLOR_RESET "> ", 
             short_cwd, 
             active_cluster ? active_cluster->name : "no-cluster",
             db_name);
    
    input = jugadline(&history, prompt);

    if (!input || strlen(input) == 0) {
      free(input);
      continue;
    }
    
    if (strcmp(input, "exit") == 0 || strcmp(input, "quit") == 0) {
      free(input);
      break;
    }
    
    if (is_cluster_cmd(input)) {
      process_cluster_cmd(cluster_manager, &db, input);
    } else if (db) {
      if (!process_dot_cmd(db, input)) {
        Result result = process(db, input);
        if (output_filename && result.exec.code == 0) {
          print_text_table_to_file(result.exec, result.cmd, output_filename);
        }
        if (result.exec.message) {
          free(result.exec.message);
        }
      }
    } else {
      printf("No active database. Use .cluster commands to create or select a database.\n");
    }

    free(input);
  }

  cluster_manager_save(cluster_manager);
  cluster_manager_free(cluster_manager);
  
  for (int i = 0; i < history.size; i++) {
    free(history.history[i]);
  }
  
  return 0;
}