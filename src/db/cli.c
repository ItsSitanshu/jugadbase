#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils/xmem.h"
#include "utils/setup.h"
#include "utils/cli.h"
#include "utils/jugadline.h"

#include "kernel/kernel.h"

int main(int argc, char* argv[]) {
  SetupResult setup = perform_setup(argc, argv);
  
  if (!setup.success) {
    fprintf(stderr, "%s\n", setup.error_message);
    return 1;
  }
  
  ClusterManager* cluster_manager = setup.cluster_manager;
  Database* db = setup.db;
  SetupConfig* config = &setup.config;
  
  char* input = NULL;
  CommandHistory history = { .current = 1, .size = 0 };
  char prompt[512];
  
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

    char* current_role = active_cluster->databases[active_cluster->active_db]->current_role ? 
      active_cluster->databases[active_cluster->active_db]->current_role->name : "unverified monkey";

    snprintf(prompt, sizeof(prompt), 
             COLOR_RED "[%s @ %s] " COLOR_MAGENTA "<jdb: %s>" COLOR_RESET " $ ", 
             db_name,
             current_role,
             active_cluster ? active_cluster->name : "no-cluster");
    
    input = jugadline(&history, prompt);

    if (!input || strlen(input) == 0) {
      xfree(input);
      continue;
    }
    
    if (!(process_cluster_cmd(cluster_manager, &db, input))) {
      if (process_cmd_no_db(cluster_manager, input)) {
      } else if (db) {
        if (!process_cmd_with_db(db, input)) {
          Result result = process(db, input);

          if (result.exec.code == 0) {
            char output_buffer[8192];
            output_result(config, output_buffer);

            if (config->output_mode == OUTPUT_FILE && config->output_filename) {
              print_text_table_to_file(result.exec, result.cmd, config->output_filename);
            }
          }
          free_result(&result);
        }
      } else {
        const char* msg = "Lost? Use help command or read documentation at https://itssitanshu.github.io/jugadbase/\n";
        output_result(config, msg);
      }
    }

    xfree(input);
  }

  if (config->output_mode == OUTPUT_MEMORY && config->memory_buffer) {
    printf("\n--- Memory Buffer Contents ---\n%s\n", config->memory_buffer);
  }

  cleanup_setup(&setup);
  
  for (int i = 0; i < history.size; i++) {
    xfree(history.history[i]);
  }
  
  return 0;
}

__attribute__((constructor))
static void xmem_init_report(void) {
  atexit(xmem_report);
}