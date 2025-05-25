#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "setup.h"
#include "utils/log.h"

SetupConfig parse_arguments(int argc, char* argv[]) {
  SetupConfig config = {
    .output_filename = NULL,
    .location = DB_ROOT_DIRECTORY,
    .default_db = "public",
    .cluster_name = "default", 
    .create_default = true,
    .verbosity_level = 0,
    .output_mode = OUTPUT_CONSOLE,
    .print_to_console = true,
    .memory_buffer = NULL,
    .buffer_size = 0
  };
  
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--verbose") == 0 && i + 1 < argc) {
      config.verbosity_level = atoi(argv[++i]); 
    } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
      config.output_filename = argv[++i];
      config.output_mode = OUTPUT_FILE;
    } else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
      config.default_db = argv[++i];
    } else if (strcmp(argv[i], "--cluster") == 0 && i + 1 < argc) {
      config.cluster_name = argv[++i];
    } else if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) {
      config.location = argv[++i];
    } else if (strcmp(argv[i], "--no-default") == 0) {
      config.create_default = false;
    } else if (strcmp(argv[i], "--no-console") == 0) {
      config.print_to_console = false;
    } else if (strcmp(argv[i], "--memory") == 0) {
      config.output_mode = OUTPUT_MEMORY;
      if (i + 1 < argc && argv[i + 1][0] != '-') {
        config.buffer_size = atoi(argv[++i]);
      } else {
        config.buffer_size = 4096;
      }
    } else if (strcmp(argv[i], "--output-mode") == 0 && i + 1 < argc) {
      char* mode = argv[++i];
      if (strcmp(mode, "console") == 0) {
        config.output_mode = OUTPUT_CONSOLE;
      } else if (strcmp(mode, "file") == 0) {
        config.output_mode = OUTPUT_FILE;
      } else if (strcmp(mode, "memory") == 0) {
        config.output_mode = OUTPUT_MEMORY;
      }
    }
  }
  
  if (config.output_mode == OUTPUT_MEMORY && config.buffer_size > 0) {
    config.memory_buffer = calloc(config.buffer_size, sizeof(char));
  }
  
  return config;
}

SetupResult init_cluster_manager(const SetupConfig* config) {
  SetupResult result = {0};
  result.config = *config;
  
  if (config->verbosity_level == 1) {
    LOG_WARN("Invalid verbosity level: %d. Defaulting to WARN.", config->verbosity_level);
  }
  
  result.cluster_manager = cluster_manager_init(result.config.location);
  if (!result.cluster_manager) {
    result.success = false;
    result.error_message = "Failed to initialize cluster manager";
    return result;
  }
  
  result.success = true;
  return result;
}

SetupResult setup_default_cluster(ClusterManager* cluster_manager, const SetupConfig* config) {
  SetupResult result = {
    .cluster_manager = cluster_manager,
    .success = true,
    .db = NULL,
    .config = *config
  };
  
  if (cluster_manager->active_cluster == -1 && config->create_default) {
    
    if (!cluster_create(cluster_manager, config->cluster_name)) {
      result.success = false;
      result.error_message = "Failed to create default cluster";
      return result;
    }
    
    if (!cluster_add_db(cluster_manager, 0, config->default_db)) {
      result.success = false;
      result.error_message = "Failed to add default database to cluster";
      return result;
    }
    
    result.db = cluster_get_active_db(cluster_manager);
    
    if (!result.db && cluster_manager->cluster_count > 0) {
      cluster_switch(cluster_manager, 1);
      if (cluster_manager->clusters[1].db_count > 0) {
        cluster_switch_db(cluster_manager, 1);
        result.db = cluster_get_active_db(cluster_manager);
      }
    }
  }
  
  return result;
}

void setup_database_core(ClusterManager* cluster_manager, Database** db) {
  if (cluster_manager->active_cluster >= 0 && *db) {
    DbCluster cluster = cluster_manager->clusters[cluster_manager->active_cluster];
    *db = cluster.databases[cluster.active_db];
    (*db)->core = cluster.databases[0];
    (*db)->core->core = (*db)->core;
  }
}

void check_database_availability(const ClusterManager* cluster_manager, const Database* db) {
  if (!db && cluster_manager->active_cluster == -1) {
    LOG_WARN("No active database available. Use cluster commands to create or switch to a cluster and database.");
  }
}

SetupResult perform_setup(int argc, char* argv[]) {
  SetupResult final_result = {0};
  
  SetupConfig config = parse_arguments(argc, argv);
  
  SetupResult init_result = init_cluster_manager(&config);
  if (!init_result.success) {
    return init_result;
  }

  LOG_WARN("CONFIG %s", config.location);
  
  SetupResult cluster_result = setup_default_cluster(init_result.cluster_manager, &config);
  if (!cluster_result.success) {
    cluster_manager_free(init_result.cluster_manager);
    return cluster_result;
  }
  
  setup_database_core(cluster_result.cluster_manager, &cluster_result.db);
  
  check_database_availability(cluster_result.cluster_manager, cluster_result.db);
  
  final_result.cluster_manager = cluster_result.cluster_manager;
  final_result.db = cluster_result.db;
  final_result.success = true;
  final_result.config = config;

  
  return final_result;
}

void cleanup_setup(SetupResult* setup) {
  if (setup->cluster_manager) {
    cluster_manager_free(setup->cluster_manager);
    setup->cluster_manager = NULL;
  }
  
  if (setup->config.memory_buffer) {
    free(setup->config.memory_buffer);
    setup->config.memory_buffer = NULL;
  }
}

void output_result(const SetupConfig* config, const char* data) {
  if (!data) return;
  
  switch (config->output_mode) {
    case OUTPUT_CONSOLE:
      if (config->print_to_console) {
        printf("%s", data);
      }
      break;
      
    case OUTPUT_FILE:
      if (config->output_filename) {
        FILE* file = fopen(config->output_filename, "a");
        if (file) {
          fprintf(file, "%s", data);
          fclose(file);
        }
      }
      if (config->print_to_console) {
        printf("%s", data);
      }
      break;
      
    case OUTPUT_MEMORY:
      if (config->memory_buffer && config->buffer_size > 0) {
        size_t current_len = strlen(config->memory_buffer);
        size_t data_len = strlen(data);
        if (current_len + data_len < config->buffer_size - 1) {
          strcat(config->memory_buffer, data);
        }
      }
      if (config->print_to_console) {
        printf("%s", data);
      }
      break;
  }
}