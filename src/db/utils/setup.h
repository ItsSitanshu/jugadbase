#ifndef SETUP_H
#define SETUP_H

#include <stdbool.h>
#include "storage/cluster.h"

typedef enum OutputMode {
  OUTPUT_CONSOLE,
  OUTPUT_FILE,
  OUTPUT_MEMORY
} OutputMode;

typedef struct {
  char* output_filename;
  char* location;
  char* default_db;
  char* cluster_name;
  bool create_default;
  int verbosity_level;
  OutputMode output_mode;
  bool print_to_console;
  char* memory_buffer;
  size_t buffer_size;
} SetupConfig;

typedef struct SetupResult {
  ClusterManager* cluster_manager;
  Database* db;
  bool success;
  char* error_message;
  SetupConfig config;
} SetupResult;

SetupConfig parse_arguments(int argc, char* argv[]);
SetupResult init_cluster_manager(const SetupConfig* config);
SetupResult setup_default_cluster(ClusterManager* cluster_manager, const SetupConfig* config);
void setup_database_core(ClusterManager* cluster_manager, Database** db);
void check_database_availability(const ClusterManager* cluster_manager, const Database* db);
SetupResult perform_setup(int argc, char* argv[]);
void cleanup_setup(SetupResult* setup);
void output_result(const SetupConfig* config, const char* data);

#endif