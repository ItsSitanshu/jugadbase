#ifndef DATABASE_H
#define DATABASE_H

#include "storage/fs.h"
#include "utils/io.h"
#include "internal/btree.h"
#include "parser/parser.h"
#include "storage.h"
#include "internal/toast.h"
#include "wal.h"

#define MAX_COMMANDS 1024
#define MAX_TABLES 256 
#define DB_INIT_MAGIC 0x4A554741  // "JUGA" 

typedef struct Database Database;
typedef struct ClusterManager ClusterManager;

typedef struct Database {
  Lexer* lexer;
  Parser* parser;
  char* uuid;

  TableCatalogEntry tc[MAX_TABLES];
  BufferPool lake[MAX_TABLES];
  
  size_t table_count;
  uint8_t loaded_btree_clusters;
  uint8_t btree_idx_stack[BTREE_LIFETIME_THRESHOLD];

  FILE* tc_reader;
  FILE* tc_writer;
  FILE* tc_appender;
  FILE* wal;

  FS* fs;
  Database* core;
} Database;

Database* db_init(char* dir, Database* core);
void db_free(Database* db);

bool process_cmd(ClusterManager* cm, Database* db, char* input);
void list_tables(Database* db);
void process_file(Database* db, char* filename, bool show);

void load_tc(Database* db);
void load_table_schema(Database* db);
void load_btree_cluster(Database* db, char* table_name);
void pop_btree_cluster(Database* db);

bool load_schema_tc(Database* db, char* table_name);
TableSchema* find_table_schema_tc(Database* db, const char* filename);
bool load_initial_schema(Database* db);

void load_lake(Database* db);
void flush_lake(Database* db);

#endif // DATABASE_H