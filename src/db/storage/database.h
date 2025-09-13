#ifndef DATABASE_H
#define DATABASE_H

#include "parser/parser.h"

#include "internal/btree.h"
#include "internal/toast.h"
#include "internal/roles.h"


#include "storage/fs.h"
#include "storage/storage.h"
#include "storage/wal.h"

#include "utils/io.h"

#define MAX_COMMANDS 1024
#define MAX_TABLES 256 
#define DB_INIT_MAGIC 0x4A554741  // "JUGA" 

typedef struct Database Database;
typedef struct ClusterManager ClusterManager;
typedef struct SysCache SysCache; 

typedef struct Database {
  Lexer* lexer;
  Parser* parser;
  char* uuid;

  TableCatalogEntry tc[MAX_TABLES];
  BufferPool lake[MAX_TABLES];
  
  size_t table_count;
  bool is_core;
  
  uint8_t loaded_btree_clusters;
  uint8_t btree_idx_stack[BTREE_LIFETIME_THRESHOLD];

  FILE* tc_reader;
  FILE* tc_writer;
  FILE* tc_appender;
  FILE* wal;

  SysCache* constr_cache;

  FS* fs;
  Database* core;

  Role* current_role;
} Database;

Database* db_init(char* dir, Database* core);
void db_free(Database* db);

bool process_cmd_no_db(ClusterManager* cm, char* input);
bool process_cmd_with_db(Database* db, char* input);
void list_tables(Database* db);
void process_file(Database* db, char* filename, bool show, bool internal);

void load_tc(Database* db);
void load_table_schema(Database* db);
void load_btree_cluster(Database* db, char* table_name);
void pop_btree_cluster(Database* db);

bool load_schema_tc(Database* db, char* table_name);
TableSchema* get_table_schema(Database* db, const char* filename);
TableSchema* get_table_schema_cmd(Database* db, JQLCommand* cmd, const char* alias_or_table);
TableSchema* get_primary_schema(Database* db, JQLCommand* cmd);
bool load_initial_schema(Database* db);

void load_constr_syscache(Database* db);


void load_lake(Database* db);
void flush_lake(Database* db);

#endif // DATABASE_H