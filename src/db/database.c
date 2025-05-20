#include "database.h"
#include "executor.h"
#include "uuid.h"

#include "../utils/log.h"
#include "../utils/security.h"

Database* db_init(char* dir) {
  Database* db = (Database*)malloc(sizeof(Database));
  if (!db) {
    LOG_FATAL("Failed to allocate memory for database.");
    return NULL;
  }

  memset(db, 0, sizeof(Database));

  db->lexer = lexer_init();
  if (!db->lexer) {
    LOG_FATAL("Failed to initialize lexer.");
    free(db);
    return NULL;
  }

  db->parser = parser_init(db->lexer);
  if (!db->parser) {
    LOG_FATAL("Failed to initialize parser.");
    lexer_free(db->lexer);
    free(db);
    return NULL;
  }

  db->uuid = uuid();
  if (!db->uuid) {
    LOG_FATAL("Failed to generate UUID.");
    parser_free(db->parser);
    lexer_free(db->lexer);
    free(db);
    return NULL;
  }

  db->fs = fs_init(dir);
  if (!db->fs) {
    LOG_FATAL("Failed to initialize file system.");
    free(db->uuid);
    parser_free(db->parser);
    lexer_free(db->lexer);
    free(db);
    return NULL;
  }

  db->table_count = 0;
  memset(db->tc, 0, sizeof(db->tc));
  memset(db->tc, 0, sizeof(db->lake));

  db->tc_reader = io_init(db->fs->schema_file, FILE_READ, 1024);
  db->tc_writer = io_init(db->fs->schema_file, FILE_WRITE, 1024);
  db->tc_appender = io_init(db->fs->schema_file, FILE_APPEND, 1024);
  db->wal = wal_open(db->fs->wal_file, "w+b");

  load_tc(db);
  if (!load_initial_schema(db)) {
    LOG_FATAL("Failed to read schema");
  }
  load_lake(db);
  LOG_INFO("Successfully loaded %lu table(s) from catalog", db->table_count);
  
  register_builtin_functions();

  return db;
}

void db_free(Database* db) {
  if (!db) return;

  flush_lake(db);

  for (int i = 0; i < BTREE_LIFETIME_THRESHOLD; i++) {
    uint32_t idx = db->btree_idx_stack[i];
    TableCatalogEntry* tc = &db->tc[idx];

    if (!tc || !tc->schema) {
      break;
    }
    
    char dir_path[MAX_PATH_LENGTH];
    snprintf(dir_path, sizeof(dir_path), "%s" SEP "%s", db->fs->tables_dir, tc->schema->table_name);

    for (uint8_t j = 0; j < tc->schema->prim_column_count; j++) {
      unsigned int file_hash = hash_fnv1a(tc->schema->columns[j].name, MAX_COLUMNS);

      if (tc->btree[file_hash] != NULL) {
        
        char btree_file_path[MAX_PATH_LENGTH];
        snprintf(btree_file_path, sizeof(btree_file_path), "%s" SEP "%u.idx", dir_path, file_hash);
    
        unload_btree(tc->btree[file_hash], btree_file_path);
      }
    }
  }

  parser_free(db->parser);
  fs_free(db->fs);
  free(db->uuid);

  if (db->tc_reader) io_close(db->tc_reader);
  if (db->tc_writer) io_close(db->tc_writer);
  if (db->tc_appender) io_close(db->tc_appender);

  free(db);
}

bool process_dot_cmd(Database* db, char* input) {
  if (strcmp(input, ".help") == 0 || tolower(input[0]) == 'h') {
    LOG_INFO("Available commands:\n"
      "  tables       - List all tables\n"
      "  quit/Q         - Exit the program\n"
      "  help/H         - Show this help message\n"
      "  stats        - Show database statistics\n"
      "  dump <file>  - Export database to a file"
      "  exec/E <file>   - Run a script file"
    );
    return true;
  } else if (strcmp(input, "tables") == 0) {
    list_tables(db);
    return true;
  } else if (strcmp(input, "stats") == 0) {
    // show_db_stats(db); backtrack transactions?
    return true;
  } else if (strcmp(input, ".quit") == 0 || tolower(input[0]) == 'q') {
    LOG_INFO("Exiting...");
    db_free(db);
    exit(0);
  } else if (strcmp(input, "clear") == 0) {
    clear_screen();
    return true;
  } else if (strncmp(input, "exec", 4) == 0) {
    char* filename = input + 5;
    if (!input[4] || input[4] != ' ') {
      LOG_INFO("Expected a JQL/JCL file name after exec command");
      return true;
    }

    process_file(db, filename);
    return true;
  } else if (tolower(input[0]) == 'e') {
    char* filename = input + 2;
    if (!input[1] || input[1] != ' ') {
      LOG_INFO("Expected a JQL/JCL file name after exec command");
      return true;
    }

    process_file(db, filename);
    return true;
  }

  return false;
}

void list_tables(Database* db) {
  if (!db || db->table_count == 0) {
    LOG_INFO("No tables found in the database.");
    return;
  }

  LOG_INFO("Tables in the database:");
  for (int i = 0; i < MAX_TABLES; i++) {
    if (db->tc[i].schema) {
      printf("\\ %s (%u cols)\n", db->tc[i].schema->table_name, db->tc[i].schema->column_count);
      for (int j = 0; j < db->tc[i].schema->column_count; j++) {
        printf("|- %s (%s)\n", db->tc[i].schema->columns[j].name, get_token_type(db->tc[i].schema->columns[j].type));
      }
    }
  }
}

void process_file(Database* db, char* filename) {
  FILE* file = fopen(filename, "r");
  if (!file) {
    LOG_ERROR("Error opening file: %s", filename);
    return;
  }

  fseek(file, 0, SEEK_END); 
  long file_size = ftell(file);
  fseek(file, 0, SEEK_SET);

  if (file_size == -1) {
    LOG_ERROR("Error determining file size for %s", filename);
    fclose(file);
    return;
  }

  char* buffer = malloc(file_size + 1);
  if (!buffer) {
    LOG_ERROR("Memory allocation failed for buffer");
    fclose(file);
    return;
  }

  size_t bytes_read = fread(buffer, 1, file_size, file);
  if (bytes_read != file_size) {
    LOG_ERROR("Error reading file: expected %ld bytes but read %zu bytes", file_size, bytes_read);
    free(buffer);
    fclose(file);
    return;
  }

  buffer[bytes_read] = '\0';

  LOG_INFO("Processing file: %s", filename);
  Result* res_list = malloc(sizeof(Result) * 5);
  size_t res_n = 0;
  size_t res_capacity = 5; 
  
  lexer_set_buffer(db->lexer, buffer);
  parser_reset(db->parser);
  
  JQLCommand cmd = parser_parse(db);
  while (!is_struct_zeroed(&cmd, sizeof(JQLCommand))) {
    res_list[res_n] = execute_cmd(db, &cmd);
  
    if (res_n + 1 >= res_capacity) {
      res_capacity *= 2; 
      res_list = realloc(res_list, sizeof(Result) * res_capacity);
      if (!res_list) {
        LOG_ERROR("Memory allocation failed during reallocation");
        return;
      }
    }
  
    res_n++;
    cmd = parser_parse(db); 
  }

  free(buffer);
  fclose(file);
}

void load_tc(Database* db) {
  if (!db || !db->fs) return;

  load_table_schema(db);

  DIR* dir = opendir(db->fs->tables_dir);
  if (!dir) {
    LOG_ERROR("Failed to open schema directory: %s", db->fs->tables_dir);
    return;
  }

  struct dirent* entry;
  uint32_t tc = 0;

  while ((entry = readdir(dir)) != NULL) {
    if (entry->d_type == DT_DIR && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      bool found = false;

      for (int i = 0; i < db->table_count; i++) {
        if (strcmp(entry->d_name, db->tc[i].name) == 0) {
          found = true;
          break;
        }
      }

      if (!found) {
        continue; 
      }

      char table_path[MAX_PATH_LENGTH];
      snprintf(table_path, MAX_PATH_LENGTH, "%s" SEP "%s", db->fs->tables_dir, entry->d_name);

      char data_file[MAX_PATH_LENGTH];
      snprintf(data_file, MAX_PATH_LENGTH, "%.*s" SEP "rows.db",
        (int)(MAX_PATH_LENGTH - 10), table_path);

      if (access(data_file, F_OK) == 0) {
        tc += 1;  
      } else {
        LOG_WARN("Data file missing: %s", data_file);
      }
    }
  }


  if (tc < db->table_count) {
    LOG_WARN("Not all directories match the expected table names from the schema.");
    LOG_DEBUG("Table Catalog defines %ld whilst %d were read", db->table_count, tc);
  }

  closedir(dir);
}

void load_table_schema(Database* db) {
  if (!db || !db->fs) {
    LOG_ERROR("Invalid database or missing filesystem.");
    return;
  }
  
  io_seek(db->tc_reader, 0, SEEK_SET);

  uint32_t db_init;
  if (io_read(db->tc_reader, &db_init, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read database initialization magic number.");
    io_close(db->tc_reader);
    return;
  }

  if (db_init != DB_INIT_MAGIC) {
    LOG_ERROR("Invalid database file (wrong DB INIT magic number: 0x%X).", db_init);
    io_close(db->tc_reader);
    return;
  }

  if (io_read(db->tc_reader, &db->table_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read table count.");
    io_close(db->tc_reader);
    return;
  }

  if (db->table_count > MAX_TABLES) {
    LOG_ERROR("Table count exceeds maximum allowed tables.");
    io_close(db->tc_reader);
    return;
  }

  io_seek(db->tc_reader, sizeof(uint32_t) * MAX_TABLES, SEEK_CUR);

  for (uint32_t tc = 0; tc < db->table_count; tc++) {
    TableCatalogEntry* entry = &db->tc[tc];

    if (io_read(db->tc_reader, &entry->offset, sizeof(uint32_t)) != sizeof(uint32_t)) {
      LOG_ERROR("Failed to read table offset.");
      break;
    }

    if (io_read(db->tc_reader, &entry->name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read table name length.");
      break;
    }

    if (entry->name_length == 0 || (size_t)entry->name_length >= sizeof(entry->name)) {
      LOG_ERROR("Invalid table name length (%u).", entry->name_length);
      break;
    }

    if (io_read(db->tc_reader, entry->name, entry->name_length) != entry->name_length) {
      LOG_ERROR("Failed to read table name.");
      break;
    }
    entry->name[entry->name_length] = '\0';

    long current_pos = io_tell(db->tc_reader);
    long next_offset = entry->offset - (entry->name_length + sizeof(uint32_t) + sizeof(uint8_t));

    if (next_offset > 0) {
      io_seek(db->tc_reader, next_offset, SEEK_CUR);
    }
  }
}

void load_btree_cluster(Database* db, char* name) {
  uint8_t idx = hash_fnv1a(name, MAX_TABLES);

  if (db->tc[idx].is_populated) {
    // TODO: Consider double checking for new columns after ALTER is implemented
    return;
  }

  TableSchema* schema = (&db->tc[idx])->schema;

  char rows_db_path[MAX_PATH_LENGTH];
  snprintf(rows_db_path, sizeof(rows_db_path), "%s" SEP "%s" SEP "rows.db", 
            db->fs->tables_dir, name);
  if (!file_exists(rows_db_path)) {
    LOG_FATAL("Failed to find rows.db in directory '%s'.\n\t > run 'fix'", db->fs->tables_dir);
    return;
  }
  
  if (db->loaded_btree_clusters >= BTREE_LIFETIME_THRESHOLD) {
    pop_btree_cluster(db);
  }

  uint8_t found_prims = 0;
  for (uint8_t i = 0; i < schema->column_count; i++) {
    if (schema->columns[i].is_primary_key) {
      unsigned int file_hash = hash_fnv1a(schema->columns[i].name, MAX_COLUMNS);
      
      char btree_file_path[MAX_PATH_LENGTH];
      snprintf(btree_file_path, sizeof(btree_file_path), "%s" SEP "%s" SEP "%u.idx",
              db->fs->tables_dir, name, file_hash);
      

      FILE* fp = fopen(btree_file_path, "rb");
      BTree* btree = NULL;
      
      if (fp == NULL) {
        fp = fopen(btree_file_path, "wb+");
        if (!fp) {
          LOG_FATAL("Failed to create B-tree file: %s", btree_file_path);
          return;
        }
        btree = btree_create(schema->columns[i].type);  
        btree->id = file_hash;
      
        save_btree(btree, fp);
      } else {
        btree = load_btree(fp);
      }
      
      fclose(fp);
      
      db->tc[idx].btree[file_hash] = btree;
      db->tc[idx].is_populated = true;
      found_prims++;
      
      if (found_prims == schema->prim_column_count) {
        break;
      }
    }
  }
  
  if (found_prims != schema->prim_column_count) {
    LOG_FATAL("Mismatch: Loaded %u primary key B-trees, expected %u.", 
              found_prims, schema->prim_column_count);
    return;
  } 

  db->btree_idx_stack[db->loaded_btree_clusters] = idx;
  db->loaded_btree_clusters++;


  return;
}

void pop_btree_cluster(Database* db) {
  if (db->loaded_btree_clusters == 0) {
    LOG_WARN("No B-tree clusters to unload.");
    return;
  }

  uint32_t idx_to_unload = db->btree_idx_stack[db->loaded_btree_clusters - 1];
  
  TableCatalogEntry* tc = &db->tc[idx_to_unload];
  
  char dir_path[MAX_PATH_LENGTH];
  snprintf(dir_path, sizeof(dir_path), "%s" SEP "%s", db->fs->tables_dir, tc->name);

  for (uint8_t i = 0; i < tc->schema->prim_column_count; i++) {
    if (tc->btree[i] != NULL) {
      unsigned int file_hash = hash_fnv1a(tc->schema->columns[i].name, MAX_COLUMNS);
      
      char btree_file_path[MAX_PATH_LENGTH];
      snprintf(btree_file_path, sizeof(btree_file_path), "%s/%u.idx", dir_path, file_hash);
    
      unload_btree(tc->btree[i], btree_file_path);
    }
  }

  db->loaded_btree_clusters--;
}

bool load_schema_tc(Database* db, char* table_name) {
  if (!db || !db->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  unsigned int idx = hash_fnv1a(table_name, MAX_TABLES);
  if (db->tc[idx].schema) {
    if (strcmp(db->tc[idx].schema->table_name, table_name) == 0) {
      return true;
    }
  }
  
  uint32_t initial_offset = 0; 
  
  io_seek(db->tc_reader, (idx * sizeof(uint32_t)) + (2 * sizeof(uint32_t)), SEEK_SET);
  io_read(db->tc_reader, &initial_offset, sizeof(uint32_t));

  FILE* io = db->tc_reader;
  TableSchema* schema = malloc(sizeof(TableSchema));
  if (!schema) {
    LOG_ERROR("Memory allocation failed for schema.");
    return false;
  }

  initial_offset += sizeof(uint32_t);
  // Hash-index*4B + Magic Identifier 4B +Table Count 4B  + Table Offset 4B (skipping for reading purpose) 
  io_seek(io, initial_offset, SEEK_SET);

  uint8_t table_name_length;
  if (io_read(io, &table_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read table name length.");
    free(schema);
    return false;
  }

  if (io_read(io, schema->table_name, table_name_length) != table_name_length) {
    LOG_ERROR("Failed to read table name.");
    free(schema);
    return false;
  }
  schema->table_name[table_name_length] = '\0';

  if (io_read(io, &schema->column_count, sizeof(uint8_t)) != sizeof(uint8_t)) {
    LOG_ERROR("Failed to read column count.");
    free(schema);
    return false;
  }

  schema->columns = malloc(sizeof(ColumnDefinition) * schema->column_count);
  if (!schema->columns) {
    LOG_ERROR("Memory allocation failed for columns.");
    free(schema);
    return false;
  }
  
  for (uint8_t i = 0; i < schema->column_count; i++) {
    ColumnDefinition* col = &schema->columns[i];

    uint8_t col_name_length;
    if (io_read(io, &col_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read column name length.");
      free(schema->columns);
      free(schema);
      return false;
    }

    if (io_read(io, col->name, col_name_length) != col_name_length) {
      LOG_ERROR("Failed to read column name.");
      free(schema->columns);
      free(schema);
      return false;
    }
    col->name[col_name_length] = '\0';

    io_read(io, &col->type, sizeof(uint32_t));
    io_read(io, &col->type_varchar, sizeof(uint8_t));
    io_read(io, &col->type_decimal_precision, sizeof(uint8_t));
    io_read(io, &col->type_decimal_scale, sizeof(uint8_t));

    io_read(io, &col->has_constraints, sizeof(bool));
    io_read(io, &col->is_primary_key, sizeof(bool));
    io_read(io, &col->is_unique, sizeof(bool));
    io_read(io, &col->is_not_null, sizeof(bool));
    io_read(io, &col->is_array, sizeof(bool));
    io_read(io, &col->is_index, sizeof(bool));
    io_read(io, &col->is_auto_increment, sizeof(bool));

    io_read(io, &col->has_default, sizeof(bool));
    // if (col->has_default) {
    //   if (io_read(io, col->default_value, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
    //     LOG_ERROR("Failed to read default value.");
    //     free(schema->columns);
    //     free(schema);
    //     return false;
    //   }
    // }

    io_read(io, &col->has_check, sizeof(bool));
    if (col->has_check) {
      if (io_read(io, col->check_expr, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
        LOG_ERROR("Failed to read check constraint.");
        free(schema->columns);
        free(schema);
        return false;
      }
    }

    io_read(io, &col->is_foreign_key, sizeof(bool));
    if (col->is_foreign_key) {
      if (io_read(io, col->foreign_table, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN 
      || io_read(io, col->foreign_column, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN
      || io_read(io, &col->on_delete, sizeof(FKAction)) != sizeof(FKAction)
      || io_read(io, &col->on_update, sizeof(FKAction)) != sizeof(FKAction)) {
        LOG_ERROR("Failed to read foreign key details.");
        free(schema->columns);
        free(schema);
        return false;
      }
    }
    
    if (col->is_primary_key) {
      schema->prim_column_count += 1;
    }

    if (col->is_not_null) {
      schema->not_null_count += 1;
    }
  }

  db->tc[idx].schema = schema;
  LOG_INFO("Created new schema entry in the in memory catalog at %d", idx);
  return true;
}

TableSchema* find_table_schema_tc(Database* db, const char* filename) {
  if (!db || !filename) {
    LOG_ERROR("Invalid database or filename provided.");
    return NULL;
  }

  unsigned int idx = hash_fnv1a(filename, MAX_TABLES);
  
  if (db->tc[idx].schema && strcmp(db->tc[idx].schema->table_name, filename) == 0) {
    return db->tc[idx].schema;
  }

  for (int i = 0; i < db->table_count; i++) {
    if (db->tc[i].schema && strcmp(db->tc[i].schema->table_name, filename) == 0) {
      return db->tc[i].schema;
    }
  }

  LOG_ERROR("Schema for filename '%s' not found.", filename);
  return NULL;
}

bool load_initial_schema(Database* db) {
  if (!db || !db->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  FILE* io = db->tc_reader;
  size_t CONTENTS_OFFSET = 2 * sizeof(uint32_t) + MAX_TABLES * sizeof(uint32_t);
  io_seek(io, CONTENTS_OFFSET, SEEK_SET);

  for (size_t i = 0; i < db->table_count; i++) {
    const char* table_name = db->tc[i].name;
    unsigned int idx = hash_fnv1a(table_name, MAX_TABLES);
    
    if (db->tc[idx].schema) continue;

    TableSchema* schema = malloc(sizeof(TableSchema));
    if (!schema) {
      LOG_ERROR("Memory allocation failed for schema.");
      return false;
    }

    io_seek(io, sizeof(uint32_t), SEEK_CUR);

    uint8_t table_name_length;
    if (io_read(io, &table_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read table name length.");
      free(schema);
      return false;
    }

    if (io_read(io, schema->table_name, table_name_length) != table_name_length) {
      LOG_ERROR("Failed to read table name.");
      free(schema);
      return false;
    }
    schema->table_name[table_name_length] = '\0';

    if (io_read(io, &schema->column_count, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read column count.");
      free(schema);
      return false;
    }

    schema->columns = malloc(sizeof(ColumnDefinition) * schema->column_count);
    if (!schema->columns) {
      LOG_ERROR("Memory allocation failed for columns.");
      free(schema);
      return false;
    }
    
    for (uint8_t j = 0; j < schema->column_count; j++) {
      ColumnDefinition* col = &schema->columns[j];

      uint8_t col_name_length;
      if (io_read(io, &col_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
        LOG_ERROR("Failed to read column name length.");
        free(schema->columns);
        free(schema);
        return false;
      }

      if (io_read(io, col->name, col_name_length) != col_name_length) {
        LOG_ERROR("Failed to read column name.");
        free(schema->columns);
        free(schema);
        return false;
      }
      col->name[col_name_length] = '\0';

      io_read(io, &col->type, sizeof(uint32_t));
      io_read(io, &col->type_varchar, sizeof(uint8_t));
      io_read(io, &col->type_decimal_precision, sizeof(uint8_t));
      io_read(io, &col->type_decimal_scale, sizeof(uint8_t));

      io_read(io, &col->has_constraints, sizeof(bool));
      io_read(io, &col->is_primary_key, sizeof(bool));
      io_read(io, &col->is_unique, sizeof(bool));
      io_read(io, &col->is_not_null, sizeof(bool));
      io_read(io, &col->is_array, sizeof(bool));
      io_read(io, &col->is_index, sizeof(bool));
      io_read(io, &col->is_auto_increment, sizeof(bool));

      io_read(io, &col->has_default, sizeof(bool));
      // if (col->has_default) {
      //   if (io_read(io, col->default_value, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
      //     LOG_ERROR("Failed to read default value.");
      //     free(schema->columns);
      //     free(schema);
      //     return false;
      //   }
      // }

      io_read(io, &col->has_check, sizeof(bool));
      if (col->has_check) {
        if (io_read(io, col->check_expr, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
          LOG_ERROR("Failed to read check constraint.");
          free(schema->columns);
          free(schema);
          return false;
        }
      }

      io_read(io, &col->is_foreign_key, sizeof(bool));
      if (col->is_foreign_key) {
        if (io_read(io, col->foreign_table, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN 
        || io_read(io, col->foreign_column, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN
        || io_read(io, &col->on_delete, sizeof(FKAction)) != sizeof(FKAction)
        || io_read(io, &col->on_update, sizeof(FKAction)) != sizeof(FKAction)) {
          LOG_ERROR("Failed to read foreign key details.");
          free(schema->columns);
          free(schema);
          return false;
        }
      }

      if (col->is_primary_key) {
        schema->prim_column_count += 1;
      }  

      if (col->is_not_null) {
        schema->not_null_count += 1;
      }
    }

    db->tc[idx].schema = schema;
  }

  return true;
}


void load_lake(Database* db) {
  FILE* file = NULL;
  char file_path[MAX_PATH_LENGTH];

  for (int i = 0; i < MAX_TABLES; i++) {
    if (db->tc[i].schema) {
      sprintf(file_path, "%s" SEP "%s" SEP "rows.db", db->fs->tables_dir, db->tc[i].schema->table_name);
      file = fopen(file_path, "rb");
      if (!file) {
        LOG_WARN("Could not open file for reading: %s", file_path);
        continue;
      }

      fseek(file, 0, SEEK_END);
      long file_size = ftell(file);
      fseek(file, 0, SEEK_SET);

      uint32_t num_pages = (file_size + PAGE_SIZE - 1) / PAGE_SIZE;
      if (num_pages == 0) num_pages = 1;

      uint32_t idx = hash_fnv1a(db->tc[i].schema->table_name, MAX_TABLES);

      for (int j = 0; j < num_pages; j++) {
        uint32_t pg_n = j;
        
        if (!db->lake[idx].pages[pg_n]) {
          db->lake[idx].pages[pg_n] = page_init(pg_n);
          read_page(file, pg_n, db->lake[idx].pages[pg_n], db->tc[idx]);
          db->lake[idx].num_pages += 1;
        }

        if ((j + 1) == num_pages) {
          break;
        }
      }

      fclose(file);

      memcpy(db->lake[idx].file, file_path, MAX_PATH_LENGTH - 1);
      db->lake[idx].file[MAX_PATH_LENGTH - 1] = '\0';
      db->lake[idx].idx = idx; 
    }
  }
}

void flush_lake(Database* db) {
  FILE* file = NULL;

  for (int i = 0; i < MAX_COLUMNS; i++) {
    uint32_t idx = db->lake[i].idx;
    if (db->lake[idx].file[0] != 0) {  

      file = fopen(db->lake[i].file, "r+b"); 
      if (!file) {
        file = fopen(db->lake[i].file, "w+b");
      }
      
      for (int j = 0; j < POOL_SIZE; j++) {
        if (db->lake[idx].pages[j] == NULL) {
          continue;
        }
        
        if (is_struct_zeroed(db->lake[idx].pages[j], sizeof(Page))) {
          continue;
        }

        if (db->lake[idx].pages[j]->is_dirty){
          write_page(file, db->lake[idx].page_numbers[j], db->lake[idx].pages[j], db->tc[idx]);
          db->lake[idx].pages[j]->is_dirty = false; 
        }
      }
      
      fclose(file);
    }
  }

  FILE* wal = fopen(db->fs->wal_file, "wb");
  fclose(wal);
}