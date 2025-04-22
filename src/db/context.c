#include "context.h"
#include "executor.h"
#include "uuid.h"
#include "toast.h"

#include "../utils/log.h"
#include "../utils/security.h"

Context* ctx_init(char* dir) {
  Context* ctx = (Context*)malloc(sizeof(Context));
  if (!ctx) {
    LOG_FATAL("Failed to allocate memory for context.");
    return NULL;
  }

  memset(ctx, 0, sizeof(Context));

  ctx->lexer = lexer_init();
  if (!ctx->lexer) {
    LOG_FATAL("Failed to initialize lexer.");
    free(ctx);
    return NULL;
  }

  ctx->parser = parser_init(ctx->lexer);
  if (!ctx->parser) {
    LOG_FATAL("Failed to initialize parser.");
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->uuid = uuid();
  if (!ctx->uuid) {
    LOG_FATAL("Failed to generate UUID.");
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->fs = fs_init(dir);
  if (!ctx->fs) {
    LOG_FATAL("Failed to initialize file system.");
    free(ctx->uuid);
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->table_count = 0;
  memset(ctx->tc, 0, sizeof(ctx->tc));
  memset(ctx->tc, 0, sizeof(ctx->lake));

  ctx->tc_reader = io_init(ctx->fs->schema_file, FILE_READ, 1024);
  ctx->tc_writer = io_init(ctx->fs->schema_file, FILE_WRITE, 1024);
  ctx->tc_appender = io_init(ctx->fs->schema_file, FILE_APPEND, 1024);

  load_tc(ctx);
  if (!load_initial_schema(ctx)) {
    LOG_FATAL("Failed to read schema");
  }
  load_lake(ctx);
  LOG_INFO("Successfully loaded %lu table(s) from catalog", ctx->table_count);
  
  register_builtin_functions();
  toast_create(ctx);

  return ctx;
}

void ctx_free(Context* ctx) {
  if (!ctx) return;

  flush_lake(ctx);

  for (int i = 0; i < BTREE_LIFETIME_THRESHOLD; i++) {
    uint32_t idx = ctx->btree_idx_stack[i];
    TableCatalogEntry* tc = &ctx->tc[idx];

    if (!tc || !tc->schema) {
      break;
    }
    
    char dir_path[MAX_PATH_LENGTH];
    snprintf(dir_path, sizeof(dir_path), "%s" SEP "%s", ctx->fs->tables_dir, tc->schema->table_name);

    for (uint8_t j = 0; j < tc->schema->prim_column_count; j++) {
      unsigned int file_hash = hash_fnv1a(tc->schema->columns[j].name, MAX_COLUMNS);

      if (tc->btree[file_hash] != NULL) {
        
        char btree_file_path[MAX_PATH_LENGTH];
        snprintf(btree_file_path, sizeof(btree_file_path), "%s" SEP "%u.idx", dir_path, file_hash);
    
        unload_btree(tc->btree[file_hash], btree_file_path);
      }
    }
  }

  parser_free(ctx->parser);
  fs_free(ctx->fs);
  free(ctx->uuid);

  if (ctx->tc_reader) io_close(ctx->tc_reader);
  if (ctx->tc_writer) io_close(ctx->tc_writer);
  if (ctx->tc_appender) io_close(ctx->tc_appender);

  free(ctx);
}

bool process_dot_cmd(Context* ctx, char* input) {
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
    list_tables(ctx);
    return true;
  } else if (strcmp(input, "stats") == 0) {
    // show_db_stats(ctx); backtrack transactions?
    return true;
  } else if (strcmp(input, ".quit") == 0 || tolower(input[0]) == 'q') {
    LOG_INFO("Exiting...");
    ctx_free(ctx);
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

    process_file(ctx, filename);
    return true;
  } else if (tolower(input[0]) == 'e') {
    char* filename = input + 2;
    if (!input[1] || input[1] != ' ') {
      LOG_INFO("Expected a JQL/JCL file name after exec command");
      return true;
    }

    process_file(ctx, filename);
    return true;
  }

  return false;
}

void list_tables(Context* ctx) {
  if (!ctx || ctx->table_count == 0) {
    LOG_INFO("No tables found in the database.");
    return;
  }

  LOG_INFO("Tables in the database:");
  for (int i = 0; i < MAX_TABLES; i++) {
    if (ctx->tc[i].schema) {
      printf("\\ %s (%u cols)\n", ctx->tc[i].schema->table_name, ctx->tc[i].schema->column_count);
      for (int j = 0; j < ctx->tc[i].schema->column_count; j++) {
        printf("|- %s (%s)\n", ctx->tc[i].schema->columns[j].name, get_token_type(ctx->tc[i].schema->columns[j].type));
      }
    }
  }
}

void process_file(Context* ctx, char* filename) {
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
  
  lexer_set_buffer(ctx->lexer, buffer);
  parser_reset(ctx->parser);
  
  JQLCommand cmd = parser_parse(ctx);
  while (!is_struct_zeroed(&cmd, sizeof(JQLCommand))) {
    res_list[res_n] = execute_cmd(ctx, &cmd);
  
    if (res_n + 1 >= res_capacity) {
      res_capacity *= 2; 
      res_list = realloc(res_list, sizeof(Result) * res_capacity);
      if (!res_list) {
        LOG_ERROR("Memory allocation failed during reallocation");
        return;
      }
    }
  
    res_n++;
    cmd = parser_parse(ctx); 
  }

  free(buffer);
  fclose(file);
}

void load_tc(Context* ctx) {
  if (!ctx || !ctx->fs) return;

  load_table_schema(ctx);

  DIR* dir = opendir(ctx->fs->tables_dir);
  if (!dir) {
    LOG_ERROR("Failed to open schema directory: %s", ctx->fs->tables_dir);
    return;
  }

  struct dirent* entry;
  uint32_t tc = 0;

  while ((entry = readdir(dir)) != NULL) {
    if (entry->d_type == DT_DIR && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      bool found = false;

      for (int i = 0; i < ctx->table_count; i++) {
        if (strcmp(entry->d_name, ctx->tc[i].name) == 0) {
          found = true;
          break;
        }
      }

      if (!found) {
        continue; 
      }

      char table_path[MAX_PATH_LENGTH];
      snprintf(table_path, MAX_PATH_LENGTH, "%s" SEP "%s", ctx->fs->tables_dir, entry->d_name);

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


  if (tc < ctx->table_count) {
    LOG_WARN("Not all directories match the expected table names from the schema.");
    LOG_DEBUG("Table Catalog defines %ld whilst %d were read", ctx->table_count, tc);
  }

  closedir(dir);
}

void load_table_schema(Context* ctx) {
  if (!ctx || !ctx->fs) {
    LOG_ERROR("Invalid context or missing filesystem.");
    return;
  }
  
  io_seek(ctx->tc_reader, 0, SEEK_SET);

  uint32_t db_init;
  if (io_read(ctx->tc_reader, &db_init, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read database initialization magic number.");
    io_close(ctx->tc_reader);
    return;
  }

  if (db_init != DB_INIT_MAGIC) {
    LOG_ERROR("Invalid database file (wrong DB INIT magic number: 0x%X).", db_init);
    io_close(ctx->tc_reader);
    return;
  }

  if (io_read(ctx->tc_reader, &ctx->table_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
    LOG_ERROR("Failed to read table count.");
    io_close(ctx->tc_reader);
    return;
  }

  if (ctx->table_count > MAX_TABLES) {
    LOG_ERROR("Table count exceeds maximum allowed tables.");
    io_close(ctx->tc_reader);
    return;
  }

  io_seek(ctx->tc_reader, sizeof(uint32_t) * MAX_TABLES, SEEK_CUR);

  for (uint32_t tc = 0; tc < ctx->table_count; tc++) {
    TableCatalogEntry* entry = &ctx->tc[tc];

    if (io_read(ctx->tc_reader, &entry->offset, sizeof(uint32_t)) != sizeof(uint32_t)) {
      LOG_ERROR("Failed to read table offset.");
      break;
    }

    if (io_read(ctx->tc_reader, &entry->name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      LOG_ERROR("Failed to read table name length.");
      break;
    }

    if (entry->name_length == 0 || (size_t)entry->name_length >= sizeof(entry->name)) {
      LOG_ERROR("Invalid table name length (%u).", entry->name_length);
      break;
    }

    if (io_read(ctx->tc_reader, entry->name, entry->name_length) != entry->name_length) {
      LOG_ERROR("Failed to read table name.");
      break;
    }
    entry->name[entry->name_length] = '\0';

    long current_pos = io_tell(ctx->tc_reader);
    long next_offset = entry->offset - (entry->name_length + sizeof(uint32_t) + sizeof(uint8_t));

    if (next_offset > 0) {
      io_seek(ctx->tc_reader, next_offset, SEEK_CUR);
    }
  }
}

void load_btree_cluster(Context* ctx, char* name) {
  uint8_t idx = hash_fnv1a(name, MAX_TABLES);

  if (ctx->tc[idx].is_populated) {
    // TODO: Consider double checking for new columns after ALTER is implemented
    return;
  }

  TableSchema* schema = (&ctx->tc[idx])->schema;

  char rows_db_path[MAX_PATH_LENGTH];
  snprintf(rows_db_path, sizeof(rows_db_path), "%s" SEP "%s" SEP "rows.db", 
            ctx->fs->tables_dir, name);
  if (!file_exists(rows_db_path)) {
    LOG_FATAL("Failed to find rows.db in directory '%s'.\n\t > run jugad-cli fix", ctx->fs->tables_dir);
    return;
  }
  
  if (ctx->loaded_btree_clusters >= BTREE_LIFETIME_THRESHOLD) {
    pop_btree_cluster(ctx);
  }

  uint8_t found_prims = 0;
  for (uint8_t i = 0; i < schema->column_count; i++) {
    if (schema->columns[i].is_primary_key) {
      unsigned int file_hash = hash_fnv1a(schema->columns[i].name, MAX_COLUMNS);
      
      char btree_file_path[MAX_PATH_LENGTH];
      snprintf(btree_file_path, sizeof(btree_file_path), "%s" SEP "%s" SEP "%u.idx",
              ctx->fs->tables_dir, name, file_hash);
      

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
      
      ctx->tc[idx].btree[file_hash] = btree;
      ctx->tc[idx].is_populated = true;
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

  ctx->loaded_btree_clusters++;
  ctx->btree_idx_stack[ctx->loaded_btree_clusters - 1] = idx;


  return;
}

void pop_btree_cluster(Context* ctx) {
  if (ctx->loaded_btree_clusters == 0) {
    LOG_WARN("No B-tree clusters to unload.");
    return;
  }

  uint32_t idx_to_unload = ctx->btree_idx_stack[ctx->loaded_btree_clusters - 1];
  
  TableCatalogEntry* tc = &ctx->tc[idx_to_unload];
  
  char dir_path[MAX_PATH_LENGTH];
  snprintf(dir_path, sizeof(dir_path), "%s" SEP "%s", ctx->fs->tables_dir, tc->name);

  for (uint8_t i = 0; i < tc->schema->prim_column_count; i++) {
    if (tc->btree[i] != NULL) {
      unsigned int file_hash = hash_fnv1a(tc->schema->columns[i].name, MAX_COLUMNS);
      
      char btree_file_path[MAX_PATH_LENGTH];
      snprintf(btree_file_path, sizeof(btree_file_path), "%s/%u.idx", dir_path, file_hash);
    
      unload_btree(tc->btree[i], btree_file_path);
    }
  }

  ctx->loaded_btree_clusters--;
}

bool load_schema_tc(Context* ctx, char* table_name) {
  if (!ctx || !ctx->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  unsigned int idx = hash_fnv1a(table_name, MAX_TABLES);
  if (ctx->tc[idx].schema) {
    if (strcmp(ctx->tc[idx].schema->table_name, table_name) == 0) {
      return true;
    }
  }
  
  uint32_t initial_offset = 0; 
  
  io_seek(ctx->tc_reader, (idx * sizeof(uint32_t)) + (2 * sizeof(uint32_t)), SEEK_SET);
  io_read(ctx->tc_reader, &initial_offset, sizeof(uint32_t));

  FILE* io = ctx->tc_reader;
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

    io_read(io, &col->is_primary_key, sizeof(bool));
    io_read(io, &col->is_unique, sizeof(bool));
    io_read(io, &col->is_not_null, sizeof(bool));
    io_read(io, &col->is_index, sizeof(bool));
    io_read(io, &col->is_auto_increment, sizeof(bool));

    io_read(io, &col->has_default, sizeof(bool));
    if (col->has_default) {
      if (io_read(io, col->default_value, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
        LOG_ERROR("Failed to read default value.");
        free(schema->columns);
        free(schema);
        return false;
      }
    }

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
      if (io_read(io, col->foreign_table, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN ||
          io_read(io, col->foreign_column, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
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

  ctx->tc[idx].schema = schema;
  LOG_INFO("Created new schema entry in the in memory catalog at %d", idx);
  return true;
}

TableSchema* find_table_schema_tc(Context* ctx, const char* filename) {
  if (!ctx || !filename) {
    LOG_ERROR("Invalid context or filename provided.");
    return NULL;
  }

  unsigned int idx = hash_fnv1a(filename, MAX_TABLES);
  
  if (ctx->tc[idx].schema && strcmp(ctx->tc[idx].schema->table_name, filename) == 0) {
    return ctx->tc[idx].schema;
  }

  for (int i = 0; i < ctx->table_count; i++) {
    if (ctx->tc[i].schema && strcmp(ctx->tc[i].schema->table_name, filename) == 0) {
      return ctx->tc[i].schema;
    }
  }

  LOG_ERROR("Schema for filename '%s' not found.", filename);
  return NULL;
}

bool load_initial_schema(Context* ctx) {
  if (!ctx || !ctx->tc_reader) {
    LOG_ERROR("No database file is open.");
    return false;
  }

  FILE* io = ctx->tc_reader;
  size_t CONTENTS_OFFSET = 2 * sizeof(uint32_t) + MAX_TABLES * sizeof(uint32_t);
  io_seek(io, CONTENTS_OFFSET, SEEK_SET);

  for (size_t i = 0; i < ctx->table_count; i++) {
    const char* table_name = ctx->tc[i].name;
    unsigned int idx = hash_fnv1a(table_name, MAX_TABLES);
    
    if (ctx->tc[idx].schema) continue;

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
      io_read(io, &col->is_primary_key, sizeof(bool));

      io_read(io, &col->is_unique, sizeof(bool));
      io_read(io, &col->is_not_null, sizeof(bool));
      io_read(io, &col->is_index, sizeof(bool));
      io_read(io, &col->is_auto_increment, sizeof(bool));

      io_read(io, &col->has_default, sizeof(bool));
      if (col->has_default) {
        if (io_read(io, col->default_value, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
          LOG_ERROR("Failed to read default value.");
          free(schema->columns);
          free(schema);
          return false;
        }
      }

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
        if (io_read(io, col->foreign_table, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN ||
            io_read(io, col->foreign_column, MAX_IDENTIFIER_LEN) != MAX_IDENTIFIER_LEN) {
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

    ctx->tc[idx].schema = schema;
  }

  return true;
}


void load_lake(Context* ctx) {
  FILE* file = NULL;
  char file_path[MAX_PATH_LENGTH];

  for (int i = 0; i < MAX_TABLES; i++) {
    if (ctx->tc[i].schema) {
      sprintf(file_path, "%s" SEP "%s" SEP "rows.db", ctx->fs->tables_dir, ctx->tc[i].schema->table_name);
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

      uint32_t idx = hash_fnv1a(ctx->tc[i].schema->table_name, MAX_TABLES);

      for (int j = 0; j < num_pages; j++) {
        uint32_t pg_n = j;
        
        if (!ctx->lake[idx].pages[pg_n]) {
          ctx->lake[idx].pages[pg_n] = page_init(pg_n);
          read_page(file, pg_n, ctx->lake[idx].pages[pg_n], ctx->tc[idx]);
          ctx->lake[idx].num_pages += 1;
        }

        if ((j + 1) == num_pages) {
          break;
        }
      }

      fclose(file);

      memcpy(ctx->lake[idx].file, file_path, MAX_PATH_LENGTH - 1);
      ctx->lake[idx].file[MAX_PATH_LENGTH - 1] = '\0';
      ctx->lake[idx].idx = idx; 
    }
  }
}

void flush_lake(Context* ctx) {
  FILE* file = NULL;

  for (int i = 0; i < MAX_COLUMNS; i++) {
    if (ctx->lake[i].file[0] != 0) {  

      file = fopen(ctx->lake[i].file, "r+b"); 
      if (!file) {
        file = fopen(ctx->lake[i].file, "w+b");
      }
      
      for (int j = 0; j < ctx->lake[i].num_pages; j++) {
        uint32_t pg_n = ctx->lake[i].page_numbers[j];
        uint32_t idx = ctx->lake[i].idx;
        
        if (ctx->lake[i].pages[pg_n]->is_dirty
          && (!(is_struct_zeroed(ctx->lake[i].pages[pg_n], sizeof(Page))))
        ) {
          write_page(file, pg_n, ctx->lake[i].pages[pg_n], ctx->tc[idx]);
          ctx->lake[i].pages[pg_n]->is_dirty = false;
          LOG_DEBUG("Updating pool %d, NPN: %u, File: %s, I: %u", 
            i, ctx->lake[i].next_pg_no, ctx->lake[i].file, ctx->lake[i].idx);      
        }
      }
      
      fclose(file);
    }
  }
}