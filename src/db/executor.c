#include "executor.h"

#include "../utils/log.h"

ExecutionResult process(Context* ctx, char* buffer) {
  if (!ctx || !ctx->lexer || !ctx->parser) {
    return (ExecutionResult){1, "Invalid execution context"};
  }

  lexer_set_buffer(ctx->lexer, buffer);
  parser_reset(ctx->parser);

  JQLCommand cmd = parser_parse(ctx->parser);
  return execute_cmd(ctx, &cmd);
}

ExecutionResult execute_cmd(Context* ctx, JQLCommand* cmd) {
  if (cmd->is_invalid) {
    return (ExecutionResult){1, "Invalid command"};
  }

  ExecutionResult result = {0, "Execution successful"};

  switch (cmd->type) {
    case CMD_CREATE:
      result = execute_create_table(ctx, cmd);
      switch_schema(ctx, cmd->schema->table_name);
      break;
    case CMD_INSERT:
      result = execute_insert(ctx, cmd);
      break;
    case CMD_SELECT:
      result = (ExecutionResult){1, "SELECT not implemented yet"};
      break;
    default:
      result = (ExecutionResult){1, "Unknown command type"};
  }

  return result;
}

ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd) {
  /*
  [4B]  DB_INIT_MAGIC
  [4B]  Table Count
  [4B * 255] Table Offsets
  (every file not every entry) 

  For each table:
    [4B] Schema Offset

    For each schema:
    [1B] Table Name Length
    [var] Table Name
    [1B] Column Count

    For each column:
      [1B] Column Name Length
      [var] Column Name
      [4B] Column Type
      [1B] Varchar Length
      [1B] Decimal Precision
      [1B] Decimal Scale
      [1B] Column Flags
      [1B] Has Default
      [var] Default Value
      [1B] Has Check
      [var] Check Expression
      [1B] Is Foreign Key
      [var] Foreign Table
      [var] Foreign Column
  */
  if (!ctx || !cmd || !ctx->tc_appender) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  IO* tca_io = ctx->tc_appender;
  TableSchema* schema = cmd->schema;
  
  uint32_t table_count;
  io_seek(tca_io, sizeof(uint32_t), SEEK_SET);
  if (io_read(tca_io, &table_count, sizeof(uint32_t)) != sizeof(uint32_t)) {
    table_count = ctx->table_count ? ctx->table_count : 0;
    io_write(tca_io, &table_count, sizeof(uint32_t)); 
  }

  uint32_t schema_offset = 0;
  io_write(tca_io, &schema_offset, sizeof(uint32_t));  
  io_flush(tca_io);

  schema_offset = io_tell(tca_io) - sizeof(uint32_t);  

  uint8_t table_name_length = (uint8_t)strlen(schema->table_name);
  io_write(tca_io, &table_name_length, sizeof(uint8_t));
  io_write(tca_io, schema->table_name, table_name_length);

  uint8_t column_count = (uint8_t)schema->column_count;
  io_write(tca_io, &column_count, sizeof(uint8_t));

  for (int i = 0; i < column_count; i++) {
    ColumnDefinition* col = &schema->columns[i];

    uint8_t col_name_length = (uint8_t)strlen(col->name);
    io_write(tca_io, &col_name_length, sizeof(uint8_t));
    io_write(tca_io, col->name, col_name_length);

    io_write(tca_io, &col->type, sizeof(int));
    io_write(tca_io, &col->type_varchar, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_precision, sizeof(uint8_t));
    io_write(tca_io, &col->type_decimal_scale, sizeof(uint8_t));

    io_write(tca_io, &col->is_primary_key, sizeof(bool));
    io_write(tca_io, &col->is_unique, sizeof(bool));
    io_write(tca_io, &col->is_not_null, sizeof(bool));
    io_write(tca_io, &col->is_index, sizeof(bool));
    io_write(tca_io, &col->is_auto_increment, sizeof(bool));

    io_write(tca_io, &col->has_default, sizeof(bool));
    if (col->has_default) {
      io_write(tca_io, col->default_value, MAX_IDENTIFIER_LEN);
    }

    io_write(tca_io, &col->has_check, sizeof(bool));
    if (col->has_check) {
      io_write(tca_io, col->check_expr, MAX_IDENTIFIER_LEN);
    }

    io_write(tca_io, &col->is_foreign_key, sizeof(bool));
    if (col->is_foreign_key) {
      io_write(tca_io, col->foreign_table, MAX_IDENTIFIER_LEN);
      io_write(tca_io, col->foreign_column, MAX_IDENTIFIER_LEN);
    }
  }

  table_count++;
  io_seek_write(ctx->tc_writer, TABLE_COUNT_OFFSET, &table_count, sizeof(uint32_t), SEEK_SET); 
  
  uint32_t schema_length = (uint32_t)(io_tell(tca_io) - schema_offset);
  io_seek(ctx->tc_writer, schema_offset, SEEK_SET); 
  io_write(ctx->tc_writer, &schema_length, sizeof(uint32_t)); 

  int offset_index = hash_table_name(schema->table_name) * sizeof(uint32_t) + (2 * sizeof(uint32_t));
  io_seek_write(ctx->tc_writer, offset_index, &schema_offset, sizeof(uint32_t), SEEK_SET);
  
  off_t schema_offset_before_flush = io_tell(tca_io); 

  io_flush(tca_io);

  char table_dir[MAX_PATH_LENGTH];
  snprintf(table_dir, sizeof(table_dir), "%s/%s", ctx->fs->tables_dir, schema->table_name);

  if (create_directory(table_dir) != 0) {
    LOG_ERROR("Failed to create table directory");

    io_seek(tca_io, schema_offset_before_flush, SEEK_SET);

    io_clear_buffer(tca_io); 
    io_clear_buffer(ctx->tc_writer); 

    if (tca_io->buf_size != 0 || ctx->tc_writer->buf_size != 0) {
      LOG_FATAL("Failed to clear schema buffer after mkdir failure.\n\t > run jugad-cli fix");
    }

    rmdir(table_dir);

    table_count--;  
    io_seek_write(ctx->tc_writer, TABLE_COUNT_OFFSET, &table_count, sizeof(uint32_t), SEEK_SET);

    return (ExecutionResult){0, "Table creat.ion failed"};;
  }

  char rows_file[MAX_PATH_LENGTH];
  int ret = snprintf(rows_file, MAX_PATH_LENGTH, "%s" PATH_SEPARATOR "rows.db", table_dir);
  if (ret >= MAX_PATH_LENGTH) {
    LOG_WARN("Rows file path is too long, truncating to fit buffer size.");
    rows_file[MAX_PATH_LENGTH - 1] = '\0';
  }

  FILE* rows_fp = fopen(rows_file, "w");
  if (!rows_fp) {
    LOG_ERROR("Failed to create rows file: %s", rows_file);
    rmdir(table_dir);
    return (ExecutionResult){0, "Failed to create rows file"};
  }

  fclose(rows_fp);

  io_flush(ctx->tc_writer);

  load_tc(ctx);
  load_schema_tc(ctx, schema->table_name);

  return (ExecutionResult){0, "Table schema written successfully"};
}

ExecutionResult execute_insert(Context* ctx, JQLCommand* cmd) {
  switch_schema(ctx, cmd->schema->table_name);

  if (!ctx || !cmd || !ctx->tc_appender) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  } 

  IO* io_A = ctx->tc_appender;

  TableSchema* schema = find_table_schema_tc(ctx, cmd->schema->table_name);
  
  if (!schema) {
    return (ExecutionResult){1, "Error: Invalid schema"};
  }

  cmd->schema = schema;
  uint8_t column_count = (uint8_t)cmd->schema->column_count;

  ColumnDefinition* primary_key_col = NULL;
  ColumnValue* primary_key_val = NULL;

  for (uint8_t i = 0; i < column_count; i++) {
    if (cmd->schema->columns[i].is_primary_key) {
      primary_key_col = &cmd->schema->columns[i];
      primary_key_val = &cmd->values[i];
      break;
    }
  }

  if (primary_key_col) {  
    // if (btree_search(ctx->btree, primary_key_val) != -1) {
    //   return (ExecutionResult){1, "Primary Key already exists"};
    // }
  }

  long row_start = io_tell(io_A);
  uint16_t row_length = 0;

  uint32_t row_id = ctx->schema.next_row_id++;

  // btree_insert(ctx->btree, primary_key_val, row_start);

  io_write(io_A, &row_length, sizeof(uint16_t));  
  io_write(io_A, &row_id, sizeof(uint32_t));
  io_write(io_A, &column_count, sizeof(uint8_t));

  for (uint8_t i = 0; i < column_count; i++) {
    ColumnValue* col_val = &cmd->values[i];
    ColumnDefinition* col_def = &cmd->schema->columns[i];

    io_write(io_A, &col_val->column_index, sizeof(uint8_t));
    write_column_value(io_A, col_val, col_def);
  }

  io_flush(io_A);

  row_length = (uint16_t)(io_tell(io_A) - row_start);
  io_seek_write(ctx->tc_writer, row_start, &row_length, sizeof(uint16_t), SEEK_SET);

  io_seek(io_A, row_start + row_length, SEEK_SET);
  io_flush(io_A);

  return (ExecutionResult){0, "Record inserted successfully"};
}


void write_column_value(IO* io, ColumnValue* col_val, ColumnDefinition* col_def) {
  uint16_t text_len, str_len, max_len;

  switch (col_def->type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
      io_write(io, &col_val->int_value, sizeof(int));
      break;

    case TOK_T_BOOL:
      uint8_t bool_value = col_val->bool_value ? 1 : 0;
      io_write(io, &bool_value, sizeof(uint8_t));
      break;

    case TOK_T_FLOAT:
      io_write(io, &col_val->float_value, sizeof(float));
      break;

    case TOK_T_DOUBLE:
      io_write(io, &col_val->double_value, sizeof(double));
      break;

    case TOK_T_DECIMAL:
      io_write(io, &col_val->decimal.precision, sizeof(int));
      io_write(io, &col_val->decimal.scale, sizeof(int));
      io_write(io, col_val->decimal.decimal_value, MAX_DECIMAL_LEN);
      break;

    case TOK_T_UUID: 
      size_t uuid_len = strlen(col_val->str_value);
      
      if (uuid_len == 36) {  
        uint8_t binary_uuid[16];
        if (!parse_uuid_string(col_val->str_value, binary_uuid)) {
          fprintf(stderr, "Error: Invalid UUID format.\n");
          return;
        }
        io_write(io, binary_uuid, 16);
      } else if (uuid_len == 16) {
          io_write(io, col_val->str_value, 16);
      } else {
          fprintf(stderr, "Error: Invalid UUID length.\n");
          return;
      }
      break;

    case TOK_T_TIMESTAMP:
    case TOK_T_DATETIME:
    case TOK_T_TIME:
    case TOK_T_DATE:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR: 
      str_len = (uint16_t)strlen(col_val->str_value);
      max_len = col_def->type_varchar == 0 ? 255 : col_def->type_varchar;

      if (str_len > max_len) {
        str_len = max_len;
      }

      io_write(io, &str_len, sizeof(uint8_t));
      io_write(io, col_val->str_value, str_len);
      break;

    case TOK_T_TEXT:
    case TOK_T_JSON:
      text_len = (uint16_t)strlen(col_val->str_value);
      max_len = (col_def->type == TOK_T_JSON) ? MAX_JSON_SIZE : MAX_TEXT_SIZE;

      if (text_len > max_len) {
          text_len = max_len;
      }
      io_write(io, &text_len, sizeof(uint16_t));
      io_write(io, col_val->str_value, text_len);
      break;

    // case TOK_T_BLOB: // TODO: SUPPORT BLOBs
    //   uint16_t blob_size = col_val->blob_value.size;
    //   if (blob_size > MAX_BLOB_SIZE) {
    //       fprintf(stderr, "Error: Blob size exceeds limit.\n");
    //       return;
    //   }
    //   io_write(io, &blob_size, sizeof(uint16_t));
    //   io_write(io, col_val->blob_value.data, blob_size);
    //   break;
    // 

    default:
      fprintf(stderr, "Error: Unsupported data type.\n");
      break;
  }
}


uint32_t get_table_offset(Context* ctx, const char* table_name) {
  for (int i = 0; i < ctx->table_count; i++) {
    if (strcmp(ctx->tc[i].name, table_name) == 0) {
      return ctx->tc[i].offset;
    }
  }
  return 0;  
}


bool parse_uuid_string(const char* uuid_str, uint8_t* output) {
  if (strlen(uuid_str) != 36) return false; 

  static const char hex_map[] = "0123456789abcdef";
  size_t j = 0;

  for (size_t i = 0; i < 36; i++) {
      if (uuid_str[i] == '-') continue;

      const char* p = strchr(hex_map, tolower(uuid_str[i]));
      if (!p) return false;

      output[j / 2] = (output[j / 2] << 4) | (p - hex_map);
      j++;
  }

  return j == 32;
}


// ExecutionOrder* generate_execution_plan(JQLCommand* command) {
//   ExecutionOrder* order = malloc(sizeof(ExecutionOrder));
//   if (!order) return NULL;

//   order->num_steps = 1;  
//   order->steps = malloc(sizeof(ExecutionStep) * order->num_steps);
//   if (!order->steps) {
//     free(order);
//     return NULL;
//   }

//   order->steps[0].type = EXECUTION_CREATE_TABLE;  
//   order->steps[0].command = *command;  

//   return order;
// }