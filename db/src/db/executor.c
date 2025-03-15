#include "executor.h"

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
  if (!cmd) {
    return (ExecutionResult){1, "Invalid command"};
  }

  ExecutionResult result = {0, "Execution successful"};

  switch (cmd->type) {
    case CMD_CREATE:
      result = execute_create_table(ctx, cmd);
      break;
    case CMD_INSERT:
      result = (ExecutionResult){1, "INSERT not implemented yet"};
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
  if (!ctx || !cmd || !ctx->appender) {
    return (ExecutionResult){1, "Invalid execution context or command"};
  }

  IO* io = ctx->appender;
  TableSchema schema = cmd->schema;

  uint8_t table_name_length = (uint8_t)strlen(schema.table_name); 
  io_write(io, &table_name_length, sizeof(uint8_t));  
  io_write(io, schema.table_name, table_name_length); 

  uint8_t column_count = (uint8_t)schema.column_count;
  io_write(io, &column_count, sizeof(uint8_t));

  for (int i = 0; i < column_count; i++) {
    uint8_t col_name_length = (uint8_t)strlen(schema.columns[i].name);
    io_write(io, &col_name_length, sizeof(uint8_t));

    io_write(io, schema.columns[i].name, col_name_length);

    int col_type = schema.columns[i].type;
    io_write(io, &col_type, sizeof(int));
  }

  io_flush(io);  

  return (ExecutionResult){0, "Table schema written successfully"};
}


TableSchema read_table_schema(Context* ctx) {
  if (!ctx || !ctx->reader) {
    fprintf(stderr, "Error: No database file is open.\n");
    return (TableSchema){};
  }

  TableSchema schema;
  memset(&schema, 0, sizeof(TableSchema));  // Zero out the struct
  memset(schema.table_name, 0, MAX_IDENTIFIER_LEN);

  schema.table_name[0] = '\0';

  uint8_t table_name_length;
  if (io_read(ctx->reader, &table_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
    fprintf(stderr, "Error: Failed to read table name length.\n");
    return (TableSchema){};
  }

  if (table_name_length >= MAX_IDENTIFIER_LEN) {
    fprintf(stderr, "Error: Table name too long.\n");
    return (TableSchema){};
  }

  if (io_read(ctx->reader, schema.table_name, table_name_length) != table_name_length) {
    fprintf(stderr, "Error: Failed to read table name.\n");
    return (TableSchema){};
  }
  schema.table_name[table_name_length] = '\0';

  if (io_read(ctx->reader, &schema.column_count, sizeof(uint8_t)) != sizeof(uint8_t)) {
    fprintf(stderr, "Error: Failed to read column count.\n");
    return (TableSchema){};
  }

  schema.columns = malloc(sizeof(ColumnDefinition) * schema.column_count);
  if (!schema.columns) {
    fprintf(stderr, "Error: Memory allocation failed for columns.\n");
    return (TableSchema){};
  }

  for (uint8_t i = 0; i < schema.column_count; i++) {
    ColumnDefinition* col = &schema.columns[i];

    uint8_t col_name_length;
    if (io_read(ctx->reader, &col_name_length, sizeof(uint8_t)) != sizeof(uint8_t)) {
      fprintf(stderr, "Error: Failed to read column name length.\n");
      return (TableSchema){};
    }

    if (col_name_length >= MAX_IDENTIFIER_LEN) {
      fprintf(stderr, "Error: Column name too long.\n");
      return (TableSchema){};
    }

    if (io_read(ctx->reader, col->name, col_name_length) != col_name_length) {
      fprintf(stderr, "Error: Failed to read column name.\n");
      return (TableSchema){};
    }
    col->name[col_name_length] = '\0';

    if (io_read(ctx->reader, &col->type, sizeof(int)) != sizeof(int)) {
      fprintf(stderr, "Error: Failed to read column type.\n");
      return (TableSchema){};
    }

    // Commenting out additional metadata
    /*
    io_read(ctx->reader, &col->type_varchar, sizeof(uint8_t));
    io_read(ctx->reader, &col->type_decimal_precision, sizeof(uint8_t));
    io_read(ctx->reader, &col->type_decimal_scale, sizeof(uint8_t));

    io_read(ctx->reader, &col->is_primary_key, sizeof(bool));
    io_read(ctx->reader, &col->is_unique, sizeof(bool));
    io_read(ctx->reader, &col->is_not_null, sizeof(bool));
    io_read(ctx->reader, &col->is_index, sizeof(bool));
    io_read(ctx->reader, &col->is_auto_increment, sizeof(bool));

    io_read(ctx->reader, &col->has_default, sizeof(bool));
    if (col->has_default) {
      io_read(ctx->reader, col->default_value, MAX_IDENTIFIER_LEN);
    }

    io_read(ctx->reader, &col->has_check, sizeof(bool));
    if (col->has_check) {
      io_read(ctx->reader, col->check_expr, MAX_IDENTIFIER_LEN);
    }

    io_read(ctx->reader, &col->is_foreign_key, sizeof(bool));
    if (col->is_foreign_key) {
      io_read(ctx->reader, col->foreign_table, MAX_IDENTIFIER_LEN);
      io_read(ctx->reader, col->foreign_column, MAX_IDENTIFIER_LEN);
    }
    */
  }


  return schema;
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