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


  uint8_t table_name_length = (uint8_t)strlen(cmd->table); 
  io_write(io, &table_name_length, sizeof(uint8_t));  
  io_write(io, cmd->table, table_name_length); 

  uint8_t column_count = (uint8_t)cmd->column_count;
  io_write(io, &column_count, sizeof(uint8_t));

  for (int i = 0; i < column_count; i++) {
    uint8_t col_name_length = (uint8_t)strlen(cmd->columns[i]);
    io_write(io, &col_name_length, sizeof(uint8_t));

    io_write(io, cmd->columns[i], col_name_length);

    int col_type = cmd->column_types[i];
    printf("%d\n", col_type);
    io_write(io, &col_type, sizeof(int));
  }

  io_flush(io);  

  return (ExecutionResult){0, "Table schema written successfully"};
}


void read_table_schema(Context* ctx) {
  if (!ctx || !ctx->reader) {
    printf("Error: Invalid context or reader.\n");
    return;
  }

  IO* io = ctx->reader;

  uint8_t table_name_length;
  io_read(io, &table_name_length, sizeof(uint8_t));

  char table_name[256];  
  io_read(io, table_name, table_name_length);
  table_name[table_name_length] = '\0';

  uint8_t column_count;
  io_read(io, &column_count, sizeof(uint8_t));

  printf("Table: %s\n", table_name);
  printf("Columns (%d):\n", column_count);

  for (int i = 0; i < column_count; i++) {
    uint8_t col_name_length;
    io_read(io, &col_name_length, sizeof(uint8_t));

    char column_name[256];
    io_read(io, column_name, col_name_length);
    column_name[col_name_length] = '\0';

    int column_type;
    io_read(io, &column_type, sizeof(int));

    printf("  - %s (Type: %d)\n", column_name, column_type);
  }
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