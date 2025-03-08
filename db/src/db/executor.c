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
  printf("Executing CREATE TABLE command\n");
  printf("Table Name: %s\n", cmd->table);

  printf("Columns:\n");
  for (int i = 0; i < cmd->column_count; i++) {
    printf("\tColumn %d: %s of type %d\n", i + 1, cmd->columns[i], cmd->column_types[i]);
  }

  printf("Constraints: None specified (future functionality)\n");

  ExecutionResult result;
  result.status_code = 0;
  result.message = "Table created successfully"; 

  return result;
}
