#include "context.h"
#include "executor.h"

#include "uuid.h"

Context* ctx_init() {
  Context* ctx = malloc(sizeof(Context));
  if (!ctx) return NULL; 

  ctx->lexer = lexer_init();
  if (!ctx->lexer) {
    free(ctx);
    return NULL;
  }

  ctx->parser = parser_init(ctx->lexer);
  if (!ctx->parser) {
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  ctx->reader = NULL;
  ctx->writer = NULL;
  ctx->appender = NULL;

  ctx->uuid = uuid();
  if (!ctx->uuid) { 
    parser_free(ctx->parser);
    lexer_free(ctx->lexer);
    free(ctx);
    return NULL;
  }

  return ctx;
}

void ctx_free(Context* ctx) {
  parser_free(ctx->parser);
  free(ctx);
}

ExecutionResult process(Context* ctx, char* buffer) {
  lexer_set_buffer(ctx->lexer, buffer);
  parser_reset(ctx->parser);

  JQLCommand cmd = parser_parse(ctx->parser);
  return execute_cmd(ctx, &cmd);
}

bool process_dot_cmd(Context* ctx, char* input) {
  if (strncmp(input, ".schema", 7) == 0) {
    char* schema_name = input + 8; 
    while (*schema_name == ' ') schema_name++;

    if (*schema_name == '\0') {
      printf("Usage: .schema <schemaname>\n");
    } else {
      printf("Changing schema to: %s\n", schema_name);
      switch_schema(ctx, schema_name);
    }
    return true;
  } else if (strcmp(input, ".help") == 0) {
    printf("Available commands:\n");
    printf("  .schema <schemaname>  - Show schema of the given name\n");
    printf("  .quit                 - Exit the program\n");
    printf("  .help                 - Show this help message\n");
    return true;
  } else if (strcmp(input, ".quit") == 0) {
    printf("Exiting...\n");
    exit(0);
  }

  return false;
}

void process_file(char* filename) {

}

void switch_schema(Context* ctx, char* filename) {
  if (ctx->filename && strcmp(ctx->filename, filename) == 0) {
    return;
  }

  if (ctx->filename) {
    free(ctx->filename);
  }

  ctx->filename = strdup(filename);

  if (ctx->reader) io_close(ctx->reader);
  if (ctx->writer) io_close(ctx->writer);
  if (ctx->appender) io_close(ctx->appender);

  ctx->reader = io_init(ctx->filename, IO_READ, 1024);
  ctx->writer = io_init(ctx->filename, IO_WRITE, 1024);
  ctx->appender = io_init(ctx->filename, IO_APPEND, 1024);
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
  // ExecutionOrder* order = generate_execution_plan(cmd);
  // if (!order) {
  //   return (ExecutionResult){1, "Failed to generate execution plan"};
  // }

  ExecutionResult result = {0, "Execution successful"};

  if (cmd->type == CMD_CREATE) {
    result = execute_create_table(ctx, cmd);
  }

  // free(order->steps);
  // free(order);

  return result;
}

ExecutionResult execute_create_table(Context* ctx, JQLCommand* cmd) {
  if (!ctx || !ctx->writer) {
    return (ExecutionResult){1, "Database not initialized"};
  }
  
  char schema[512] = {0};
  strcat(schema, " (");
  for (int i = 0; i < cmd->column_count; i++) {
    strcat(schema, cmd->columns[i]);
    if (i < cmd->column_count - 1) strcat(schema, ", ");
  }
  strcat(schema, ")");

  io_write_metadata(ctx->writer, cmd->table, schema);

  return (ExecutionResult){0, "Table created successfully"};
}
