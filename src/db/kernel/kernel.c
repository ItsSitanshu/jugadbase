#include "kernel/kernel.h"

Result process(Database* db, char* buffer) {
  if (!db || !db->lexer || !db->parser) {
    return (Result){(ExecutionResult){1, "Invalid context"}, NULL};
  }

  lexer_set_buffer(db->lexer, buffer);
  parser_reset(db->parser);

  JQLCommand* cmd = malloc(sizeof(JQLCommand));
  *cmd = parser_parse(db);

  Result result = execute_cmd(db, cmd, true);
  return result;
}

Result process_silent(Database* db, char* buffer) {
  if (!db || !db->lexer || !db->parser) {
    return (Result){(ExecutionResult){1, "Invalid context"}, NULL};
  }

  lexer_set_buffer(db->lexer, buffer);
  parser_reset(db->parser);


  JQLCommand* cmd = malloc(sizeof(JQLCommand));
  *cmd = parser_parse(db);

  Result result = execute_cmd(db, cmd, false);
  return result;
}

Result execute_cmd(Database* db, JQLCommand* cmd, bool show) {
  if (cmd->is_invalid) {
    return (Result){(ExecutionResult){1, "Invalid command"}, NULL};
  }

  Result result = {(ExecutionResult){0, "Execution successful"}, NULL};

  switch (cmd->type) {
    case CMD_CREATE: {
      result = (Result){execute_create_table(db, cmd), cmd};
      break;
    }
    case CMD_ALTER:
      result = (Result){execute_alter_table(db, cmd), cmd};
      break;
    case CMD_INSERT:
      result = (Result){execute_insert(db, cmd), cmd};
      break;
    case CMD_SELECT:
      result = (Result){execute_select(db, cmd), cmd};
      break;
    case CMD_UPDATE:
      result = (Result){execute_update(db, cmd), cmd};
      break;
    case CMD_DELETE:
      result = (Result){execute_delete(db, cmd), cmd};
      break;
    default:
      result = (Result){(ExecutionResult){1, "Unknown command type"}, NULL};
  }

  if (!show) return result;

  LOG_INFO("%s (effected %u rows)", result.exec.message, result.exec.row_count);

  if (result.exec.rows && result.exec.alias_limit > 0 && result.exec.row_count > 0) {
    printf("-> Returned %u row(s):\n", result.exec.row_count);

    for (uint32_t i = 0; i < result.exec.row_count; i++) {
      Row* row = &result.exec.rows[i];
      if (is_struct_zeroed(row, sizeof(Row))) { 
        printf("Slot %u is [nil]\n", i + 1);
        continue;
      }

      printf("Row %u [%u.%u]: ", i + 1, row->id.page_id, row->id.row_id);

      uint8_t alias_count = 0;
      for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
        ColumnDefinition col = cmd->schema->columns[c];
        ColumnValue val = row->values[c];

        // LOG_DEBUG("%d : alias: %s norm: %s", c, result.exec.aliases[alias_count], col.name);

        if (!val.is_null && result.exec.aliases[alias_count]) {
          printf("%s: ", result.exec.aliases[alias_count]);
          alias_count++;
        }
        print_column_value(&val);

        if (alias_count >= result.exec.alias_limit) {
          break;
        }

        if ((c < cmd->value_counts[0] - 1) && (!val.is_null))  {
          printf(", ");
        }
      }

      printf("\n");
    }
  }
  
  return result;
}


void free_row(Row* row) {
  if (!row || !row->values) return;
  for (uint32_t i = 0; i < row->n_values; i++) {
    free_column_value(&row->values[i]);
  }
  free(row->values);
}

void free_execution_result(ExecutionResult* result) {
  if (!result) return;

  if (result->aliases) {
    for (size_t i = 0; i < result->alias_limit; i++) {
      if (result->aliases[i]) {
        free(result->aliases[i]);
      }
    }
    free(result->aliases);
  }

  // if (result->rows && result->alias_limit > 0) {
  //   for (uint32_t i = 0; i < result->row_count; i++) {
  //     free_row(&result->rows[i]);
  //   }
  //   free(result->rows);
  // }
}

void free_result(Result* result) {
  free_execution_result(&result->exec);
  free_jql_command(result->cmd);
}