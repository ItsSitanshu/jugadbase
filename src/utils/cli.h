#ifndef CLI_H
#define CLI_H

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "executor.h"

#include "../utils/log.h"

void get_short_cwd(char* buffer, size_t size) {
  char cwd[1024];
  if (getcwd(cwd, sizeof(cwd)) != NULL) {
    char* last_slash = strrchr(cwd, '/');
    if (last_slash && last_slash != cwd) {
      *last_slash = '\0';
      char* second_last_slash = strrchr(cwd, '/');
      snprintf(buffer, size, "%s/%s", 
        second_last_slash ? second_last_slash + 1 : cwd, 
        last_slash + 1);
    } else {
      snprintf(buffer, size, "%s", last_slash ? last_slash + 1 : cwd);
    }
  } else {
    snprintf(buffer, size, "unknown");
  }
}

char* format_text_table(ExecutionResult result, JQLCommand* cmd) {
  if (result.row_count == 0) {
    return strdup("(0 rows)\n");
  }

  size_t* col_widths = calloc(cmd->schema->column_count, sizeof(size_t));
  
  for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
    col_widths[c] = strlen(cmd->schema->columns[c].name);
  }
  
  char** formatted_values = calloc(result.row_count * cmd->schema->column_count, sizeof(char*));
  
  for (uint32_t i = 0; i < result.row_count; i++) {
    Row* row = &result.rows[i];
    if (is_struct_zeroed(row, sizeof(Row))) {
      continue;
    }
    
    for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
      ColumnValue val = row->values[c];
      
      char buffer[256] = {0};
      sprintf_column_value(&val, buffer);
      
      formatted_values[i * cmd->schema->column_count + c] = strdup(buffer);
      
      size_t len = strlen(buffer);
      if (len > col_widths[c]) {
        col_widths[c] = len;
      }
    }
  }
  
  size_t total_width = 0;
  for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
    total_width += col_widths[c] + 3; // width + padding + separator
  }
  
  total_width += 6; // " XXX |"
  
  size_t total_size = (total_width + 1) * 2 + (total_width + 1) * result.row_count + 50;
  
  char* output = malloc(total_size);
  if (!output) {
    for (uint32_t i = 0; i < result.row_count * cmd->schema->column_count; i++) {
      if (formatted_values[i] != NULL) {
        free(formatted_values[i]);
      }
    }
    free(formatted_values);
    free(col_widths);
    return NULL;
  }
  
  char* ptr = output;
  size_t remaining = total_size;
  
  int written = snprintf(ptr, remaining, " id |");
  ptr += written;
  remaining -= written;
  
  for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
    written = snprintf(ptr, remaining, " %-*s |", 
                      (int)col_widths[c], cmd->schema->columns[c].name);
    ptr += written;
    remaining -= written;
  }
  
  written = snprintf(ptr, remaining, "\n");
  ptr += written;
  remaining -= written;
  
  written = snprintf(ptr, remaining, "----+");
  ptr += written;
  remaining -= written;
  
  for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
    for (size_t i = 0; i < col_widths[c] + 2; i++) {
      written = snprintf(ptr, remaining, "-");
      ptr += written;
      remaining -= written;
    }
    written = snprintf(ptr, remaining, "+");
    ptr += written;
    remaining -= written;
  }
  
  written = snprintf(ptr, remaining, "\n");
  ptr += written;
  remaining -= written;
  
  for (uint32_t i = 0; i < result.row_count; i++) {
    Row* row = &result.rows[i];
    if (is_struct_zeroed(row, sizeof(Row))) {
      continue;
    }
    
    written = snprintf(ptr, remaining, "%3u |", row->id.row_id);
    ptr += written;
    remaining -= written;
    
    for (uint8_t c = 0; c < cmd->schema->column_count; c++) {
      char* value = formatted_values[i * cmd->schema->column_count + c];
      
      if (cmd->schema->columns[c].type == TOK_T_INT || 
          cmd->schema->columns[c].type == TOK_T_DOUBLE || 
          cmd->schema->columns[c].type == TOK_T_FLOAT || 
          cmd->schema->columns[c].type == TOK_T_UINT || 
          cmd->schema->columns[c].type == TOK_T_SERIAL) {
        written = snprintf(ptr, remaining, " %*s |", (int)col_widths[c], value);
      } else {
        written = snprintf(ptr, remaining, " %-*s |", (int)col_widths[c], value);
      }
      ptr += written;
      remaining -= written;
    }
    
    written = snprintf(ptr, remaining, "\n");
    ptr += written;
    remaining -= written;
  }
  
  written = snprintf(ptr, remaining, "(%u %s)\n", 
                    result.row_count, result.row_count == 1 ? "row" : "rows");
  ptr += written;
  remaining -= written;
  
  for (uint32_t i = 0; i < result.row_count * cmd->schema->column_count; i++) {
    if (formatted_values[i] != NULL) {
      free(formatted_values[i]);
    }
  }
  free(formatted_values);
  free(col_widths);
  
  return output;
}

void print_text_table(ExecutionResult result, JQLCommand* cmd) {
  char* formatted_table = format_text_table(result, cmd);
  if (formatted_table) {
    free(formatted_table);
  } else {
    printf("Error formatting table output\n");
  }
}

void print_text_table_to_file(ExecutionResult result, JQLCommand* cmd, const char* filename) {
  char* formatted_table = format_text_table(result, cmd);
  if (formatted_table) {
    FILE* fp = fopen(filename, "w");
    if (fp) {
      fprintf(fp, "%s", formatted_table);
      fclose(fp);
    } else {
      printf("Error opening file: %s\n", filename);
    }
    free(formatted_table);
  } else {
    printf("Error formatting table output\n");
  }
}

#endif // CLI_H 