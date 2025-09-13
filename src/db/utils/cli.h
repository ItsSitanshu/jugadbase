#ifndef CLI_H
#define CLI_H

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "kernel/kernel.h"

#include "utils/log.h"
#include "utils/xmem.h"


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
    return xstrdup("(0 rows)\n");
  }

  TableSchema* schema = cmd->schema;
  ColumnDefinition* columns = schema->columns;
  uint8_t col_c = schema->column_count;

  size_t* col_widths = xcalloc(col_c, sizeof(size_t));
  
  for (uint8_t c = 0; c < col_c; c++) {
    col_widths[c] = strlen(columns[c].name);
  }
  
  char** formatted_values = xcalloc(result.row_count * col_c, sizeof(char*));
  
  for (uint32_t i = 0; i < result.row_count; i++) {
    Row* row = &result.rows[i];
    if (is_struct_zeroed(row, sizeof(Row))) {
      continue;
    }
    
    for (uint8_t c = 0; c < col_c; c++) {
      ColumnValue val = row->values[c];
      
      char* buffer;
      format_column_value(buffer, 256, &val);
      
      formatted_values[i * col_c + c] = xstrdup(buffer);
      
      size_t len = strlen(buffer);
      if (len > col_widths[c]) {
        col_widths[c] = len;
      }
    }
  }
  
  size_t total_width = 0;
  for (uint8_t c = 0; c < col_c; c++) {
    total_width += col_widths[c] + 3; // width + padding + separator
  }
  
  total_width += 6; // " XXX |"
  
  size_t total_size = (total_width + 1) * 2 + (total_width + 1) * result.row_count + 50;
  
  char* output = xmalloc(total_size);
  if (!output) {
    for (uint32_t i = 0; i < result.row_count * col_c; i++) {
      if (formatted_values[i] != NULL) {
        xfree(formatted_values[i]);
      }
    }
    xfree(formatted_values);
    xfree(col_widths);
    return NULL;
  }
  
  char* ptr = output;
  size_t remaining = total_size;
  
  int written = snprintf(ptr, remaining, " id |");
  ptr += written;
  remaining -= written;
  
  for (uint8_t c = 0; c < col_c; c++) {
    written = snprintf(ptr, remaining, " %-*s |", 
                      (int)col_widths[c], columns[c].name);
    ptr += written;
    remaining -= written;
  }
  
  written = snprintf(ptr, remaining, "\n");
  ptr += written;
  remaining -= written;
  
  written = snprintf(ptr, remaining, "----+");
  ptr += written;
  remaining -= written;
  
  for (uint8_t c = 0; c < col_c; c++) {
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
    
    for (uint8_t c = 0; c < col_c; c++) {
      char* value = formatted_values[i * col_c + c];
      
      if (columns[c].type == TOK_T_INT || 
          columns[c].type == TOK_T_DOUBLE || 
          columns[c].type == TOK_T_FLOAT || 
          columns[c].type == TOK_T_UINT || 
          columns[c].type == TOK_T_SERIAL) {
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
  
  for (uint32_t i = 0; i < result.row_count * col_c; i++) {
    if (formatted_values[i] != NULL) {
      xfree(formatted_values[i]);
    }
  }
  xfree(formatted_values);
  xfree(col_widths);
  
  return output;
}

void print_text_table(ExecutionResult result, JQLCommand* cmd) {
  char* formatted_table = format_text_table(result, cmd);
  if (formatted_table) {
    xfree(formatted_table);
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
    xfree(formatted_table);
  } else {
    printf("Error formatting table output\n");
  }
}

#endif // CLI_H 