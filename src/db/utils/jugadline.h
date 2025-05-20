#ifndef JUGADLINE_H
#define JUGADLINE_H 

#include "storage/fs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#define MAX_HISTORY 10
#define MAX_CMD_LENGTH 1024
#define MAX_COMPLETIONS 100

typedef struct {
  char *history[MAX_HISTORY];
  int current;
  int size;
} CommandHistory;

// Get terminal width
int get_terminal_width() {
  struct winsize w;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
  return w.ws_col;
}

char getch() {
  struct termios oldt, newt;
  char ch;
  tcgetattr(STDIN_FILENO, &oldt);
  newt = oldt;
  newt.c_lflag &= ~(ICANON | ECHO);
  tcsetattr(STDIN_FILENO, TCSANOW, &newt);
  ch = getchar();
  tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
  return ch;
}

void add_to_history(CommandHistory *history, const char *cmd) {
  if (strlen(cmd) == 0) {
    return;
  }
  
  if (history->size < MAX_HISTORY) {
    history->history[history->size] = strdup(cmd);
    history->size++;
  } else {
    free(history->history[0]);
    for (int i = 1; i < MAX_HISTORY; i++) {
      history->history[i - 1] = history->history[i];
    }
    history->history[MAX_HISTORY - 1] = strdup(cmd);
  }
}

// Simple function to redraw the command line
void redraw_command_line(const char *prefix, const char *cmd, int cursor_pos) {
  // Clear the current line from the beginning
  printf("\r\033[K");
  
  // Print the prefix and command
  printf("%s%s", prefix, cmd);
  
  // Move cursor to position
  if (cursor_pos < strlen(cmd)) {
    printf("\033[%dD", (int)(strlen(cmd) - cursor_pos));
  }
  
  fflush(stdout);
}

int is_directory(const char *path) {
  struct stat statbuf;
  if (stat(path, &statbuf) != 0) {
    return 0;
  }
  return S_ISDIR(statbuf.st_mode);
}

char* find_word_start(char *cmd, int cursor_pos) {
  int i = cursor_pos - 1;
  while (i >= 0 && cmd[i] != ' ' && cmd[i] != '/') {
    i--;
  }
  return &cmd[i + 1];
}

char* get_path_prefix(char *cmd, int cursor_pos) {
  char *result = malloc(MAX_PATH_LENGTH);
  if (!result) return NULL;
  
  int word_start = 0;
  int path_start = cursor_pos - 1;
  
  while (path_start >= 0 && cmd[path_start] != ' ') {
    path_start--;
  }
  word_start = path_start + 1;
  
  int last_slash = -1;
  for (int i = word_start; i < cursor_pos; i++) {
    if (cmd[i] == '/') {
      last_slash = i;
    }
  }
  
  if (last_slash == -1) {
    strcpy(result, ".");
    return result;
  }
  
  int len = last_slash - word_start;
  if (len == 0) {
    strcpy(result, "/");
  } else {
    strncpy(result, &cmd[word_start], len);
    result[len] = '\0';
  }
  
  return result;
}

char* get_partial_name(char *cmd, int cursor_pos) {
  char *result = malloc(MAX_PATH_LENGTH);
  if (!result) return NULL;
  
  int path_start = cursor_pos - 1;
  while (path_start >= 0 && cmd[path_start] != ' ') {
    path_start--;
  }
  path_start++;
  
  int last_slash = path_start - 1;
  for (int i = path_start; i < cursor_pos; i++) {
    if (cmd[i] == '/') {
      last_slash = i;
    }
  }
  
  int partial_start = last_slash + 1;
  int len = cursor_pos - partial_start;
  strncpy(result, &cmd[partial_start], len);
  result[len] = '\0';
  
  return result;
}

int find_completions(const char *dir_path, const char *partial, char **completions) {
  DIR *dir;
  struct dirent *entry;
  int count = 0;
  
  dir = opendir(dir_path);
  if (!dir) {
    return 0;
  }
  
  while ((entry = readdir(dir)) != NULL && count < MAX_COMPLETIONS) {
    if (strncmp(entry->d_name, partial, strlen(partial)) == 0) {
      completions[count] = malloc(strlen(entry->d_name) + 2);
      if (completions[count]) {
        strcpy(completions[count], entry->d_name);
        
        char full_path[MAX_PATH_LENGTH];
        if (strcmp(dir_path, "/") == 0) {
          snprintf(full_path, MAX_PATH_LENGTH, "/%s", entry->d_name);
        } else if (strcmp(dir_path, ".") == 0) {
          snprintf(full_path, MAX_PATH_LENGTH, "%s", entry->d_name);
        } else {
          snprintf(full_path, MAX_PATH_LENGTH, "%s/%s", dir_path, entry->d_name);
        }
        
        if (is_directory(full_path)) {
          strcat(completions[count], "/");
        }
        
        count++;
      }
    }
  }
  
  closedir(dir);
  return count;
}

void handle_tab_completion(char *cmd, int *cursor_pos, int *cmd_len, const char *prefix) {
  char *dir_path = get_path_prefix(cmd, *cursor_pos);
  char *partial = get_partial_name(cmd, *cursor_pos);
  
  char *completions[MAX_COMPLETIONS];
  int completion_count = find_completions(dir_path, partial, completions);
  
  if (completion_count == 0) {
    free(dir_path);
    free(partial);
    return;
  }
  
  if (completion_count == 1) {
    int word_start = *cursor_pos - strlen(partial);
    
    int chars_to_add = strlen(completions[0]) - strlen(partial);
    if (*cmd_len + chars_to_add >= MAX_CMD_LENGTH) {
      printf("\nCommand too long for completion");
      free(dir_path);
      free(partial);
      free(completions[0]);
      return;
    }
    
    memmove(&cmd[word_start + strlen(completions[0])], 
            &cmd[*cursor_pos], 
            *cmd_len - *cursor_pos + 1);
    
    memcpy(&cmd[word_start], completions[0], strlen(completions[0]));
    *cursor_pos = word_start + strlen(completions[0]);
    *cmd_len = *cmd_len + chars_to_add;
    
    redraw_command_line(prefix, cmd, *cursor_pos);
  } else {
    printf("\n");
    
    // Calculate the maximum length of completions
    size_t max_len = 0;
    for (int i = 0; i < completion_count; i++) {
      size_t len = strlen(completions[i]);
      if (len > max_len) max_len = len;
    }
    max_len += 2; // Add spacing
    
    // Calculate how many columns we can fit
    int term_width = get_terminal_width();
    int cols = term_width / max_len;
    if (cols == 0) cols = 1;
    
    // Print completions in columns
    for (int i = 0; i < completion_count; i++) {
      printf("%-*s", (int)max_len, completions[i]);
      if ((i + 1) % cols == 0 || i == completion_count - 1) {
        printf("\n");
      }
    }
    
    // Reprint prompt and command
    printf("%s%s", prefix, cmd);
    
    // Place cursor at the right position
    if (*cursor_pos < strlen(cmd)) {
      printf("\033[%dD", (int)(strlen(cmd) - *cursor_pos));
    }
    
    fflush(stdout);
  }
  
  for (int i = 0; i < completion_count; i++) {
    free(completions[i]);
  }
  
  free(dir_path);
  free(partial);
}

char* jugadline(CommandHistory *history, char* prefix) {
  char *cmd = calloc(MAX_CMD_LENGTH, sizeof(char));
  if (cmd == NULL) {
    return NULL;
  }
  
  int cursor_pos = 0;
  int cmd_len = 0;
  char ch;
  int history_pos = history->size;
  char *temp_cmd = NULL;
  
  // Print initial prompt
  printf("%s", prefix);
  fflush(stdout);
  
  while (1) {
    ch = getch();
    
    if (ch == 127 || ch == 8) { // Backspace
      if (cursor_pos > 0) {
        memmove(&cmd[cursor_pos-1], &cmd[cursor_pos], cmd_len - cursor_pos + 1);
        cursor_pos--;
        cmd_len--;
        
        // For long commands, simply clear entire line and reprint
        redraw_command_line(prefix, cmd, cursor_pos);
      }
    } else if (ch == 9) { // Tab
      handle_tab_completion(cmd, &cursor_pos, &cmd_len, prefix);
    } else if (ch == 27) { // Escape sequence
      ch = getch();
      if (ch == 91) {
        ch = getch();
        
        if (ch == 68) { // Left arrow
          if (cursor_pos > 0) {
            cursor_pos--;
            // Move cursor left
            printf("\033[1D");
            fflush(stdout);
          }
        } else if (ch == 67) { // Right arrow
          if (cursor_pos < cmd_len) {
            cursor_pos++;
            // Move cursor right
            printf("\033[1C");
            fflush(stdout);
          }
        } else if (ch == 65) { // Up arrow
          if (history_pos > 0 && history->size > 0) {
            if (history_pos == history->size && cmd_len > 0) {
              temp_cmd = strdup(cmd);
            }
            
            history_pos--;
            strncpy(cmd, history->history[history_pos], MAX_CMD_LENGTH - 1);
            cmd_len = strlen(cmd);
            cursor_pos = cmd_len;
            redraw_command_line(prefix, cmd, cursor_pos);
          }
        } else if (ch == 66) { // Down arrow
          if (history_pos < history->size - 1) {
            history_pos++;
            strncpy(cmd, history->history[history_pos], MAX_CMD_LENGTH - 1);
            cmd_len = strlen(cmd);
            cursor_pos = cmd_len;
            redraw_command_line(prefix, cmd, cursor_pos);
          } else if (history_pos == history->size - 1) {
            history_pos = history->size;
            if (temp_cmd) {
              strncpy(cmd, temp_cmd, MAX_CMD_LENGTH - 1);
              cmd_len = strlen(cmd);
              cursor_pos = cmd_len;
              redraw_command_line(prefix, cmd, cursor_pos);
            } else {
              cmd[0] = '\0';
              cmd_len = 0;
              cursor_pos = 0;
              redraw_command_line(prefix, cmd, cursor_pos);
            }
          }
        }
      }
    } else if (ch == 10 || ch == 13) { // Enter key
      cmd[cmd_len] = '\0';
      printf("\n");
      if (cmd_len > 0) {
        add_to_history(history, cmd);
      }
      
      if (temp_cmd) {
        free(temp_cmd);
      }
      
      return cmd;
    } else { // Regular character
      if (cmd_len < MAX_CMD_LENGTH - 1) {
        memmove(&cmd[cursor_pos+1], &cmd[cursor_pos], cmd_len - cursor_pos + 1);
        cmd[cursor_pos] = ch;
        cursor_pos++;
        cmd_len++;
        
        // For a simple approach, clear and reprint everything
        redraw_command_line(prefix, cmd, cursor_pos);
      }
    }
  }
}

#endif // JUGADLINE_H