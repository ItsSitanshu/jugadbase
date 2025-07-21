#ifndef JUGADLINE_H
#define JUGADLINE_H

#define MAX_HISTORY 10
#define MAX_CMD_LENGTH 1024
#define MAX_COMPLETIONS 100

typedef struct {
  char *history[MAX_HISTORY];
  int current;
  int size;
} CommandHistory;

int get_terminal_width(void);
char getch(void);
void add_to_history(CommandHistory *history, const char *cmd);
void redraw_command_line(const char *prefix, const char *cmd, int cursor_pos);
int is_directory(const char *path);
char* find_word_start(char *cmd, int cursor_pos);
char* get_path_prefix(char *cmd, int cursor_pos);
char* get_partial_name(char *cmd, int cursor_pos);
int find_completions(const char *dir_path, const char *partial, char **completions);
void handle_tab_completion(char *cmd, int *cursor_pos, int *cmd_len, const char *prefix);
char* jugadline(CommandHistory *history, char* prefix);
char* get_hidden_input(void);

#endif // JUGADLINE_H
