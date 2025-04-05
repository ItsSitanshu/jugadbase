#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <unistd.h>

#define MAX_HISTORY 10
#define MAX_CMD_LENGTH 100

// Structure to store command history
typedef struct {
    char *history[MAX_HISTORY];
    int current;
    int size;
} CommandHistory;

// Function to handle reading a single key press
char getch() {
    struct termios oldt, newt;
    char ch;
    tcgetattr(STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO); // disable canonical mode and echo
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    ch = getchar();
    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    return ch;
}

void add_to_history(CommandHistory *history, const char *cmd) {
    if (history->size < MAX_HISTORY) {
        history->history[history->size] = strdup(cmd);
    } else {
        free(history->history[0]);
        for (int i = 1; i < MAX_HISTORY; i++) {
            history->history[i - 1] = history->history[i];
        }
        history->history[MAX_HISTORY - 1] = strdup(cmd);
    }

    history->size++;
}

void clear_line() {
    printf("\r\033[K");
}

char* jugadline(CommandHistory *history, char* prefix) {
    char *cmd = malloc(MAX_CMD_LENGTH);
    int index = 0;
    char ch;

    while (1) {
        ch = getch();
        
        if (ch == 127) {
            if (index > 0) {
                index--;
                printf("\b \b"); 
            }
        }
        else if (ch == 27) { 
            ch = getch(); 
            if (ch == 91) {
                ch = getch(); 
                

                if (ch == 65 && history->current != 0) { 
                    history->current--;
                    clear_line(); 
                    printf("%s%s", prefix, history->history[history->current]);
                    index = strlen(history->history[history->current]);
                    fflush(stdout);
                    cmd = strdup(history->history[history->current]);
                } else if (ch == 66) { 
                    if (history->current < history->size - 1) {
                        history->current++;
                        clear_line(); 
                        printf("%s%s", prefix, history->history[history->current]);
                        index = strlen(history->history[history->current ]);
                        fflush(stdout);
                        cmd = strdup(history->history[history->current]);
                    } else {
                        history->current = history->size;
                        clear_line();
                        printf("%s", prefix);
                        fflush(stdout);
                        index = 0; 
                    }
                }
            }
        } else if (ch == 10 || ch == 13) { 
            cmd[index] = '\0';
            if (strlen(cmd) > 0) {
                add_to_history(history, cmd);
            }
            history->current = history->size;
            break;
        } else {
            if (index < MAX_CMD_LENGTH - 1) {
                cmd[index++] = ch;
                putchar(ch); 
            }
        }
    }
    printf("\n");

    return cmd;
}

