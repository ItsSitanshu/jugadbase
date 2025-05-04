#ifndef TOAST_H
#define TOAST_H

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define TOAST_CHUNK_SIZE 2048

typedef struct {
  char** chunks;
  size_t count;
} ToastChunks;

typedef struct Database Database;

bool toast_create(Database* db);
uint32_t toast_new_entry(Database* db, const char* data);
char* toast_concat(Database* db, uint32_t toast_id);
bool toast_delete(Database* db, uint32_t toast_id);

ToastChunks* toast_split_entry(const char* data);
void toast_free_chunks(ToastChunks* chunks);

#endif // TOAST_H