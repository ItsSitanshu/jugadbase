#ifndef TOAST_H
#define TOAST_H

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define TOAST_CHUNK_SIZE 2048

typedef struct {
  char** chunks;
  size_t count;
} ToastChunks;

typedef struct Context Context;

bool toast_create(Context* ctx);
bool toast_new_entry(Context* ctx, const char* data);

ToastChunks* toast_split_entry(const char* data);
void toast_free_chunks(ToastChunks* chunks);

#endif // TOAST_H