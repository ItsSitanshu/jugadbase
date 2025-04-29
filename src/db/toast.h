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

typedef struct Context Context;

bool toast_create(Context* ctx);
uint32_t toast_new_entry(Context* ctx, const char* data);
char* toast_concat(Context* ctx, uint32_t toast_id);
bool toast_delete(Context* ctx, uint32_t toast_id);

ToastChunks* toast_split_entry(const char* data);
void toast_free_chunks(ToastChunks* chunks);

#endif // TOAST_H