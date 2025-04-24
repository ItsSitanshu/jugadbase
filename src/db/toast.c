#include "toast.h"
#include "executor.h"

bool toast_create(Context* ctx) {
  if (!ctx) {
    return false;
  }

  char* buffer = "CREATE TABLE IF NOT EXISTS jb_toast ("
    "id SERIAL, "
    "seq SERIAL, "
    "data TEXT"
    ");"; 

  Result res = process(ctx, buffer);
  
  LOG_INFO("Successfully loaded JB_TOAST for large attributes");

  return true;
}

uint32_t toast_new_entry(Context* ctx, const char* data) {
  if (!ctx || !data) {
    return 0;
  }

  size_t data_size = strlen(data);

  ToastChunks* chunks = toast_split_entry(data);
  if (!chunks) {
    return 0;
  } 

  Result id_query = process(ctx, "SELECT id FROM jb_toast ORDER BY id");
  uint32_t id = (id_query.exec.row_count > 0) ?
    (id_query.exec.rows[0].values[0].int_value + 1)
    : 0;

  for (size_t i = 0; i < chunks->count; ++i) {
    char insert_query[2200];
    snprintf(insert_query, sizeof(insert_query),
      "INSERT INTO jb_toast (id, seq, data) VALUES (%d, %zu, \"%s\");",
      id, i, chunks->chunks[i]);
    process(ctx, insert_query);
  }

  toast_free_chunks(chunks);
  return id;
}

ToastChunks* toast_split_entry(const char* data) {
  if (!data) return NULL;

  size_t length = strlen(data);
  size_t num_chunks = abs((length / (TOAST_CHUNK_SIZE - 1)) + 1);

  ToastChunks* result = malloc(sizeof(ToastChunks));
  if (!result) return NULL;

  result->chunks = malloc(sizeof(char*) * num_chunks);
  result->count = num_chunks;


  for (size_t i = 0; i < num_chunks; ++i) {
    size_t start = i * (TOAST_CHUNK_SIZE - 1);
    size_t chunk_len = ((length - start) < (TOAST_CHUNK_SIZE - 1)) ? (length - start) : (TOAST_CHUNK_SIZE - 1);

    result->chunks[i] = malloc(chunk_len + 1); // +1 for null terminator
    if (!result->chunks[i]) {
      for (size_t j = 0; j < i; ++j) free(result->chunks[j]);
      free(result->chunks);
      free(result);
      return NULL;
    }

    strncpy(result->chunks[i], data + start, chunk_len);
    result->chunks[i][chunk_len] = '\0';
  }

  return result;
}

void toast_free_chunks(ToastChunks* chunks) {
  if (!chunks) return;
  for (size_t i = 0; i < chunks->count; ++i) {
    free(chunks->chunks[i]);
  }
  free(chunks->chunks);
  free(chunks);
}