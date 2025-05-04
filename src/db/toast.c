#include "toast.h"
#include "executor.h"

bool toast_create(Database* db) {
  if (!db) {
    return false;
  }

  char* buffer = "CREATE TABLE IF NOT EXISTS jb_toast ("
    "id SERIAL, "
    "seq SERIAL, "
    "data TEXT"
    ");"; 

  Result res = process(db, buffer);
  
  LOG_INFO("Successfully loaded JB_TOAST for large attributes");

  return true;
}

uint32_t toast_new_entry(Database* db, const char* data) {
  if (!db || !data) {
    return 0;
  }

  size_t data_size = strlen(data);

  ToastChunks* chunks = toast_split_entry(data);
  if (!chunks) {
    return 0;
  } 

  Result id_query = process(db, "SELECT id FROM jb_toast ORDER BY id");
  uint32_t id = (id_query.exec.row_count > 0) ?
    (id_query.exec.rows[0].values[0].int_value + 1)
    : 0;

  for (size_t i = 0; i < chunks->count; ++i) {
    char insert_query[2200];
    snprintf(insert_query, sizeof(insert_query),
      "INSERT INTO jb_toast (id, seq, data) VALUES (%d, %zu, \"%s\");",
      id, i, chunks->chunks[i]);
    process(db, insert_query);
  }

  toast_free_chunks(chunks);
  return id;
}

char* toast_concat(Database* db, uint32_t toast_id) {
  char query[128];
  snprintf(query, sizeof(query),
           "SELECT data FROM jb_toast WHERE id = %u ORDER BY seq;", toast_id);
    
  Result res = process(db, query);
  if (res.exec.code != 0 || res.exec.row_count == 0) {
    return NULL;
  }

  size_t total_len = 0;
  for (size_t i = 0; i < res.exec.row_count; ++i) {
    total_len += strlen(res.exec.rows[i].values[2].str_value);
  }

  char* full_text = malloc(total_len + 1);
  if (!full_text) return NULL;

  full_text[0] = '\0';
  for (size_t i = 0; i < res.exec.row_count; ++i) {
    strcat(full_text, res.exec.rows[i].values[2].str_value);
  }

  return full_text;
}

bool toast_delete(Database* db, uint32_t toast_id) {
  char query[128];
  snprintf(query, sizeof(query),
           "DELETE FROM jb_toast WHERE id = %u;", toast_id);

  Result res = process(db, query);
  if (res.exec.code != 0) {
    LOG_ERROR("Failed to delete TOAST entry with id %u", toast_id);
    return false;
  }

  LOG_INFO("Deleted TOAST entry with id %u", toast_id);
  return true;
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