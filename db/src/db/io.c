#include "io.h"

IO* io_init(const char* filename, IOMode mode, size_t buffer_capacity) {
  IO* io = malloc(sizeof(IO));
  if (!io) exit(EXIT_FAILURE);

  const char* mode_str = (mode == IO_READ) ? "rb" : (mode == IO_WRITE) ? "wb" : "ab";
  io->file = fopen(filename, mode_str);
  if (!io->file) {
    free(io);
    return NULL;
  }

  io->buffer = malloc(buffer_capacity);
  if (!io->buffer) {
    fclose(io->file);
    free(io);
    exit(EXIT_FAILURE);
  }

  io->buf_size = 0;
  io->buf_capacity = buffer_capacity;
  return io;
}

void io_close(IO* io) {
  if (!io) return;

  io_flush(io);
  fclose(io->file);
  free(io->buffer);
  free(io);
}

void io_flush(IO* io) {
  if (io->buf_size > 0) {
    fwrite(io->buffer, io->buf_size, 1, io->file);
    io->buf_size = 0;
    fflush(io->file);
  }
}

void io_write(IO* io, const void* data, size_t size) {
  if (io->buf_size + size > io->buf_capacity) {
    fwrite(io->buffer, io->buf_size, 1, io->file);
    io->buf_size = 0;
  }

  memcpy(io->buffer + io->buf_size, data, size);
  io->buf_size += size;
}

size_t io_read(IO* io, void* buffer, size_t size) {
  return fread(buffer, 1, size, io->file);
}

TableMetadata* io_read_metadata(IO* io, const char* table_name) {
  fseek(io->file, 0, SEEK_SET);
  
  TableMetadata* meta = malloc(sizeof(TableMetadata));
  while (fread(meta, sizeof(TableMetadata), 1, io->file)) {
    if (strcmp(meta->table_name, table_name) == 0) {
      return meta;
    }
  }

  free(meta);
  return NULL;
}

int io_write_record(IO* io, int key, void* record, size_t record_size) {
  fseek(io->file, 0, SEEK_END);
  long offset = ftell(io->file);

  if (fwrite(&key, sizeof(int), 1, io->file) != 1) return 0;
  if (fwrite(record, record_size, 1, io->file) != 1) return 0;

  io_flush(io);
  return 1;
}

int io_read_record(IO* io, int key, void* buffer, size_t record_size) {
  fseek(io->file, sizeof(TableMetadata), SEEK_SET);
  
  int record_key;
  while (fread(&record_key, sizeof(int), 1, io->file)) {
    if (record_key == key) {
      return fread(buffer, record_size, 1, io->file) == 1;
    }
    fseek(io->file, record_size, SEEK_CUR);
  }
  return 0;
}

void btree_insert(IO* io, TableMetadata* meta, int key, long record_offset) {
  fseek(io->file, meta->root_offset, SEEK_SET);
  
  BTreeNode node;
  fread(&node, sizeof(BTreeNode), 1, io->file);

  if (node.num_keys < BTREE_ORDER - 1) {
    node.keys[node.num_keys] = key;
    node.children[node.num_keys] = record_offset;
    node.num_keys++;

    fseek(io->file, meta->root_offset, SEEK_SET);
    fwrite(&node, sizeof(BTreeNode), 1, io->file);
    io_flush(io);
  }
}

long btree_search(IO* io, TableMetadata* meta, int key) {
  fseek(io->file, meta->root_offset, SEEK_SET);
  
  BTreeNode node;
  fread(&node, sizeof(BTreeNode), 1, io->file);

  for (int i = 0; i < node.num_keys; i++) {
    if (node.keys[i] == key) {
      return node.children[i];
    }
  }
  return -1;
}
