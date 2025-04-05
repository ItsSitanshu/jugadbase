#include "io.h"
#include "../utils/log.h"

IO* io_init(const char* filename, IOMode mode, size_t buffer_capacity) {
  IO* io = malloc(sizeof(IO));
  if (!io) exit(EXIT_FAILURE);

  const char* mode_str = (mode == IO_READ) ? "rb" : (mode == IO_WRITE) ? "r+b" : "ab+";
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

  LOG_INFO("IO initialized for %s, buffer capacity: %zu", (strrchr(filename, '/') + 1), buffer_capacity);
  return io;
}

void io_close(IO* io) {
  if (!io) return;

  io_flush(io);
  fclose(io->file);
  free(io->buffer);
  free(io);
  
  LOG_INFO("IO closed and resources freed");
}

void io_flush(IO* io) {
  if (io->buf_size > 0) {
    fwrite(io->buffer, io->buf_size, 1, io->file);
    io->buf_size = 0;
    fflush(io->file);
    LOG_INFO("Flushed %zu bytes to file", io->buf_size);
  }
}

void io_write(IO* io, const void* data, size_t size) {
  if (io->buf_size + size > io->buf_capacity) {
    fwrite(io->buffer, io->buf_size, 1, io->file);
    io->buf_size = 0;
    LOG_WARN("Buffer overflow, flushed %zu bytes", io->buf_size);
  }

  memcpy(io->buffer + io->buf_size, data, size);
  io->buf_size += size;

  LOG_DEBUG("io_write: wrote %zu bytes, new buffer size: %zu", size, io->buf_size);
}

size_t io_read(IO* io, void* buffer, size_t size) {
  size_t bytes_read = fread(buffer, 1, size, io->file);
  LOG_DEBUG("io_read: read %zu bytes", bytes_read);
  return bytes_read;
}

void io_seek(IO* io, long offset, int whence) {
  if (!io || !io->file) return;

  if (io->buf_size > 0 && whence == SEEK_SET) {
    io_flush(io);
    LOG_DEBUG("Buffer flushed before seek");
  }

  fseek(io->file, offset, whence);
  LOG_DEBUG("io_seek: moved to offset %ld", offset);
}

void io_seek_write(IO* io, long offset, const void* data, size_t size, int whence) {
  io_flush(io);

  if (fseek(io->file, offset, whence) != 0) {
    perror("io_seek_write: seek failed");
    LOG_ERROR("Seek failed at offset %ld", offset);
    exit(1);
  }

  size_t written = fwrite(data, 1, size, io->file);
  if (written != size) {
    perror("io_seek_write: write failed");
    LOG_ERROR("Write failed at offset %ld, expected %zu bytes, wrote %zu bytes", offset, size, written);
    exit(1);
  }

  fflush(io->file);
  LOG_INFO("io_seek_write: wrote %zu bytes at offset %ld", size, offset);
}

long io_tell(IO* io) {
  long pos = ftell(io->file) + io->buf_size;
  LOG_DEBUG("io_tell: current position is %ld", pos);
  return pos;
}

void io_clear_buffer(IO* io) {
  memset(io->buffer, 0, io->buf_capacity);
  io->buf_size = 0;
  LOG_INFO("Buffer cleared");
}

TableMetadata* io_read_metadata(IO* io, const char* table_name) {
  fseek(io->file, 0, SEEK_SET);
  
  TableMetadata* meta = malloc(sizeof(TableMetadata));
  while (fread(meta, sizeof(TableMetadata), 1, io->file)) {
    if (strcmp(meta->table_name, table_name) == 0) {
      LOG_INFO("Found metadata for table: %s", table_name);
      return meta;
    }
  }

  free(meta);
  LOG_WARN("Table metadata not found for table: %s", table_name);
  return NULL;
}

int io_write_record(IO* io, int key, void* record, size_t record_size) {
  fseek(io->file, 0, SEEK_END);
  long offset = ftell(io->file);

  if (fwrite(&key, sizeof(int), 1, io->file) != 1) {
    LOG_ERROR("Failed to write key for record");
    return 0;
  }
  if (fwrite(record, record_size, 1, io->file) != 1) {
    LOG_ERROR("Failed to write record data");
    return 0;
  }

  io_flush(io);
  LOG_INFO("Record with key %d written at offset %ld", key, offset);
  return 1;
}

int io_read_record(IO* io, int key, void* buffer, size_t record_size) {
  fseek(io->file, sizeof(TableMetadata), SEEK_SET);
  
  int record_key;
  while (fread(&record_key, sizeof(int), 1, io->file)) {
    if (record_key == key) {
      size_t bytes_read = fread(buffer, record_size, 1, io->file);
      LOG_INFO("Record with key %d found and read %zu bytes", key, bytes_read);
      return bytes_read == 1;
    }
    fseek(io->file, record_size, SEEK_CUR);
  }
  LOG_WARN("Record with key %d not found", key);
  return 0;
}