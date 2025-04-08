#include "io.h"
#include "../utils/log.h"

FILE* io_init(const char* filename, FILEMode mode, size_t buffer_capacity) {
  FILE* file = fopen(filename, (mode == FILE_READ) ? "rb" : (mode == FILE_WRITE) ? "r+b" : "ab+");

  if (!file) {
    LOG_ERROR("Failed to open file: %s", filename);
    return NULL;
  }

  LOG_INFO("File opened: %s, buffer capacity: %zu", filename, buffer_capacity);
  return file;
}

void io_close(FILE* file) {
  if (!file) return;

  fflush(file);
  fclose(file);

  LOG_INFO("File closed and resources freed");
}

void io_flush(FILE* file) {
  if (file) {
    fflush(file);
    LOG_INFO("Flushed data to file");
  }
}

void io_write(FILE* file, const void* data, size_t size) {
  if (file && data && size > 0) {
    size_t written = fwrite(data, 1, size, file);
    if (written != size) {
      LOG_ERROR("Write failed, expected %zu bytes, wrote %zu bytes", size, written);
    } else {
      LOG_DEBUG("io_write: wrote %zu bytes", size);
    }
  }
}

size_t io_read(FILE* file, void* buffer, size_t size) {
  if (file && buffer) {
    size_t bytes_read = fread(buffer, 1, size, file);
    LOG_DEBUG("io_read: read %zu bytes", bytes_read);
    return bytes_read;
  }
  return 0;
}

void io_seek(FILE* file, long offset, int whence) {
  if (!file) return;

  fseek(file, offset, whence);
  LOG_DEBUG("io_seek: moved to offset %ld", offset);
}

void io_seek_write(FILE* file, long offset, const void* data, size_t size, int whence) {
  if (!file) return;

  fseek(file, offset, whence);
  size_t written = fwrite(data, 1, size, file);

  if (written != size) {
    LOG_ERROR("Write failed, expected %zu bytes, wrote %zu bytes", size, written);
    exit(1);
  }

  fflush(file);
  LOG_INFO("io_seek_write: wrote %zu bytes at offset %ld", size, offset);
}

long io_tell(FILE* file) {
  if (!file) return -1;

  long pos = ftell(file);
  LOG_DEBUG("io_tell: current position is %ld", pos);
  return pos;
}