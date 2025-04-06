#ifndef IO_H
#define IO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <dirent.h>

#ifdef _WIN32
  #include <windows.h>
  #include <stdio.h>

  #define statfs(path, buf) GetDiskFreeSpaceEx(path, NULL, NULL, (PULARGE_INTEGER)buf)

  struct statfs {
    unsigned long long f_bsize;
  };
#else
  #include <sys/stat.h>
  #include <sys/statfs.h>
  #include <sys/types.h>
  #include <unistd.h>
#endif

#include "../utils/log.h"

static void clear_screen() {
  #ifdef __unix__
    system("clear");
  #endif

  #ifdef _WIN32
    system("cls");
  #endif
}

static int create_directory(char* path) {
  #if defined(_WIN32) || defined(_WIN64)
    struct _stat st = {0};
    if (_stat(path, &st) == -1) {
      return _mkdir(path);  
    }
  #else
    struct stat st = {0};
    if (stat(path, &st) == -1) {
      return mkdir(path, 0700); 
    }
  #endif
  return 0;
}

static void create_file(char* file_path) {
  FILE *file = fopen(file_path, "r");
  if (file) {
    fclose(file);
  } else {
    file = fopen(file_path, "w");
    if (file) {
      fclose(file); 
    } else {
      LOG_ERROR("Failed to create file \n\t > %s\n", file_path);
    }
  }
}

static bool directory_exists(const char* path) {
  struct stat stat_buf;
  return (stat(path, &stat_buf) == 0 && S_ISDIR(stat_buf.st_mode));
}

static bool file_exists(const char* path) {
  struct stat stat_buf;
  return (stat(path, &stat_buf) == 0 && S_ISREG(stat_buf.st_mode));
}

#define MAX_TABLE_NAME 32
#define MAX_SCHEMA 128

typedef enum { IO_READ, IO_WRITE, IO_APPEND } IOMode;

typedef struct IO {
  FILE* file;
  char* buffer;
  size_t buf_size;
  size_t buf_capacity;
} IO;

typedef struct {
  char table_name[MAX_TABLE_NAME];
  char schema[MAX_SCHEMA];
  long root_offset;  
} TableMetadata;

IO* io_init(const char* filename, IOMode mode, size_t buffer_capacity);
void io_close(IO* io);
void io_flush(IO* io);
void io_write(IO* io, const void* data, size_t size);
size_t io_read(IO* io, void* buffer, size_t size);
void io_seek(IO* io, long offset, int whence);
void io_seek_write(IO* io, long offset, const void* data, size_t size, int whence);
long io_tell(IO* io);
void io_clear_buffer(IO* io);

TableMetadata* io_read_metadata(IO* io, const char* table_name);

int io_write_record(IO* io, int key, void* record, size_t record_size);
int io_read_record(IO* io, int key, void* buffer, size_t record_size);

#endif // IO_H
