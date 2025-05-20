#ifndef FILE_H
#define FILE_H

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

#elif defined(__APPLE__)
  #include <sys/param.h>
  #include <sys/mount.h>
  #include <sys/types.h>
  #include <unistd.h>

#else  // Linux and others
  #include <sys/types.h>
  #include <sys/stat.h>
  #include <sys/statfs.h>
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
      int result = _mkdir(path);
      if (result != 0) perror("mkdir");
      return result;
    }
  #else
    struct stat st = {0};
    if (stat(path, &st) == -1) {
      int result = mkdir(path, 0700);
      if (result != 0) perror("mkdir");
      return result;
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

static size_t eof_fread(void *ptr, size_t size, size_t count, FILE *file, bool *is_invalid) {
  size_t result = fread(ptr, size, count, file);
  if (result < count) {
    *is_invalid = true;
  }
  return result;
}

static bool is_struct_zeroed(const void* ptr, size_t size) {
  if (ptr == NULL) {
    return true;
  }
  
  void* zeroed_struct = malloc(size);
  memset(zeroed_struct, 0, size);

  int result = memcmp(ptr, zeroed_struct, size);
  
  free(zeroed_struct); 
  return result == 0;
}



#define MAX_TABLE_NAME 32
#define MAX_SCHEMA 128

typedef enum { FILE_READ, FILE_WRITE, FILE_APPEND } FILEMode;

FILE* io_init(const char* filename, FILEMode mode, size_t buffer_capacity);
void io_close(FILE* file);
void io_flush(FILE* file);
void io_write(FILE* file, const void* data, size_t size);
size_t io_read(FILE* file, void* buffer, size_t size);
void io_seek(FILE* file, long offset, int whence);
void io_seek_write(FILE* file, long offset, const void* data, size_t size, int whence);
long io_tell(FILE* file);

#endif // FILE_H
