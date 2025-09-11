#ifndef XMEM_H
#define XMEM_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "log.h"

typedef struct {
  size_t malloc_count;
  size_t calloc_count;
  size_t realloc_count;
  size_t free_count;
  size_t memcpy_count;
  size_t memmove_count;
  size_t strdup_count;
  size_t strcpy_count;

  size_t bytes_allocated;  // total allocated bytes
  size_t bytes_freed;      // total freed bytes
} MemStats;

static MemStats mem_stats = {0};

#define xmalloc(sz)      __xmalloc(sz, __FILE__, __LINE__)
#define xcalloc(n, sz)   __xcalloc(n, sz, __FILE__, __LINE__)
#define xrealloc(p, sz)  __xrealloc(p, sz, __FILE__, __LINE__)
#define xfree(p)         __xfree(p, __FILE__, __LINE__)
#define xmemcpy(d,s,n)   __xmemcpy(d,s,n, __FILE__, __LINE__)
#define xmemmove(d,s,n)  __xmemmove(d,s,n, __FILE__, __LINE__)
#define xstrdup(s)       __xstrdup(s, __FILE__, __LINE__)
#define xstrncpy(d,s,n)  __xstrncpy(d,s,n, __FILE__, __LINE__)
#define xstrcpy(d,s)     __xstrcpy(d,s, __FILE__, __LINE__)

static inline void* __xmalloc(size_t sz, const char* file, int line) {
  void* p = malloc(sz);
  if (!p) LOG_FATAL("malloc(%zu) failed at %s:%d", sz, file, line);
  mem_stats.malloc_count++;
  mem_stats.bytes_allocated += sz;
  return p;
}

static inline void* __xcalloc(size_t n, size_t sz, const char* file, int line) {
  void* p = calloc(n, sz);
  if (!p) LOG_FATAL("calloc(%zu, %zu) failed at %s:%d", n, sz, file, line);
  mem_stats.calloc_count++;
  mem_stats.bytes_allocated += n*sz;
  return p;
}

static inline void* __xrealloc(void* old, size_t sz, const char* file, int line) {
  void* p = realloc(old, sz);
  if (!p) LOG_FATAL("realloc(%p, %zu) failed at %s:%d", old, sz, file, line);
  mem_stats.realloc_count++;
  mem_stats.bytes_allocated += sz;
  return p;
}

static inline void __xfree(void* p, const char* file, int line) {
  if (!p) return;
  free(p);
  mem_stats.free_count++;
}

static inline void* __xmemcpy(void* dest, const void* src, size_t n, const char* file, int line) {
  if (!dest || !src) LOG_FATAL("memcpy NULL pointer at %s:%d", file, line);
  mem_stats.memcpy_count++;
  return memcpy(dest, src, n);
}

static inline void* __xmemmove(void* dest, const void* src, size_t n, const char* file, int line) {
  if (!dest || !src) LOG_FATAL("memmove NULL pointer at %s:%d", file, line);
  mem_stats.memmove_count++;
  return memmove(dest, src, n);
}

static inline char* __xstrdup(const char* s, const char* file, int line) {
  if (!s) LOG_FATAL("strdup NULL pointer at %s:%d", file, line);
  mem_stats.strdup_count++;
  char* dup = strdup(s);
  if (!dup) LOG_FATAL("strdup failed at %s:%d", file, line);
  return dup;
}

static inline char* __xstrncpy(char* dest, const char* src, size_t n, const char* file, int line) {
  if (!dest || !src) LOG_FATAL("strncpy NULL pointer at %s:%d", file, line);
  mem_stats.strcpy_count++;
  if (n > 0) {
    strncpy(dest, src, n);
    dest[n-1] = '\0';
  }
  return dest;
}


static inline char* __xstrcpy(char* dest, const char* src, const char* file, int line) {
  if (!dest || !src) LOG_FATAL("strcpy NULL pointer at %s:%d", file, line);
  mem_stats.strcpy_count++;  
  return strcpy(dest, src);
}



static inline void xmem_report(void) {
  fprintf(stderr,
    "\n==== MEMORY REPORT ====\n"
    "malloc    : %zu calls\n"
    "calloc    : %zu calls\n"
    "realloc   : %zu calls\n"
    "free      : %zu calls\n"
    "memcpy    : %zu calls\n"
    "memmove   : %zu calls\n"
    "strdup    : %zu calls\n"
    "strncpy   : %zu calls\n"
    "bytes alloc: %zu\n"
    "bytes freed:  %zu\n"
    "======================\n",
    mem_stats.malloc_count,
    mem_stats.calloc_count,
    mem_stats.realloc_count,
    mem_stats.free_count,
    mem_stats.memcpy_count,
    mem_stats.memmove_count,
    mem_stats.strdup_count,
    mem_stats.strcpy_count,
    mem_stats.bytes_allocated,
    mem_stats.bytes_freed
  );
}

__attribute__((constructor))
static void xmem_init_report(void) {
  atexit(xmem_report);
}

#endif
