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
  if (!p) {
    LOG_FATAL(
      "[xmalloc] malloc(%zu) returned NULL\n"
      "  Location : %s:%d\n"
      "  Requested size : %zu bytes\n"
      "  Current stats  : malloc=%zu calloc=%zu realloc=%zu free=%zu bytes_alloc=%zu\n",
      sz, file, line, sz,
      mem_stats.malloc_count, mem_stats.calloc_count,
      mem_stats.realloc_count, mem_stats.free_count,
      mem_stats.bytes_allocated
    );
  }
  mem_stats.malloc_count++;
  mem_stats.bytes_allocated += sz;
  return p;
}

static inline void* __xcalloc(size_t n, size_t sz, const char* file, int line) {
  void* p = calloc(n, sz);
  if (!p) {
    LOG_FATAL(
      "[xcalloc] calloc(%zu, %zu) returned NULL\n"
      "  Location : %s:%d\n"
      "  Requested size : %zu bytes (n=%zu * sz=%zu)\n"
      "  Current stats  : malloc=%zu calloc=%zu realloc=%zu free=%zu bytes_alloc=%zu\n",
      n, sz, file, line, n * sz, n, sz,
      mem_stats.malloc_count, mem_stats.calloc_count,
      mem_stats.realloc_count, mem_stats.free_count,
      mem_stats.bytes_allocated
    );
  }
  mem_stats.calloc_count++;
  mem_stats.bytes_allocated += n * sz;
  return p;
}

static inline void* __xrealloc(void* old, size_t sz, const char* file, int line) {
  void* p = realloc(old, sz);
  if (!p) {
    LOG_FATAL(
      "[xrealloc] realloc(%p, %zu) returned NULL\n"
      "  Location : %s:%d\n"
      "  Requested size : %zu bytes\n"
      "  Old pointer    : %p\n"
      "  Current stats  : malloc=%zu calloc=%zu realloc=%zu free=%zu bytes_alloc=%zu\n",
      old, sz, file, line, sz, old,
      mem_stats.malloc_count, mem_stats.calloc_count,
      mem_stats.realloc_count, mem_stats.free_count,
      mem_stats.bytes_allocated
    );
  }
  mem_stats.realloc_count++;
  mem_stats.bytes_allocated += sz;
  return p;
}

static inline void __xfree(void* p, const char* file, int line) {
  if (!p) {
    LOG_WARN(
      "[xfree] free(NULL) ignored\n"
      "  Location : %s:%d\n"
      "  Current stats: free_count=%zu\n",
      file, line, mem_stats.free_count
    );
    return;
  }
  free(p);
  mem_stats.free_count++;
}

static inline void* __xmemcpy(void* dest, const void* src, size_t n, const char* file, int line) {
  if (n == 0) return dest;
  if (!dest || !src) {
    LOG_FATAL(
      "[xmemcpy] NULL pointer detected\n"
      "  Location : %s:%d\n"
      "  dest=%p, src=%p, size=%zu\n",
      file, line, dest, src, n
    );
  }
  mem_stats.memcpy_count++;
  return memcpy(dest, src, n);
}

static inline void* __xmemmove(void* dest, const void* src, size_t n, const char* file, int line) {
  if (!dest || !src) {
    LOG_FATAL(
      "[xmemmove] NULL pointer detected\n"
      "  Location : %s:%d\n"
      "  dest=%p, src=%p, size=%zu\n",
      file, line, dest, src, n
    );
  }
  mem_stats.memmove_count++;
  return memmove(dest, src, n);
}

static inline char* __xstrdup(const char* s, const char* file, int line) {
  if (!s) {
    LOG_FATAL(
      "[xstrdup] NULL source string\n"
      "  Location : %s:%d\n",
      file, line
    );
  }
  char* dup = strdup(s);
  if (!dup) {
    LOG_FATAL(
      "[xstrdup] strdup failed\n"
      "  Location : %s:%d\n"
      "  Source   : \"%s\"\n",
      file, line, s
    );
  }
  mem_stats.strdup_count++;
  return dup;
}

static inline char* __xstrncpy(char* dest, const char* src, size_t n, const char* file, int line) {
  if (!dest || !src) {
    LOG_FATAL(
      "[xstrncpy] NULL pointer detected\n"
      "  Location : %s:%d\n"
      "  dest=%p, src=%p, n=%zu\n",
      file, line, dest, src, n
    );
  }
  mem_stats.strcpy_count++;
  if (n > 0) {
    strncpy(dest, src, n);
    dest[n-1] = '\0';
  }
  return dest;
}

static inline char* __xstrcpy(char* dest, const char* src, const char* file, int line) {
  if (!dest || !src) {
    LOG_FATAL(
      "[xstrcpy] NULL pointer detected\n"
      "  Location : %s:%d\n"
      "  dest=%p, src=%p\n",
      file, line, dest, src
    );
  }
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

#endif
