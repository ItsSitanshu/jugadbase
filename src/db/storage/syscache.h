#ifndef SYS_CACHE_H
#define SYS_CACHE_H

#include <stdint.h>
#include <string.h>

#include "utils/ext/uthash.h"  
#include "kernel/kernel.h"  

typedef struct ConstraintCacheEntry {
  int64_t constraint_id;
  Constraint constraint;
  UT_hash_handle hh;
} ConstraintCacheEntry;

typedef struct SysCache {
  ConstraintCacheEntry* constraint_cache_by_id;
} SysCache;

SysCache* create_syscache();
void destroy_syscache(SysCache* cache);

void syscache_add_constraint(SysCache* cache, Constraint* c);
Constraint* syscache_get_constraint_by_id(SysCache* cache, int64_t id);
void syscache_clear_constraints(SysCache* cache);

#endif // SYS_CACHE_H