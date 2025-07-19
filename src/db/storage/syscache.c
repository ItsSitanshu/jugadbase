#include <stdlib.h>

#include "storage/syscache.h"

SysCache* create_syscache() {
  SysCache* cache = malloc(sizeof(SysCache));
  if (!cache) return NULL;
  cache->constraint_cache_by_id = NULL;
  return cache;
}

void destroy_syscache(SysCache* cache) {
  if (!cache) return;
  syscache_clear_constraints(cache);
  free(cache);
}

void syscache_add_constraint(SysCache* cache, Constraint* c) {
  if (!cache || !c) return;

  ConstraintCacheEntry* entry = malloc(sizeof(ConstraintCacheEntry));
  if (!entry) return;

  entry->constraint_id = c->id;
  entry->constraint = *c;
  HASH_ADD_INT(cache->constraint_cache_by_id, constraint_id, entry);
}

Constraint* syscache_get_constraint_by_id(SysCache* cache, int64_t id) {
  if (!cache) return NULL;

  ConstraintCacheEntry* entry = NULL;
  HASH_FIND_INT(cache->constraint_cache_by_id, &id, entry);
  if (!entry) return NULL;

  return &entry->constraint;
}

void syscache_clear_constraints(SysCache* cache) {
  if (!cache) return;

  ConstraintCacheEntry *entry, *tmp;
  HASH_ITER(hh, cache->constraint_cache_by_id, entry, tmp) {
    HASH_DEL(cache->constraint_cache_by_id, entry);
    free(entry);
  }
}
