#ifndef SYS_CACHE_H
#define SYS_CACHE_H

#include <stdint.h>
#include <string.h>

#include "uthash.h"  
#include "kernel/kernel.h"  

typedef struct ConstraintCacheEntry {
  Constraint constraint;

  UT_hash_handle hh_id;      // by constraint.id
  UT_hash_handle hh_name;    // by constraint.name
  UT_hash_handle hh_tableid; // by constraint.table_id
  UT_hash_handle hh_refid; // by constraint.table_id
} ConstraintCacheEntry;

typedef struct SysCache {
  ConstraintCacheEntry* by_id;
  ConstraintCacheEntry* by_name;
  ConstraintCacheEntry* by_table_id;
  ConstraintCacheEntry* by_ref_id;
} SysCache;

typedef struct ConstraintListNode {
  Constraint* constraint;
  struct ConstraintListNode* next;
} ConstraintListNode;

SysCache* create_syscache();
void destroy_syscache(SysCache* cache);

void syscache_add_constraint(SysCache* cache, Constraint* c);

Constraint* syscache_get_constraint_by_id(SysCache* cache, int64_t id);
Constraint* syscache_get_constraint_by_name(SysCache* cache, const char* name);
ConstraintListNode* syscache_get_constraints_by_table(SysCache* cache, int64_t table_id);
void free_constraint_list(ConstraintListNode* head);

void syscache_remove_constraint_by_id(SysCache* cache, int64_t id);

#endif // SYS_CACHE_H