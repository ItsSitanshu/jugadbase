#include <stdlib.h>
#include <string.h>

#include "storage/syscache.h"
#include "uthash.h" 

SysCache* create_syscache() {
  SysCache* cache = xmalloc(sizeof(SysCache));
  if (!cache) return NULL;
  cache->by_id = NULL;
  cache->by_name = NULL;
  cache->by_table_id = NULL;
  cache->by_ref_id = NULL;  // initialize new hash index
  return cache;
}

void destroy_syscache(SysCache* cache) {
  if (!cache) return;

  ConstraintCacheEntry *entry, *tmp;
  HASH_ITER(hh_id, cache->by_id, entry, tmp) {
    HASH_DELETE(hh_id, cache->by_id, entry);
    HASH_DELETE(hh_name, cache->by_name, entry);
    HASH_DELETE(hh_tableid, cache->by_table_id, entry);
    HASH_DELETE(hh_refid, cache->by_ref_id, entry);  // delete from ref_id hash
    xfree(entry);
  }
  xfree(cache);
}

void syscache_add_constraint(SysCache* cache, Constraint* c) {
  if (!cache || !c) return;

  ConstraintCacheEntry* entry = xmalloc(sizeof(ConstraintCacheEntry));
  if (!entry) return;

  entry->constraint = *c;

  HASH_ADD(hh_id, cache->by_id, constraint.id, sizeof(int64_t), entry);
  HASH_ADD_KEYPTR(hh_name, cache->by_name, entry->constraint.name, strlen(entry->constraint.name), entry);
  HASH_ADD(hh_tableid, cache->by_table_id, constraint.table_id, sizeof(int64_t), entry);
  HASH_ADD(hh_refid, cache->by_ref_id, constraint.ref_table_id, sizeof(int64_t), entry);  // add to ref_id hash
}

Constraint* syscache_get_constraint_by_id(SysCache* cache, int64_t id) {
  if (!cache) return NULL;

  ConstraintCacheEntry* entry = NULL;
  HASH_FIND(hh_id, cache->by_id, &id, sizeof(int64_t), entry);
  return entry ? &entry->constraint : NULL;
}

Constraint* syscache_get_constraint_by_name(SysCache* cache, const char* name) {
  if (!cache || !name) return NULL;

  ConstraintCacheEntry* entry = NULL;
  HASH_FIND(hh_name, cache->by_name, name, strlen(name), entry);
  return entry ? &entry->constraint : NULL;
}

ConstraintListNode* syscache_get_constraints_by_table(SysCache* cache, int64_t table_id) {
  if (!cache) return NULL;

  ConstraintListNode *head = NULL, *tail = NULL;

  ConstraintCacheEntry *entry, *tmp;
  HASH_ITER(hh_tableid, cache->by_table_id, entry, tmp) {
    if (entry->constraint.table_id == table_id) {
      ConstraintListNode* node = xmalloc(sizeof(ConstraintListNode));
      if (!node) {
        // free list on OOM
        while (head) {
          ConstraintListNode* next = head->next;
          xfree(head);
          head = next;
        }
        return NULL;
      }
      node->constraint = &entry->constraint;
      node->next = NULL;
      if (!head) head = tail = node;
      else { tail->next = node; tail = node; }
    }
  }
  return head;
}

ConstraintListNode* syscache_get_constraints_by_ref_table(SysCache* cache, int64_t ref_table) {
  if (!cache) return NULL;

  ConstraintListNode *head = NULL, *tail = NULL;

  ConstraintCacheEntry *entry, *tmp;
  HASH_ITER(hh_refid, cache->by_ref_id, entry, tmp) {
    if (entry->constraint.ref_table_id == ref_table) {
      ConstraintListNode* node = xmalloc(sizeof(ConstraintListNode));
      if (!node) {
        while (head) {
          ConstraintListNode* next = head->next;
          xfree(head);
          head = next;
        }
        return NULL;
      }
      node->constraint = &entry->constraint;
      node->next = NULL;
      if (!head) head = tail = node;
      else { tail->next = node; tail = node; }
    }
  }
  return head;
}

void free_constraint_list(ConstraintListNode* head) {
  while (head) {
    ConstraintListNode* next = head->next;
    xfree(head);
    head = next;
  }
}

void syscache_remove_constraint_by_id(SysCache* cache, int64_t id) {
  if (!cache) return;

  ConstraintCacheEntry* entry = NULL;
  HASH_FIND(hh_id, cache->by_id, &id, sizeof(int64_t), entry);
  if (!entry) return;

  HASH_DELETE(hh_id, cache->by_id, entry);
  HASH_DELETE(hh_name, cache->by_name, entry);
  HASH_DELETE(hh_tableid, cache->by_table_id, entry);
  HASH_DELETE(hh_refid, cache->by_ref_id, entry);  // remove from ref_id hash

  xfree(entry);
}
