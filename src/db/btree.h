#ifndef BTREE_H
#define BTREE_H


#include "io.h"
 
#define MAX_KEYS_PER_NODE 1000
#define TABLE_COUNT_OFFSET 4UL

typedef struct BTreeNode {
  bool is_leaf;
  int num_keys;
  void** keys;  
  long* row_pointers;
  struct BTreeNode** children; 
} BTreeNode;

typedef struct {
  BTreeNode* root;
  long btree_order;
  int key_type;
  int* unique_key_types;
} BTree;

BTree* btree_create();

BTreeNode* btree_create_node(bool is_leaf);
long btree_search(BTree* tree, void* key);
void btree_insert(BTree* tree, void* key, long row_offset);
void btree_insert_nonfull(BTree* tree, BTreeNode* node, void* key, long row_offset);
void btree_split_child(BTreeNode* parent, int index, BTreeNode* child, size_t btree_order);
void btree_destroy(BTree* tree);
void btree_free_node(BTreeNode* node);

size_t sizeof_key(u_int32_t type);
void copy_key(void* dest, void* src, u_int32_t type);
int key_compare(void* key1, void* key2);
int compare_keys(void* key1, void* key2, u_int8_t type);

long calculate_btree_order();

#endif // BTREE_H