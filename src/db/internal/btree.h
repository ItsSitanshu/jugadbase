#ifndef BTREE_H
#define BTREE_H

#include "utils/io.h"
#include "parser/token.h"

#include <stdint.h>

#define MAX_KEYS_PER_NODE 1000
#define BTREE_LIFETIME_THRESHOLD 10
#define TABLE_COUNT_OFFSET 4UL

typedef struct __attribute__((aligned(32))) BTreeNode {
  bool is_leaf;
  int num_keys;
  void** keys;  
  RowID* row_pointers;
  struct BTreeNode** children; 
} BTreeNode;

typedef struct {
  uint32_t id; 
  BTreeNode* root;
  long btree_order;
  uint8_t key_type;
} BTree;

BTree* btree_create(uint8_t key_type);
BTreeNode* btree_create_node(bool is_leaf, size_t btree_order);
RowID btree_search(BTree* tree, void* key);
bool btree_delete(BTree* tree, void* key);
bool delete_from_node(BTreeNode* node, void* key, uint8_t key_type, size_t order);
bool btree_insert(BTree* tree, void* key, RowID row_offset);
void btree_insert_nonfull(BTree* tree, BTreeNode* node, void* key, RowID row_offset);
void btree_split_child(BTreeNode* parent, int index, BTreeNode* child, size_t btree_order);
void btree_free_node(BTreeNode* node, size_t btree_order);
void btree_destroy(BTree* tree);

BTree* load_btree(FILE* file);
BTreeNode* load_tree_node(FILE* db_file, uint8_t key_type);
void save_btree(BTree* btree, FILE* db_file);
void save_tree_node(BTreeNode* node, FILE* db_file, uint8_t key_type);
void unload_btree(BTree* btree, char* file_path);

void* btree_get_predecessor(BTreeNode* node, uint8_t type);
RowID btree_get_predecessor_ptr(BTreeNode* node);
void* btree_get_successor(BTreeNode* node, uint8_t type);
RowID btree_get_successor_ptr(BTreeNode* node);
void btree_merge_children(BTreeNode* parent, int idx, size_t order, uint8_t key_type);
void btree_rebalance(BTreeNode* parent, int idx, size_t order, uint8_t key_type);

int compare_arrays(void* array1, void* array2);
int key_compare(void* key1, void* key2, int16_t type);
void copy_key(void* dest, void* src, uint8_t type);
int key_size_for_type(uint8_t key_type);

long calculate_btree_order();

#endif // BTREE_H