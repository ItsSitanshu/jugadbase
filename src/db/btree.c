#include "btree.h"

BTree* btree_create() {
  BTree* tree = (BTree*)malloc(sizeof(BTree));

  tree->btree_order = calculate_btree_order();

  tree->root = NULL;
  return tree;
}

BTreeNode* btree_create_node(bool is_leaf) {
  BTreeNode* node = (BTreeNode*)malloc(sizeof(BTreeNode));
  node->is_leaf = is_leaf;
  node->num_keys = 0;
  memset(node->keys, 0, sizeof(node->keys));
  memset(node->children, 0, sizeof(node->children));
  return node;
}

long btree_search(BTree* tree, void* key) {
  if (!tree || !tree->root) return -1;

  BTreeNode* node = tree->root;
  while (node) {
    int i = 0;

    while (i < node->num_keys && compare_keys(key, node->keys[i], tree->key_type) > 0) {
      i++;
    }

    if (i < node->num_keys && compare_keys(key, node->keys[i], tree->key_type) == 0) {
      return node->row_pointers[i]; 
    }

    node = node->children[i]; 
  }
  
  return -1;
}


void btree_split_child(BTreeNode* parent, int index, BTreeNode* child, size_t btree_order) {
  int mid = btree_order / 2;
  BTreeNode* new_node = btree_create_node(child->is_leaf);

  new_node->num_keys = mid - 1;

  for (int i = 0; i < mid - 1; i++) {
    new_node->keys[i] = child->keys[i + mid];
    new_node->row_pointers[i] = child->row_pointers[i + mid];
  }

  if (!child->is_leaf) {
    for (int i = 0; i < mid; i++) {
      new_node->children[i] = child->children[i + mid];
    }
  }

  child->num_keys = mid - 1;

  for (int i = parent->num_keys; i > index; i--) {
    parent->children[i + 1] = parent->children[i];
  }
  parent->children[index + 1] = new_node;

  for (int i = parent->num_keys - 1; i >= index; i--) {
    parent->keys[i + 1] = parent->keys[i];
    parent->row_pointers[i + 1] = parent->row_pointers[i];
  }

  parent->keys[index] = child->keys[mid - 1];
  parent->row_pointers[index] = child->row_pointers[mid - 1];
  parent->num_keys++;
}

void btree_insert_nonfull(BTree* tree, BTreeNode* node, void* key, long row_offset) {
  int i = node->num_keys - 1;

  if (node->is_leaf) {
    while (i >= 0 && compare_keys(key, node->keys[i], tree->key_type) < 0) {
      node->keys[i + 1] = node->keys[i];
      node->row_pointers[i + 1] = node->row_pointers[i];
      i--;
    }

    void* new_key = malloc(sizeof_key(tree->key_type));
    copy_key(new_key, key, tree->key_type);  

    node->keys[i + 1] = new_key;
    node->row_pointers[i + 1] = row_offset;
    node->num_keys++;
  } else {
    while (i >= 0 && compare_keys(key, node->keys[i], tree->key_type) < 0) {
      i--;
    }

    if (node->children[i + 1]->num_keys == tree->btree_order - 1) {
      btree_split_child(node, i + 1, node->children[i + 1], tree->btree_order);

      if (compare_keys(key, node->keys[i + 1], tree->key_type) > 0) {
        i++;
      }
    }

    btree_insert_nonfull(tree, node->children[i + 1], key, row_offset);
  }
}


void btree_insert(BTree* tree, void* key, long row_offset) {
  if (!tree->root) {
    tree->root = btree_create_node(true);
  }

  BTreeNode* root = tree->root;
  if (root->num_keys == tree->btree_order - 1) {
    BTreeNode* new_root = btree_create_node(false);
    new_root->children[0] = root;
    btree_split_child(new_root, 0, root, tree->btree_order);
    tree->root = new_root;
    btree_insert_nonfull(tree, new_root, key, row_offset);
  } else {
    btree_insert_nonfull(tree, root, key, row_offset);
  }
}

void btree_free_node(BTreeNode* node) {
  if (!node) return;

  if (!node->is_leaf) {
    for (int i = 0; i <= node->num_keys; i++) {
      btree_free_node(node->children[i]);
    }
  }

  free(node);
}

void btree_destroy(BTree* tree) {
  if (!tree) return;
  btree_free_node(tree->root);
  free(tree);
}

int key_compare(void* key1, void* key2) {
  return strcmp((char*)key1, (char*)key2);
}

int compare_keys(void* key1, void* key2, u_int8_t type) {
  return strcmp((char*)key1, (char*)key2);
}


size_t sizeof_key(u_int32_t type) {
  return 0;
}

void copy_key(void* dest, void* src, u_int32_t type) {
  
}

long calculate_btree_order() {
  size_t key_size = sizeof(int);               
  size_t row_pointer_size = sizeof(long);     
  size_t child_pointer_size = sizeof(BTreeNode*); 
  
  struct statfs fs_info;
  if (statfs("/", &fs_info) == -1) {
    return 4096;
  }
  unsigned long long block_size = fs_info.f_bsize; 

  size_t node_size = (key_size + row_pointer_size + child_pointer_size);
  
  size_t total_size = node_size + sizeof(bool) + sizeof(int); 

  unsigned long order = block_size / total_size;

  if (order > MAX_KEYS_PER_NODE) {
    order = MAX_KEYS_PER_NODE;
  }

  return order;
}