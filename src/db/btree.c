#include "btree.h"

BTree* btree_create(uint8_t key_type) {
  BTree* tree = (BTree*)malloc(sizeof(BTree));

  tree->btree_order = calculate_btree_order();
  tree->key_type = key_type;

  tree->root = NULL;
  return tree;
}

BTreeNode* btree_create_node(bool is_leaf, size_t btree_order) {
  BTreeNode* node = (BTreeNode*)malloc(sizeof(BTreeNode));

  node->is_leaf = is_leaf;
  node->num_keys = 0;

  node->keys = (void**)malloc(sizeof(void*) * (btree_order - 1));  
  node->row_pointers = (RowID*)malloc(sizeof(RowID) * (btree_order - 1));  
  node->children = (BTreeNode**)malloc(sizeof(BTreeNode*) * btree_order);  

  memset(node->keys, 0, sizeof(void*) * (btree_order - 1));
  memset(node->row_pointers, 0, sizeof(RowID) * (btree_order - 1));
  memset(node->children, 0, sizeof(BTreeNode*) * btree_order);

  return node;
}

RowID btree_search(BTree* tree, void* key) {
  if (!tree || !tree->root) return (RowID){0};
  LOG_DEBUG("Calling btree_search(tree: %p, key: %p", tree, key);

  BTreeNode* node = tree->root;
  while (node) {
    int i = 0;

    while (i < node->num_keys && key_compare(key, node->keys[i], tree->key_type) != 0) {
      i++;
    }

    if (i < node->num_keys && key_compare(key, node->keys[i], tree->key_type) == 0) {
      return node->row_pointers[i]; 
    }

    if (node->is_leaf) {
      break;
    }

    node = node->children[i]; 
  }
  
  return (RowID){0};
}

void btree_split_child(BTreeNode* parent, int index, BTreeNode* child, size_t btree_order) {
  int mid = btree_order / 2;
  BTreeNode* new_node = btree_create_node(child->is_leaf, btree_order);

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

void btree_insert_nonfull(BTree* tree, BTreeNode* node, void* key, RowID row_offset) {
  int i = node->num_keys - 1;

  if (node->is_leaf) {
    while (i >= 0 && key_compare(key, node->keys[i], tree->key_type) < 0) {
      node->keys[i + 1] = node->keys[i];
      node->row_pointers[i + 1] = node->row_pointers[i];
      i--;
    }

    void* new_key = malloc(key_size_for_type(tree->key_type));
    copy_key(new_key, key, tree->key_type);

    node->keys[i + 1] = new_key;
    node->row_pointers[i + 1] = row_offset;
    node->num_keys++;
  } else {
    while (i >= 0 && key_compare(key, node->keys[i], tree->key_type) < 0) {
      i--;
    }

    i++; 

    if (node->children[i]->num_keys == tree->btree_order - 1) {
      btree_split_child(node, i, node->children[i], tree->btree_order);

      if (key_compare(key, node->keys[i], tree->key_type) > 0) {
        i++;
      }
    }

    btree_insert_nonfull(tree, node->children[i], key, row_offset);
  }
}


bool btree_insert(BTree* tree, void* key, RowID row_offset) {
  if (!tree) {
    LOG_ERROR("B-tree couldn't be updated properly.\n\t > run jugad-cli fix");
    return false; 
  }

  LOG_DEBUG("Calling btree_insert(tree: %p, key: %p, off: {%u, %u})", tree, key, row_offset.page_id, row_offset.row_id);
  
  if (!tree->root) {
    tree->root = btree_create_node(true, tree->btree_order); // initialze as leaf
  }

  BTreeNode* root = tree->root;
  if (root->num_keys == tree->btree_order - 1) {
    BTreeNode* new_root = btree_create_node(false, tree->btree_order);
    new_root->children[0] = root;
    btree_split_child(new_root, 0, root, tree->btree_order);
    tree->root = new_root;
    btree_insert_nonfull(tree, new_root, key, row_offset);
  } else {
    btree_insert_nonfull(tree, root, key, row_offset);
  }

  return true; 
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

BTree* load_btree(FILE* file) {
  /*
  [4B] - Unique Tree Identifier
  [8B] - B-Tree Order
  [4B] - Key Type
  */

  BTree* tree = malloc(sizeof(BTree));
  fread(&tree->id, sizeof(uint32_t), 1, file);
  fread(&tree->btree_order, sizeof(long), 1, file);
  fread(&tree->key_type, sizeof(int), 1, file);

  tree->root = load_tree_node(file, tree->key_type);
  
  return tree;
}

BTreeNode* load_tree_node(FILE* db_file, uint8_t key_type) {
  BTreeNode* node = malloc(sizeof(BTreeNode));
  bool is_invalid = false;

  eof_fread(&node->is_leaf, sizeof(bool), 1, db_file, &is_invalid);
  
  if (is_invalid) {
    return NULL;
  }

  fread(&node->num_keys, sizeof(int), 1, db_file);

  int ksize = key_size_for_type(key_type);

  node->keys = malloc(sizeof(void*) * node->num_keys);
  for (int i = 0; i < node->num_keys; i++) {
    node->keys[i] = malloc(ksize);
    fread(node->keys[i], ksize, 1, db_file);
  }

  node->row_pointers = malloc(sizeof(RowID) * node->num_keys);
  fread(node->row_pointers, sizeof(RowID), node->num_keys, db_file);

  if (!node->is_leaf) {
    node->children = malloc(sizeof(BTreeNode*) * (node->num_keys + 1));
    for (int i = 0; i <= node->num_keys; i++) {
      node->children[i] = load_tree_node(db_file, key_type);
    }
  } else {
    node->children = NULL;
  }

  return node;
}

void save_btree(BTree* btree, FILE* db_file) {
  fwrite(&btree->id, sizeof(uint32_t), 1, db_file);  

  fwrite(&btree->btree_order, sizeof(long), 1, db_file);  

  fwrite(&btree->key_type, sizeof(int), 1, db_file);  

  save_tree_node(btree->root, db_file, btree->key_type);
}

void save_tree_node(BTreeNode* node, FILE* db_file, uint8_t key_type) {
  /*
  [1B] Flag (is leaf)
  [4B] Num Keys
  [<type>B] Actual Key Value
  [<num keys>*8B] Row Pointers
  */
  if (!node) { return; }
  
  fwrite(&node->is_leaf, sizeof(bool), 1, db_file);
  fwrite(&node->num_keys, sizeof(uint32_t), 1, db_file);

  int ksize = key_size_for_type(key_type);
  for (int i = 0; i < node->num_keys; i++) {
    fwrite(node->keys[i], ksize, 1, db_file);
  }

  fwrite(node->row_pointers, sizeof(RowID), node->num_keys, db_file);

  for (int i = 0; i < node->num_keys; i++) {
    LOG_DEBUG("Row pointer[%d]: {page: %u, slot: %u}", i, 
              node->row_pointers[i].page_id, node->row_pointers[i].row_id);
  }

  if (!node->is_leaf) {
    for (int i = 0; i <= node->num_keys; i++) {
      save_tree_node(node->children[i], db_file, key_type);
    }
  }
}

void unload_btree(BTree* btree, char* file_path) {
  FILE* fp = fopen(file_path, "wb+");
  if (!fp) {
    LOG_FATAL("Failed to open B-tree file '%s' for writing, indexes will not match.\n\t > Run jugad-cli fix", file_path);
    return;
  }

  save_btree(btree, fp);
  btree_destroy(btree);

  if (fclose(fp) != 0) {
    LOG_ERROR("Failed to close B-tree file '%s'.", file_path);
    return;
  }
}

int key_compare(void* key1, void* key2, uint8_t type) {
  switch (type) {
    case TOK_T_INT:
    case TOK_T_SERIAL: {
      int a = *(int*)key1;
      int b = *(int*)key2;
      LOG_DEBUG("a: %d b: %d", a, b);
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }

    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
    case TOK_T_TEXT:
    case TOK_T_DATE:
    case TOK_T_TIME:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
    case TOK_T_JSON: {
      int cmp = strcmp((char*)key1, (char*)key2);
      if (cmp < 0) return -1;
      if (cmp > 0) return 1;
      return 0;
    }

    case TOK_T_BOOL: {
      bool a = *(bool*)key1;
      bool b = *(bool*)key2;
      if (a == b) return 0;
      return a ? 1 : -1;
    }

    case TOK_T_FLOAT: {
      float a = *(float*)key1;
      float b = *(float*)key2;
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }

    case TOK_T_DOUBLE:
    case TOK_T_DECIMAL: {
      double a = *(double*)key1;
      double b = *(double*)key2;
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }

    case TOK_T_BLOB: {
      size_t len = *(size_t*)key1;
      void* data1 = (uint8_t*)key1 + sizeof(size_t);
      void* data2 = (uint8_t*)key2 + sizeof(size_t);
      return memcmp(data1, data2, len);
    }

    case TOK_T_UUID: {
      return memcmp(key1, key2, 16);
    }

    default:
      return 0;
  }
}

void copy_key(void* dest, void* src, uint8_t type) {
  size_t size = key_size_for_type(type);

  switch (type) {
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
    case TOK_T_TEXT:
    case TOK_T_UUID:
    case TOK_T_DATE:
    case TOK_T_TIME:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
    case TOK_T_JSON:
      strncpy((char*)dest, (char*)src, size);
      ((char*)dest)[size - 1] = '\0';
      break;

    default:
      memcpy(dest, src, size);
      break;
  }
}

int key_size_for_type(uint8_t key_type) {
  switch (key_type) {
    case TOK_T_INT:
    case TOK_T_BOOL:
    case TOK_T_SERIAL:
      return sizeof(int);

    case TOK_T_FLOAT:
      return sizeof(float);

    case TOK_T_DOUBLE:
    case TOK_T_DECIMAL:
      return sizeof(double);

    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
    case TOK_T_TEXT:
    case TOK_T_DATE:
    case TOK_T_TIME:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
    case TOK_T_JSON:
      return 256;

    case TOK_T_BLOB:
      return 512; 

    case TOK_T_UUID:
      return 16; 

    default:
      return sizeof(int); // fallback
  }
}

long calculate_btree_order() {
  size_t key_size = sizeof(int);               
  size_t row_pointer_size = sizeof(RowID);     
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