#include "io.h"

#include "../utils/log.h"

#include "io.h"
#include "../utils/log.h"

IO* io_init(const char* filename, IOMode mode, size_t buffer_capacity) {
  IO* io = malloc(sizeof(IO));
  if (!io) exit(EXIT_FAILURE);

  const char* mode_str = (mode == IO_READ) ? "rb" : (mode == IO_WRITE) ? "r+b" : "ab+";
  io->file = fopen(filename, mode_str);

  if (!io->file) {
    free(io);
    return NULL;
  }

  io->buffer = malloc(buffer_capacity);
  if (!io->buffer) {
    fclose(io->file);
    free(io);
    exit(EXIT_FAILURE);
  }

  io->buf_size = 0;
  io->buf_capacity = buffer_capacity;

  LOG_INFO("IO initialized for %s, buffer capacity: %zu", (strrchr(filename, '/') + 1), buffer_capacity);
  return io;
}

void io_close(IO* io) {
  if (!io) return;

  io_flush(io);
  fclose(io->file);
  free(io->buffer);
  free(io);
  
  LOG_INFO("IO closed and resources freed");
}

void io_flush(IO* io) {
  if (io->buf_size > 0) {
    fwrite(io->buffer, io->buf_size, 1, io->file);
    io->buf_size = 0;
    fflush(io->file);
    LOG_INFO("Flushed %zu bytes to file", io->buf_size);
  }
}

void io_write(IO* io, const void* data, size_t size) {
  if (io->buf_size + size > io->buf_capacity) {
    fwrite(io->buffer, io->buf_size, 1, io->file);
    io->buf_size = 0;
    LOG_WARN("Buffer overflow, flushed %zu bytes", io->buf_size);
  }

  memcpy(io->buffer + io->buf_size, data, size);
  io->buf_size += size;

  LOG_DEBUG("io_write: wrote %zu bytes, new buffer size: %zu", size, io->buf_size);
}

size_t io_read(IO* io, void* buffer, size_t size) {
  size_t bytes_read = fread(buffer, 1, size, io->file);
  LOG_DEBUG("io_read: read %zu bytes", bytes_read);
  return bytes_read;
}

void io_seek(IO* io, long offset, int whence) {
  if (!io || !io->file) return;

  if (io->buf_size > 0 && whence == SEEK_SET) {
    io_flush(io);
    LOG_DEBUG("Buffer flushed before seek");
  }

  fseek(io->file, offset, whence);
  LOG_DEBUG("io_seek: moved to offset %ld", offset);
}

void io_seek_write(IO* io, long offset, const void* data, size_t size, int whence) {
  io_flush(io);

  if (fseek(io->file, offset, whence) != 0) {
    perror("io_seek_write: seek failed");
    LOG_ERROR("Seek failed at offset %ld", offset);
    exit(1);
  }

  size_t written = fwrite(data, 1, size, io->file);
  if (written != size) {
    perror("io_seek_write: write failed");
    LOG_ERROR("Write failed at offset %ld, expected %zu bytes, wrote %zu bytes", offset, size, written);
    exit(1);
  }

  fflush(io->file);
  LOG_INFO("io_seek_write: wrote %zu bytes at offset %ld", size, offset);
}

long io_tell(IO* io) {
  long pos = ftell(io->file) + io->buf_size;
  LOG_DEBUG("io_tell: current position is %ld", pos);
  return pos;
}

void io_clear_buffer(IO* io) {
  memset(io->buffer, 0, io->buf_capacity);
  io->buf_size = 0;
  LOG_INFO("Buffer cleared");
}

TableMetadata* io_read_metadata(IO* io, const char* table_name) {
  fseek(io->file, 0, SEEK_SET);
  
  TableMetadata* meta = malloc(sizeof(TableMetadata));
  while (fread(meta, sizeof(TableMetadata), 1, io->file)) {
    if (strcmp(meta->table_name, table_name) == 0) {
      LOG_INFO("Found metadata for table: %s", table_name);
      return meta;
    }
  }

  free(meta);
  LOG_WARN("Table metadata not found for table: %s", table_name);
  return NULL;
}

int io_write_record(IO* io, int key, void* record, size_t record_size) {
  fseek(io->file, 0, SEEK_END);
  long offset = ftell(io->file);

  if (fwrite(&key, sizeof(int), 1, io->file) != 1) {
    LOG_ERROR("Failed to write key for record");
    return 0;
  }
  if (fwrite(record, record_size, 1, io->file) != 1) {
    LOG_ERROR("Failed to write record data");
    return 0;
  }

  io_flush(io);
  LOG_INFO("Record with key %d written at offset %ld", key, offset);
  return 1;
}

int io_read_record(IO* io, int key, void* buffer, size_t record_size) {
  fseek(io->file, sizeof(TableMetadata), SEEK_SET);
  
  int record_key;
  while (fread(&record_key, sizeof(int), 1, io->file)) {
    if (record_key == key) {
      size_t bytes_read = fread(buffer, record_size, 1, io->file);
      LOG_INFO("Record with key %d found and read %zu bytes", key, bytes_read);
      return bytes_read == 1;
    }
    fseek(io->file, record_size, SEEK_CUR);
  }
  LOG_WARN("Record with key %d not found", key);
  return 0;
}

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