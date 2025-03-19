#ifndef IO_H
#define IO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define MAX_TABLE_NAME 32
#define MAX_SCHEMA 128
#define BTREE_ORDER 4  

typedef enum { IO_READ, IO_WRITE, IO_APPEND } IOMode;

typedef struct IO {
  FILE* file;
  char* buffer;
  size_t buf_size;
  size_t buf_capacity;
} IO;

typedef struct {
  char table_name[MAX_TABLE_NAME];
  char schema[MAX_SCHEMA];
  long root_offset;  
} TableMetadata;

typedef struct BTreeNode {
  bool is_leaf;
  int num_keys;
  void* keys[BTREE_ORDER - 1];  
  long row_pointers[BTREE_ORDER - 1];
  struct BTreeNode* children[BTREE_ORDER]; 
} BTreeNode;

typedef struct {
  BTreeNode* root;
} BTree;

BTree* btree_create();

IO* io_init(const char* filename, IOMode mode, size_t buffer_capacity);
void io_close(IO* io);
void io_flush(IO* io);
void io_write(IO* io, const void* data, size_t size);
size_t io_read(IO* io, void* buffer, size_t size);
void io_seek(IO* io, long offset);
void io_seek_write(IO* io, long offset, const void* data, size_t size);
long io_tell(IO* io);

void io_write_metadata(IO* io, const char* table_name, const char* schema);
TableMetadata* io_read_metadata(IO* io, const char* table_name);

int io_write_record(IO* io, int key, void* record, size_t record_size);
int io_read_record(IO* io, int key, void* buffer, size_t record_size);

int key_compare(void* key1, void* key2);
BTree* btree_create();
BTreeNode* btree_create_node(bool is_leaf);
long btree_search(BTree* tree, void* key);
void btree_insert(BTree* tree, void* key, long row_offset);
void btree_insert_nonfull(BTreeNode* node, void* key, long row_offset);
void btree_split_child(BTreeNode* parent, int index, BTreeNode* child);
void btree_destroy(BTree* tree);
void btree_free_node(BTreeNode* node);

#endif // IO_H
