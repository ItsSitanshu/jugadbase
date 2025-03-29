#ifndef IO_H
#define IO_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <dirent.h>

#ifdef _WIN32
#include <windows.h>
#include <stdio.h>

#define statfs(path, buf) GetDiskFreeSpaceEx(path, NULL, NULL, (PULARGE_INTEGER)buf)

struct statfs {
  unsigned long long f_bsize;
};
#else
#include <sys/statfs.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#define MAX_TABLE_NAME 32
#define MAX_SCHEMA 128
#define MAX_KEYS_PER_NODE 1000
#define TABLE_COUNT_OFFSET 4UL

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

IO* io_init(const char* filename, IOMode mode, size_t buffer_capacity);
void io_close(IO* io);
void io_flush(IO* io);
void io_write(IO* io, const void* data, size_t size);
size_t io_read(IO* io, void* buffer, size_t size);
void io_seek(IO* io, long offset, int whence);
void io_seek_write(IO* io, long offset, const void* data, size_t size, int whence);
long io_tell(IO* io);

TableMetadata* io_read_metadata(IO* io, const char* table_name);

int io_write_record(IO* io, int key, void* record, size_t record_size);
int io_read_record(IO* io, int key, void* buffer, size_t record_size);

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

#endif // IO_H
