#ifndef STORAGE_H
#define STORAGE_H

#include "utils/io.h"
#include "storage/fs.h"
#include "parser/parser.h"

#define PAGE_SIZE 8192
#define POOL_SIZE 32
#define MAX_ROW_BUFFER 8192

typedef struct Row {
  RowID id;
  uint16_t row_length; 

  uint8_t null_bitmap_size; 
  uint8_t* null_bitmap; 
  
  ColumnValue* values; 
  size_t n_values;

  bool deleted;
} Row;

typedef struct Page {
  uint32_t page_id; 
  uint16_t num_rows;
  uint16_t free_space;
  bool is_dirty, is_full;

  Row rows[PAGE_SIZE / sizeof(Row)];
} Page;

typedef struct BufferPool {
  Page* pages[POOL_SIZE];
  char file[MAX_PATH_LENGTH];
  
  uint32_t page_numbers[POOL_SIZE];
  uint32_t next_pg_no;

  uint8_t idx;
  uint8_t num_pages;
} BufferPool;

void initialize_buffer_pool(BufferPool* pool, uint8_t idx, char* filename);
Page* page_init(uint32_t pg_n);

void read_page(FILE* file, uint64_t page_number, Page* page, TableCatalogEntry tc);
void read_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def);
void write_page(FILE* file, uint64_t page_number, Page* page, TableCatalogEntry tc);
void write_array_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def);
uint32_t write_array_value_to_buffer(uint8_t* buffer, ColumnValue* col_val, ColumnDefinition* col_def);
void write_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def);
uint32_t write_column_value_to_buffer(uint8_t* buffer, ColumnValue* col_val, ColumnDefinition* col_def);
RowID serialize_insert(BufferPool* pool, Row row, TableCatalogEntry tc);
uint32_t row_to_buffer(Row* row, BufferPool* pool, TableSchema* schema, uint8_t* buffer);
bool serialize_delete(BufferPool* pool, RowID rid);

void pop_lru_page(BufferPool* pool, TableCatalogEntry tc);

#endif // STORAGE_H