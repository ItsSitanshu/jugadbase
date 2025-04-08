#ifndef STORAGE_H
#define STORAGE_H

#include "io.h"
#include "fs.h"
#include "parser.h"

#define PAGE_SIZE 4096
#define POOL_SIZE 64

typedef struct Row {
  uint32_t page_id; 
  uint16_t row_id; 
  uint16_t row_length; 

  uint8_t null_bitmap_size; 
  uint8_t* null_bitmap; 
  
  ColumnValue* column_data; 
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
  uint32_t page_numbers[POOL_SIZE];

  uint32_t next_pg_no;
  char file[MAX_PATH_LENGTH];
  uint8_t idx;
} BufferPool;

void initialize_buffer_pool(BufferPool* pool, uint8_t idx, char* filename);

void read_page(FILE* file, uint64_t page_number, Page* page);
void write_page(FILE* file, uint64_t page_number, Page* page, TableCatalogEntry tc);
void write_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def);

void serialize_insert(BufferPool* pool, Row row);

#endif // STORAGE_H