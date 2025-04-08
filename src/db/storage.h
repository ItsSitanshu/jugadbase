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

  Row rows[PAGE_SIZE / sizeof(Row)];
} Page;

typedef struct BufferPool {
  Page* pages[POOL_SIZE];
  uint64_t page_numbers[POOL_SIZE];
  uint64_t next_row_id;
} BufferPool;

BufferPool buffer_pool;

void read_page_from_disk(uint64_t page_number, Page* page);
void write_page_to_disk(FILE* file, uint64_t page_number, Page* page);
Page* get_page_from_buffer_pool(uint64_t page_number);
void flush_page_to_disk(Page* page);
void initialize_buffer_pool();
Page* get_free_page_from_pool();
void evict_page_from_pool(uint64_t page_number);
void flush_all_pages();
void insert_row(uint64_t row_id, const void* row_data);
Row* retrieve_row(uint64_t row_id);
void delete_row(uint64_t row_id);
void initialize_null_bitmap(Row* row, uint8_t num_columns);
void set_column_null(Row* row, uint8_t column_index, bool is_null);
bool get_column_null(Row* row, uint8_t column_index);
void write_column_data(ColumnValue* column_value, uint8_t* buffer);
void read_column_data(ColumnValue* column_value, const uint8_t* buffer);
void allocate_row_memory(Row* row, uint8_t num_columns);
void free_row_memory(Row* row);

#endif // STORAGE_H
