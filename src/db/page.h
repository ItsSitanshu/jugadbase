#ifndef PAGE_H
#define PAGE_H

#include <stdint.h>

#define PAGE_SIZE 4096  // Example: 4KB Page
#define MAX_ROWS_PER_PAGE 128  // Assuming fixed row size

typedef struct {
  uint32_t page_id;
  uint16_t free_space;
  uint16_t row_count;
} PageHeader;

typedef struct {
  PageHeader header;
  char data[PAGE_SIZE - sizeof(PageHeader)];
} Page;

Page* page_init(uint32_t page_id);
void load_page(FILE* db_file, uint32_t page_id, Page* page);
void write_page(FILE* db_file, Page* page);
int find_page_with_space(FILE* db_file, uint16_t record_size);
int insert_record(FILE* db_file, void* record, uint16_t record_size);
void* get_record(FILE* db_file, uint32_t page_id, uint16_t offset, uint16_t size);

#endif
