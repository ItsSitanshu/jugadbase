#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "page.h"

Page* page_init(uint32_t page_id) {
  Page* page = (Page*)malloc(sizeof(Page));
  if (!page) return NULL;

  memset(page, 0, sizeof(Page));
  page->header.page_id = page_id;
  page->header.free_space = PAGE_SIZE - sizeof(PageHeader);
  page->header.row_count = 0;
  return page;
}

void load_page(FILE* db_file, uint32_t page_id, Page* page) {
  fseek(db_file, page_id * PAGE_SIZE, SEEK_SET);
  fread(page, PAGE_SIZE, 1, db_file);
}

void write_page(FILE* db_file, Page* page) {
  fseek(db_file, page->header.page_id * PAGE_SIZE, SEEK_SET);
  fwrite(page, PAGE_SIZE, 1, db_file);
  fflush(db_file);
}

int find_page_with_space(FILE* db_file, uint16_t record_size) {
  Page page;
  fseek(db_file, 0, SEEK_SET);  

  uint32_t page_id = 0;
  while (fread(&page, PAGE_SIZE, 1, db_file)) {
    if (page.header.free_space >= record_size) {
      return page_id;
    }
    page_id++;
  }

  return -1;  
}

int insert_record(FILE* db_file, void* record, uint16_t record_size) {
  int page_id = find_page_with_space(db_file, record_size);
  Page page;

  if (page_id == -1) {
    fseek(db_file, 0, SEEK_END);
    page_id = ftell(db_file) / PAGE_SIZE;

    memset(&page, 0, sizeof(Page));
    page.header.page_id = page_id;
    page.header.free_space = PAGE_SIZE - sizeof(PageHeader);
    page.header.row_count = 0;
  } else {
    load_page(db_file, page_id, &page);
  }

  uint16_t offset = PAGE_SIZE - page.header.free_space;
  memcpy(page.data + offset, record, record_size);

  page.header.row_count++;
  page.header.free_space -= record_size;

  write_page(db_file, &page);
  return page_id;
}

void* get_record(FILE* db_file, uint32_t page_id, uint16_t offset, uint16_t size) {
  Page page;
  load_page(db_file, page_id, &page);

  void* record = malloc(size);
  if (!record) return NULL;

  memcpy(record, page.data + offset, size);
  return record;
}
