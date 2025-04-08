// #include "storage.h"

// void read_page_from_disk(uint64_t page_number, Page* page) {
//   FILE* file = fopen("database.dat", "rb");
//   if (!file) {
//     fprintf(stderr, "Error opening file for reading.\n");
//     return;
//   }
//   fseek(file, page_number * PAGE_SIZE, SEEK_SET);
//   fread(page, PAGE_SIZE, 1, file);
//   fclose(file);
// }

// void write_page_to_disk(FILE* file, uint64_t page_number, Page* page) {
//   fseek(file, page_number * PAGE_SIZE, SEEK_SET);

//   fwrite(&page->page_id, sizeof(page->page_id), 1, file);
//   fwrite(&page->num_rows, sizeof(page->num_rows), 1, file);
//   fwrite(&page->free_space, sizeof(page->free_space), 1, file);

//   for (int i = 0; i < page->num_rows; i++) {
//     Row* row = &page->rows[i];

//     fwrite(&row->page_id, sizeof(row->page_id), 1, file);
//     fwrite(&row->row_id, sizeof(row->row_id), 1, file);
//     fwrite(&row->row_length, sizeof(row->row_length), 1, file);

//     fwrite(&row->null_bitmap_size, sizeof(row->null_bitmap_size), 1, file);
//     if (row->null_bitmap != NULL) {
//       fwrite(row->null_bitmap, row->null_bitmap_size, 1, file);
//     }

//     for (int j = 0; j < MAX_COLUMNS; j++) {
//       if (row->column_data[j].type == TOK_T_INT) {
//         fwrite(&row->column_data[j].int_value, sizeof(int), 1, file);
//       }
//     }
//   }
// }

// Page* get_page_from_buffer_pool(uint64_t page_number) {
//   for (int i = 0; i < POOL_SIZE; i++) {
//     if (buffer_pool.page_numbers[i] == page_number) {
//       return buffer_pool.pages[i];
//     }
//   }
//   Page* page = malloc(sizeof(Page));
//   read_page_from_disk(page_number, page);
//   for (int i = 0; i < POOL_SIZE; i++) {
//     if (buffer_pool.pages[i] == NULL) {
//       buffer_pool.pages[i] = page;
//       buffer_pool.page_numbers[i] = page_number;
//       return page;
//     }
//   }
//   evict_page_from_pool(buffer_pool.page_numbers[0]);
//   buffer_pool.pages[0] = page;
//   buffer_pool.page_numbers[0] = page_number;
//   return page;
// }

// void flush_page_to_disk(FILE* file, Page* page) {
//   write_page_to_disk(file, page->page_id, page);
// }

// void initialize_buffer_pool() {
//   for (int i = 0; i < POOL_SIZE; i++) {
//     buffer_pool.pages[i] = NULL;
//     buffer_pool.page_numbers[i] = -1;
//   }
//   buffer_pool.next_row_id = 0;
// }

// Page* get_free_page_from_pool() {
//   for (int i = 0; i < POOL_SIZE; i++) {
//     if (buffer_pool.pages[i] == NULL) {
//       Page* new_page = malloc(sizeof(Page));
//       new_page->page_id = buffer_pool.next_row_id++;
//       buffer_pool.pages[i] = new_page;
//       buffer_pool.page_numbers[i] = new_page->page_id;
//       return new_page;
//     }
//   }
//   return NULL;
// }

// void evict_page_from_pool(uint64_t page_number) {
//   for (int i = 0; i < POOL_SIZE; i++) {
//     if (buffer_pool.page_numbers[i] == page_number) {
//       flush_page_to_disk(buffer_pool.pages[i]);
//       buffer_pool.pages[i] = NULL;
//       buffer_pool.page_numbers[i] = -1;
//       break;
//     }
//   }
// }

// void flush_all_pages() {
//   for (int i = 0; i < POOL_SIZE; i++) {
//     if (buffer_pool.pages[i] != NULL) {
//       flush_page_to_disk(buffer_pool.pages[i]);
//     }
//   }
// }

// void insert_row(uint64_t row_id, const void* row_data) {
//   Page* page = get_page_from_buffer_pool(row_id / PAGE_SIZE);
//   if (page->free_space < sizeof(Row)) {
//     flush_page_to_disk(page);
//     page = get_page_from_buffer_pool(row_id / PAGE_SIZE);
//   }
//   Row* row = malloc(sizeof(Row));
//   memcpy(row, row_data, sizeof(Row));
//   page->rows[page->num_rows++] = *row;
//   page->free_space -= sizeof(Row);
// }

// Row* retrieve_row(uint64_t row_id) {
//   Page* page = get_page_from_buffer_pool(row_id / PAGE_SIZE);
//   for (int i = 0; i < page->num_rows; i++) {
//     if (page->rows[i].row_id == row_id) {
//       return &page->rows[i];
//     }
//   }
//   return NULL;
// }

// void delete_row(uint64_t row_id) {
//   Page* page = get_page_from_buffer_pool(row_id / PAGE_SIZE);
//   for (int i = 0; i < page->num_rows; i++) {
//     if (page->rows[i].row_id == row_id) {
//       page->rows[i] = page->rows[--page->num_rows];
//       page->free_space += sizeof(Row);
//       break;
//     }
//   }
// }

// void initialize_null_bitmap(Row* row, uint8_t num_columns) {
//   row->null_bitmap_size = (num_columns + 7) / 8;
//   row->null_bitmap = malloc(row->null_bitmap_size);
//   memset(row->null_bitmap, 0, row->null_bitmap_size);
// }

// void set_column_null(Row* row, uint8_t column_index, bool is_null) {
//   uint8_t byte = column_index / 8;
//   uint8_t bit = column_index % 8;
//   if (is_null) {
//     row->null_bitmap[byte] |= (1 << bit);
//   } else {
//     row->null_bitmap[byte] &= ~(1 << bit);
//   }
// }

// bool get_column_null(Row* row, uint8_t column_index) {
//   uint8_t byte = column_index / 8;
//   uint8_t bit = column_index % 8;
//   return (row->null_bitmap[byte] & (1 << bit)) != 0;
// }

// void write_column_data(ColumnValue* column_value, uint8_t* buffer) {
//   switch (column_value->type) {
//     case TOK_T_INT:
//       memcpy(buffer, &column_value->int_value, sizeof(int));
//       break;
//     case TOK_T_FLOAT:
//       memcpy(buffer, &column_value->float_value, sizeof(float));
//       break;
//     case TOK_T_DOUBLE:
//       memcpy(buffer, &column_value->double_value, sizeof(double));
//       break;
//     case TOK_T_BOOL:
//       memcpy(buffer, &column_value->bool_value, sizeof(bool));
//       break;
//     case TOK_T_VARCHAR:
//       memcpy(buffer, column_value->str_value, strlen(column_value->str_value));
//       break;
//     default:
//       fprintf(stderr, "Unknown column type\n");
//       break;
//   }
// }

// void read_column_data(ColumnValue* column_value, const uint8_t* buffer) {
//   switch (column_value->type) {
//     case TOK_T_INT:
//       memcpy(&column_value->int_value, buffer, sizeof(int));
//       break;
//     case TOK_T_FLOAT:
//       memcpy(&column_value->float_value, buffer, sizeof(float));
//       break;
//     case TOK_T_DOUBLE:
//       memcpy(&column_value->double_value, buffer, sizeof(double));
//       break;
//     case TOK_T_BOOL:
//       memcpy(&column_value->bool_value, buffer, sizeof(bool));
//       break;
//     case TOK_T_VARCHAR:
//       strncpy(column_value->str_value, (char*)buffer, MAX_IDENTIFIER_LEN);
//       break;
//     default:
//       fprintf(stderr, "Unknown column type\n");
//       break;
//   }
// }

// void allocate_row_memory(Row* row, uint8_t num_columns) {
//   row->column_data = malloc(num_columns * sizeof(ColumnValue));
//   row->null_bitmap = malloc((num_columns + 7) / 8);
// }

// void free_row_memory(Row* row) {
//   free(row->column_data);
//   free(row->null_bitmap);
// }