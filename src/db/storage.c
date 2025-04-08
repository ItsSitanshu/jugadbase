#include "storage.h"

void initialize_buffer_pool(BufferPool* pool, uint8_t idx, char* filename) {
  for (int i = 0; i < POOL_SIZE; i++) {
    pool->pages[i] = NULL;
    pool->page_numbers[i] = 0;
  }

  pool->next_pg_no = 0;
  pool->idx = idx;

  memcpy(pool->file, filename, MAX_PATH_LENGTH - 1);
  pool->file[MAX_PATH_LENGTH - 1] = '\0';
} 


void read_page(FILE* file, uint64_t page_number, Page* page) {
  fseek(file, page_number * PAGE_SIZE, SEEK_SET);

  fread(&page->page_id, sizeof(page->page_id), 1, file);
  fread(&page->num_rows, sizeof(page->num_rows), 1, file);
  fread(&page->free_space, sizeof(page->free_space), 1, file);

  for (int i = 0; i < page->num_rows; i++) {
    Row* row = &page->rows[i];

    fread(&row->page_id, sizeof(row->page_id), 1, file);
    fread(&row->row_id, sizeof(row->row_id), 1, file);
    fread(&row->row_length, sizeof(row->row_length), 1, file);

    fread(&row->null_bitmap_size, sizeof(row->null_bitmap_size), 1, file);
    row->null_bitmap = malloc(row->null_bitmap_size); 
    fread(row->null_bitmap, row->null_bitmap_size, 1, file);

    for (int j = 0; j < MAX_COLUMNS; j++) {
      if (!row->column_data[j].is_null) {
        // read_column_value(file, row->column_data, j);
      }
    }
  }
}


void write_page(FILE* file, uint64_t page_number, Page* page, TableCatalogEntry tc) {
  fseek(file, page_number * PAGE_SIZE, SEEK_SET);

  fwrite(&page->page_id, sizeof(page->page_id), 1, file);
  fwrite(&page->num_rows, sizeof(page->num_rows), 1, file);
  fwrite(&page->free_space, sizeof(page->free_space), 1, file);

  for (int i = 0; i < page->num_rows; i++) {
    Row* row = &page->rows[i];

    fwrite(&row->page_id, sizeof(row->page_id), 1, file);
    fwrite(&row->row_id, sizeof(row->row_id), 1, file);
    fwrite(&row->row_length, sizeof(row->row_length), 1, file);

    fwrite(&row->null_bitmap_size, sizeof(row->null_bitmap_size), 1, file);
    if (row->null_bitmap != NULL) {
      fwrite(row->null_bitmap, row->null_bitmap_size, 1, file);
    }

    for (int j = 0; j < tc.schema->column_count; j++) {
      ColumnDefinition* col_def = &tc.schema->columns[j];

      if (!row->column_data[j].is_null && col_def) {
        write_column_value(file, &row->column_data[j], col_def);
      }
    }
  }
}

void write_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def) {
  uint16_t text_len, str_len, max_len;

  if (col_val == NULL || file == NULL) {
    LOG_ERROR("Invalid column value or file pointer.\n");
    return;
  }

  LOG_DEBUG("%d", col_def->type);

  switch (col_def->type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
      fwrite(&col_val->int_value, sizeof(int), 1, file);
      break;

    case TOK_T_BOOL:
      {
        uint8_t bool_value = col_val->bool_value ? 1 : 0;
        fwrite(&bool_value, sizeof(uint8_t), 1, file);
      }
      break;

    case TOK_T_FLOAT:
      fwrite(&col_val->float_value, sizeof(float), 1, file);
      break;

    case TOK_T_DOUBLE:
      fwrite(&col_val->double_value, sizeof(double), 1, file);
      break;

    case TOK_T_DECIMAL:
      fwrite(&col_val->decimal.precision, sizeof(int), 1, file);
      fwrite(&col_val->decimal.scale, sizeof(int), 1, file);
      fwrite(col_val->decimal.decimal_value, sizeof(char), MAX_DECIMAL_LEN, file);
      break;

    case TOK_T_UUID:
      {
        size_t uuid_len = strlen(col_val->str_value);
        if (uuid_len == 36) {  
          uint8_t binary_uuid[16];
          if (!parse_uuid_string(col_val->str_value, binary_uuid)) {
            LOG_ERROR("Error: Invalid UUID format.\n");
            return;
          }
          fwrite(binary_uuid, 16, 1, file);
        } else if (uuid_len == 16) {
          fwrite(col_val->str_value, 16, 1, file);
        } else {
          LOG_ERROR("Invalid UUID length.\n");
          return;
        }
      }
      break;

    case TOK_T_TIMESTAMP:
    case TOK_T_DATETIME:
    case TOK_T_TIME:
    case TOK_T_DATE:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      str_len = (uint16_t)strlen(col_val->str_value);
      max_len = (col_def->type_varchar == 0) ? 255 : col_def->type_varchar;

      if (str_len > max_len) {
        str_len = max_len;
      }

      fwrite(&str_len, sizeof(uint8_t), 1, file);
      LOG_DEBUG("! %s", col_val->str_value);
      fwrite(col_val->str_value, str_len, 1, file);
      break;

    case TOK_T_TEXT:
    case TOK_T_JSON:
      text_len = (uint16_t)strlen(col_val->str_value);
      max_len = (col_def->type == TOK_T_JSON) ? MAX_JSON_SIZE : MAX_TEXT_SIZE;

      if (text_len > max_len) {
        text_len = max_len;
      }

      fwrite(&text_len, sizeof(uint16_t), 1, file);
      fwrite(col_val->str_value, sizeof(char), text_len, file);
      break;

    default:
      LOG_ERROR("Error: Unsupported data type.\n");
      break;
  }
}


void serialize_insert(BufferPool* pool, Row row) {
  Page* page = NULL;

  for (int i = 0; i < POOL_SIZE; i++) {
    if (pool->pages[i] != NULL) {
      page = pool->pages[i];
      if (page->free_space >= row.row_length) {
        break; 
      }
    }
  }

  if (page == NULL) {
    for (int i = 0; i < POOL_SIZE; i++) {
      if (pool->pages[i] == NULL) {
        page = (Page*)malloc(sizeof(Page));
        
        page->page_id = pool->next_pg_no;  
        page->num_rows = 0;                    
        page->free_space = PAGE_SIZE;
        
        page->is_dirty = false;
        page->is_full = false;

        pool->pages[i] = page;
        pool->next_pg_no++;
        break;
      }
    }
  }

  if (page == NULL) {
    // pop_lru_page()
    return;
  }

  row.row_id = page->num_rows;
  row.page_id = page->page_id;

  page->rows[page->num_rows] = row;

  page->num_rows++;
  page->free_space -= row.row_length;
  page->is_dirty = true;

  if (page->free_space == 0) {
    page->is_full = true;
  }  
}