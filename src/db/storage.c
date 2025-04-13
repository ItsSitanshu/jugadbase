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

Page* page_init(uint32_t pg_n) {
  Page* page = (Page*)malloc(sizeof(Page));
  if (!page) {
    LOG_ERROR("Failed to allocate memory for page");
    return NULL;
  }

  page->page_id = pg_n;          
  page->num_rows = 0;         
  page->free_space = PAGE_SIZE; 

  page->is_dirty = false;    
  page->is_full = false;   

  return page; 
}

void read_page(FILE* file, uint64_t page_number, Page* page, TableCatalogEntry tc) {
  fseek(file, page_number * PAGE_SIZE, SEEK_SET);

  fread(&page->page_id, sizeof(page->page_id), 1, file);
  fread(&page->num_rows, sizeof(page->num_rows), 1, file);
  fread(&page->free_space, sizeof(page->free_space), 1, file);

  for (int i = 0; i < page->num_rows; i++) {
    Row* row = &page->rows[i];

    fread(&row->id.page_id, sizeof(row->id.page_id), 1, file);
    fread(&row->id.row_id, sizeof(row->id.row_id), 1, file);
    fread(&row->row_length, sizeof(row->row_length), 1, file);

    fread(&row->null_bitmap_size, sizeof(row->null_bitmap_size), 1, file);
    row->null_bitmap = malloc(row->null_bitmap_size);
    fread(row->null_bitmap, row->null_bitmap_size, 1, file);

    row->values = malloc(sizeof(ColumnValue) * tc.schema->column_count);
    for (int j = 0; j < tc.schema->column_count; j++) {
      row->values[j].is_null = (row->null_bitmap[j / 8] >> (j % 8)) & 1;
    }

    for (int j = 0; j < tc.schema->column_count; j++) {
      ColumnDefinition* col_def = &tc.schema->columns[j];

      if (!row->values[j].is_null && col_def) {
        read_column_value(file, &row->values[j], col_def);
      }
    }
  }
}

void read_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def) {
  uint16_t text_len, max_len;
  uint8_t str_len;

  if (col_val == NULL || file == NULL || col_def == NULL) {
    LOG_ERROR("Invalid input to read_column_value.\n");
    return;
  }

  col_val->type = col_def->type;

  switch (col_def->type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
      fread(&col_val->int_value, sizeof(int), 1, file);
      break;
    case TOK_T_BOOL:
      {
        uint8_t bool_byte;
        fread(&bool_byte, sizeof(uint8_t), 1, file);
        col_val->bool_value = bool_byte ? true : false;
      }
      break;

    case TOK_T_FLOAT:
      fread(&col_val->float_value, sizeof(float), 1, file);
      break;

    case TOK_T_DOUBLE:
      fread(&col_val->double_value, sizeof(double), 1, file);
      break;

    case TOK_T_DECIMAL:
      fread(&col_val->decimal.precision, sizeof(int), 1, file);
      fread(&col_val->decimal.scale, sizeof(int), 1, file);
      fread(col_val->decimal.decimal_value, sizeof(char), MAX_DECIMAL_LEN, file);
      break;

    // case TOK_T_UUID:
    //   {
    //     uint8_t binary_uuid[16];
    //     fread(binary_uuid, 16, 1, file);
    //     uuid_to_string(binary_uuid, col_val->str_value);  
    //   }
    //   break;

    case TOK_T_TIMESTAMP:
    case TOK_T_DATETIME:
    case TOK_T_TIME:
    case TOK_T_DATE:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      fread(&str_len, sizeof(uint8_t), 1, file);
      fread(col_val->str_value, str_len, 1, file);
      col_val->str_value[str_len] = '\0';
      break;

    case TOK_T_TEXT:
    case TOK_T_JSON:
      fread(&text_len, sizeof(uint16_t), 1, file);
      fread(col_val->str_value, text_len, 1, file);
      col_val->str_value[text_len] = '\0';
      break;

    default:
      LOG_ERROR("Unsupported data type in read_column_value.\n");
      break;
  }
}


void write_page(FILE* file, uint64_t page_number, Page* page, TableCatalogEntry tc) {
  fseek(file, page_number * PAGE_SIZE, SEEK_SET);

  fwrite(&page->page_id, sizeof(page->page_id), 1, file);
  fwrite(&page->num_rows, sizeof(page->num_rows), 1, file);
  fwrite(&page->free_space, sizeof(page->free_space), 1, file);

  for (int i = 0; i < page->num_rows; i++) {
    Row* row = &page->rows[i];

    fwrite(&row->id.page_id, sizeof(row->id.page_id), 1, file);
    fwrite(&row->id.row_id, sizeof(row->id.row_id), 1, file);

    fwrite(&row->row_length, sizeof(row->row_length), 1, file);

    fwrite(&row->null_bitmap_size, sizeof(row->null_bitmap_size), 1, file);
    if (row->null_bitmap != NULL) {
      fwrite(row->null_bitmap, row->null_bitmap_size, 1, file);
    }

    for (int j = 0; j < tc.schema->column_count; j++) {
      ColumnDefinition* col_def = &tc.schema->columns[j];

      if (!row->values[j].is_null && col_def) {
        write_column_value(file, &row->values[j], col_def);
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
          if (!parser_parse_uuid_string(col_val->str_value, binary_uuid)) {
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

RowID serialize_insert(BufferPool* pool, Row row, TableCatalogEntry tc) {
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
        page = page_init(0);
        pool->pages[i] = page;
        pool->next_pg_no++;
        pool->num_pages++;
        break;
      }
    }
  }

  if (page == NULL) {
    pop_lru_page(pool, tc);
    return serialize_insert(pool, row, tc);
  }

  row.id.row_id = page->num_rows + 1;
  row.id.page_id = page->page_id;

  page->rows[page->num_rows] = row;

  page->num_rows++;
  page->free_space -= row.row_length;
  page->is_dirty = true;

  if (page->free_space == 0) {
    page->is_full = true;
  }  

  return (RowID){ row.id.page_id, row.id.row_id };
}

bool serialize_delete(BufferPool* pool, RowID rid) {
  if (!pool || rid.row_id == 0) return false;

  for (int i = 0; i < POOL_SIZE; i++) {
    Page* page = pool->pages[i];
    if (!page || page->page_id != rid.page_id) continue;

    if (rid.row_id == 0 || rid.row_id > page->num_rows) return false;

    Row* row = &page->rows[rid.row_id - 1];

    if (is_struct_zeroed(row, sizeof(Row))) {
      LOG_WARN("serialize_delete: Row already deleted (page_id=%u, row_id=%u)", rid.page_id, rid.row_id);
      return false;
    }

    page->free_space += row->row_length;

    memset(row, 0, sizeof(Row));

    page->is_dirty = true;
    page->is_full = false;

    LOG_DEBUG("serialize_delete: Deleted row from page %u, slot %u", rid.page_id, rid.row_id);
    return true;
  }

  LOG_ERROR("serialize_delete: Page %u not found", rid.page_id);
  return false;
}

void pop_lru_page(BufferPool* pool, TableCatalogEntry tc) {
  if (pool->num_pages == 0) return;

  Page* lru_page = pool->pages[0];
  FILE* file = fopen(pool->file, "wb");

  if (lru_page->is_dirty) {
    write_page(file, lru_page->page_id, lru_page, tc); 
  }

  fclose(file);
  free(lru_page);

  for (int i = 1; i < pool->num_pages; i++) {
    pool->pages[i - 1] = pool->pages[i];
  }

  pool->num_pages--;
}