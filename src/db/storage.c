#include "storage.h"

void initialize_buffer_pool(BufferPool* pool, uint8_t idx, char* filename) {
  for (int i = 0; i < POOL_SIZE; i++) {
    pool->pages[i] = NULL;
    pool->page_numbers[i] = 0;
  }

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

void read_array_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def) {
  if (!file || !col_val || !col_def) {
    LOG_ERROR("Invalid input to read_array_value.\n");
    return;
  }

  uint16_t len;
  fread(&len, sizeof(uint16_t), 1, file);

  col_val->is_array = true;
  col_val->array.array_size = len;
  col_val->array.array_value = calloc(len, sizeof(ColumnValue));

  ColumnDefinition base_def = *col_def;
  base_def.is_array = false;

  for (int i = 0; i < len; i++) {
    col_val->array.array_value[i].is_array = false;
    read_column_value(file, &col_val->array.array_value[i], &base_def);
  }
}

void read_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def) {
  uint16_t text_len, max_len, str_len;
  bool is_toast_pointer = false;

  if (col_val == NULL || file == NULL || col_def == NULL) {
    LOG_ERROR("Invalid input to read_column_value.\n");
    return;
  }

  if (col_def->is_array) {
    read_array_value(file, col_val, col_def);
    return;
  }  

  col_val->type = col_def->type;

  switch (col_def->type) {
    case TOK_T_CHAR:
      fread(&col_val->str_value[0], sizeof(char), 1, file);
      break;
    case TOK_T_INT:
    case TOK_T_SERIAL:
      fread(&col_val->int_value, sizeof(int64_t), 1, file);
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

    case TOK_T_UUID:
      {
        uint8_t binary_uuid[16];
        fread(binary_uuid, 16, 1, file);

        for (int i = 0; i < 16; i++) {
          col_val->str_value[i] = binary_uuid[i];
        }
        col_val->str_value[16] = '\0';
      }
      break;

    case TOK_T_DATE:
      fread(&col_val->date_value, sizeof(Date), 1, file);
      break;

    case TOK_T_TIME:
      fread(&col_val->time_value, sizeof(TimeStored), 1, file);
      break;

    case TOK_T_TIME_TZ:
      fread(&col_val->time_tz_value, sizeof(Time_TZ), 1, file);
      break;

    case TOK_T_DATETIME:
      fread(&col_val->datetime_value, sizeof(DateTime), 1, file);
      break;

    case TOK_T_DATETIME_TZ:
      fread(&col_val->datetime_tz_value, sizeof(DateTime_TZ), 1, file);
      break;

    case TOK_T_TIMESTAMP:
      fread(&col_val->timestamp_value, sizeof(Timestamp), 1, file);
      break;

    case TOK_T_TIMESTAMP_TZ:
      fread(&col_val->timestamp_tz_value, sizeof(Timestamp_TZ), 1, file);
      break;

    case TOK_T_INTERVAL:
      fread(&col_val->interval_value, sizeof(Interval), 1, file);
      break;

    case TOK_T_VARCHAR: {
      fread(&str_len, sizeof(uint16_t), 1, file);
      col_val->str_value = malloc(str_len + 1);
      fread(col_val->str_value, sizeof(char), str_len, file);
      col_val->str_value[str_len] = '\0';
      break;
    }

    case TOK_T_TEXT:
    case TOK_T_JSON:
    case TOK_T_BLOB: {
      fread(&is_toast_pointer, sizeof(bool), 1, file);
      if (!is_toast_pointer) {
        fread(&str_len, sizeof(uint16_t), 1, file);
        col_val->str_value = calloc(str_len + 1, sizeof(char));
        col_val->str_value[str_len] = '\0';  
        if (!col_val->str_value) {
          perror("malloc failed");
          abort();
        }
        fread(col_val->str_value, str_len, 1, file);
      } else {
        fread(&col_val->toast_object, sizeof(uint32_t), 1, file);
      }

      col_val->is_toast = is_toast_pointer;
      LOG_DEBUG("is_toast: %s (%u)", col_val->is_toast ? "true": "false", col_val->toast_object);
      break;
    }
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

  LOG_DEBUG("Updating pool %lu", page_number);
}

void write_array_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def) {
  if (!col_val || !file || !col_val->array.array_value || col_val->array.array_size == 0) return;

  uint16_t len = col_val->array.array_size;
  fwrite(&len, sizeof(uint16_t), 1, file);

  ColumnDefinition base_def = *col_def;
  base_def.is_array = false;

  for (int i = 0; i < len; i++) {
    ColumnValue* elem = &col_val->array.array_value[i];
    elem->is_array = false;
    write_column_value(file, elem, &base_def);
  }
}

uint32_t write_array_value_to_buffer(uint8_t* buffer, ColumnValue* col_val, ColumnDefinition* col_def) {
  if (!buffer || !col_val || !col_val->array.array_value || col_val->array.array_size == 0) {
    return 0;
  }

  uint32_t offset = 0;
  uint16_t len = col_val->array.array_size;

  memcpy(buffer + offset, &len, sizeof(uint16_t));
  offset += sizeof(uint16_t);

  ColumnDefinition base_def = *col_def;
  base_def.is_array = false;
  // base_def.type = col_val->array.array_type;

  for (int i = 0; i < len; i++) {
    ColumnValue elem = col_val->array.array_value[i];
    (&elem)->is_array = false;

    uint32_t written = write_column_value_to_buffer(buffer + offset, &elem, &base_def);
    offset += written;
  }

  return offset;
}


void write_column_value(FILE* file, ColumnValue* col_val, ColumnDefinition* col_def) {
  uint16_t text_len, str_len, max_len;
  bool is_toast_pointer = false;

  if (col_val == NULL || file == NULL) {
    LOG_ERROR("Invalid column value or file pointer.\n");
    return;
  }

  if (col_val->is_array && col_def->is_array) {
    write_array_value(file, col_val, col_def);
    return;
  }
  
  switch (col_def->type) {
    case TOK_T_CHAR:
      fwrite(&col_val->str_value[0], sizeof(char), 1, file);
      break;
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      fwrite(&col_val->int_value, sizeof(int64_t), 1, file);
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

    case TOK_T_DATE:
      fwrite(&col_val->date_value, sizeof(Date), 1, file);
      break;

    case TOK_T_TIME:
      fwrite(&col_val->time_value, sizeof(TimeStored), 1, file);
      break;

    case TOK_T_TIME_TZ:
      fwrite(&col_val->time_tz_value, sizeof(Time_TZ), 1, file);
      break;

    case TOK_T_DATETIME:
      fwrite(&col_val->datetime_value, sizeof(DateTime), 1, file);
      break;

    case TOK_T_DATETIME_TZ:
      fwrite(&col_val->datetime_tz_value, sizeof(DateTime_TZ), 1, file);
      break;

    case TOK_T_TIMESTAMP:
      fwrite(&col_val->timestamp_value, sizeof(Timestamp), 1, file);
      break;

    case TOK_T_TIMESTAMP_TZ:
      fwrite(&col_val->timestamp_tz_value, sizeof(Timestamp_TZ), 1, file);
      break;

    case TOK_T_INTERVAL:
      fwrite(&col_val->interval_value, sizeof(Interval), 1, file);
      break;

    case TOK_T_VARCHAR:
      str_len = (uint16_t)strlen(col_val->str_value);
      max_len = (col_def->type_varchar == 0) ? 255 : col_def->type_varchar;

      if (str_len > max_len) {
        LOG_ERROR("Definition expects VARCHAR(<=%d) got VARCHAR(<=%d)", max_len, str_len);
        break;
      }

      fwrite(&str_len, sizeof(uint16_t), 1, file);
      fwrite(col_val->str_value, str_len, 1, file);
      break;
          
    case TOK_T_TEXT:
    case TOK_T_JSON:
    case TOK_T_BLOB: {
      is_toast_pointer = col_val->is_toast;
      fwrite(&is_toast_pointer, sizeof(bool), 1, file);
      if (!is_toast_pointer) {
        str_len = (uint16_t)strlen(col_val->str_value);
        fwrite(&str_len, sizeof(uint16_t), 1, file);
        fwrite(col_val->str_value, str_len, 1, file);
      } else {
        fwrite(&col_val->toast_object, sizeof(uint32_t), 1, file); 
      }
      break;
    }

    default:
      LOG_ERROR("Error: Unsupported data type.\n");
      break;
  }
}

uint32_t write_column_value_to_buffer(uint8_t* buffer, ColumnValue* col_val, ColumnDefinition* col_def) {
  if (!buffer || !col_val || !col_def) {
    LOG_ERROR("Invalid column value or buffer.\n");
    return 0;
  }

  uint32_t offset = 0;
  uint16_t str_len, max_len;
  bool is_toast_pointer = false;

  if (col_val->is_array && col_def->is_array) {
    return write_array_value_to_buffer(buffer, col_val, col_def);
  }

  switch (col_def->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      memcpy(buffer + offset, &col_val->int_value, sizeof(int64_t));
      offset += sizeof(int64_t);
      break;

    case TOK_T_BOOL: {
      memcpy(buffer + offset, &col_val->bool_value, sizeof(bool));
      offset += sizeof(bool);
      break;
    }

    case TOK_T_FLOAT:
      memcpy(buffer + offset, &col_val->float_value, sizeof(float));
      offset += sizeof(float);
      break;

    case TOK_T_DOUBLE:
      memcpy(buffer + offset, &col_val->double_value, sizeof(double));
      offset += sizeof(double);
      break;

    case TOK_T_DECIMAL:
      memcpy(buffer + offset, &col_val->decimal.precision, sizeof(int));
      offset += sizeof(int);
      memcpy(buffer + offset, &col_val->decimal.scale, sizeof(int));
      offset += sizeof(int);
      memcpy(buffer + offset, col_val->decimal.decimal_value, MAX_DECIMAL_LEN);
      offset += MAX_DECIMAL_LEN;
      break;

    case TOK_T_UUID: {
      size_t uuid_len = strlen(col_val->str_value);
      if (uuid_len == 36) {
        uint8_t binary_uuid[16];
        if (!parser_parse_uuid_string(col_val->str_value, binary_uuid)) {
          LOG_ERROR("Error: Invalid UUID format.\n");
          return 0;
        }
        memcpy(buffer + offset, binary_uuid, 16);
        offset += 16;
      } else if (uuid_len == 16) {
        memcpy(buffer + offset, col_val->str_value, 16);
        offset += 16;
      } else {
        LOG_ERROR("Invalid UUID length.\n");
        return 0;
      }
      break;
    }

    case TOK_T_DATE:
      memcpy(buffer + offset, &col_val->date_value, sizeof(Date));
      offset += sizeof(Date);
      break;

    case TOK_T_TIME:
      memcpy(buffer + offset, &col_val->time_value, sizeof(TimeStored));
      offset += sizeof(TimeStored);
      break;

    case TOK_T_TIME_TZ:
      memcpy(buffer + offset, &col_val->time_tz_value, sizeof(Time_TZ));
      offset += sizeof(Time_TZ);
      break;

    case TOK_T_DATETIME:
      memcpy(buffer + offset, &col_val->datetime_value, sizeof(DateTime));
      offset += sizeof(DateTime);
      break;

    case TOK_T_DATETIME_TZ:
      memcpy(buffer + offset, &col_val->datetime_tz_value, sizeof(DateTime_TZ));
      offset += sizeof(DateTime_TZ);
      break;

    case TOK_T_TIMESTAMP:
      memcpy(buffer + offset, &col_val->timestamp_value, sizeof(Timestamp));
      offset += sizeof(Timestamp);
      break;

    case TOK_T_TIMESTAMP_TZ:
      memcpy(buffer + offset, &col_val->timestamp_tz_value, sizeof(Timestamp_TZ));
      offset += sizeof(Timestamp_TZ);
      break;

    case TOK_T_INTERVAL:
      memcpy(buffer + offset, &col_val->interval_value, sizeof(Interval));
      offset += sizeof(Interval);
      break;

    case TOK_T_VARCHAR:
    case TOK_T_CHAR: {
      str_len = (uint16_t)strlen(col_val->str_value);
      max_len = (col_def->type_varchar == 0) ? 255 : col_def->type_varchar;

      if (str_len > max_len) {
        LOG_ERROR("Definition expects VARCHAR(<=%d) got VARCHAR(<=%d)", max_len, str_len);
        break;
      }

      memcpy(buffer + offset, &str_len, sizeof(uint16_t));
      offset += sizeof(uint16_t);

      memcpy(buffer + offset, col_val->str_value, str_len);
      offset += str_len;
      break;
    }

    case TOK_T_TEXT:
    case TOK_T_JSON:
    case TOK_T_BLOB: {
      is_toast_pointer = col_val->is_toast;
      memcpy(buffer + offset, &is_toast_pointer, sizeof(bool));
      offset += sizeof(bool);

      if (!is_toast_pointer) {
        str_len = (uint16_t)strlen(col_val->str_value);
        memcpy(buffer + offset, &str_len, sizeof(uint16_t));
        offset += sizeof(uint16_t);

        memcpy(buffer + offset, col_val->str_value, str_len);
        offset += str_len;
      } else {
        memcpy(buffer + offset, &col_val->toast_object, sizeof(uint32_t));
        offset += sizeof(uint32_t);
      }
      break;
    }

    default:
      LOG_ERROR("Error: Unsupported data type. %d\n", col_def->type);
      return 0;
  }

  return offset;
}


RowID serialize_insert(BufferPool* pool, Row row, TableCatalogEntry tc) {
  Page* page = NULL;

  if (pool->num_pages == POOL_SIZE) {
    pop_lru_page(pool, tc);
    return serialize_insert(pool, row, tc);
  }

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
        page = page_init(pool->next_pg_no);

        pool->pages[i] = page;
        pool->page_numbers[i] = pool->next_pg_no;

        pool->num_pages++;
        pool->next_pg_no++;
        
        break;
      }
    }
  }

  if (page->free_space <= row.row_length) {
    page->is_full = true; // old
    page = page_init(pool->next_pg_no);

    pool->pages[pool->num_pages] = page;
    pool->page_numbers[pool->num_pages] = pool->next_pg_no;  

    pool->num_pages++;
    pool->next_pg_no++;
  } 

  row.id.row_id = page->num_rows + 1;
  row.id.page_id = page->page_id;

  page->rows[page->num_rows] = row;

  page->num_rows++;
  page->free_space -= row.row_length;
  page->is_dirty = true;

  return (RowID){ row.id.page_id, row.id.row_id };
}

uint32_t row_to_buffer(RowID* row_id, BufferPool* pool, TableSchema* schema, uint8_t* buffer) {
  if (!row_id || !pool || !buffer) return 0;

  Row* row = &(pool->pages[row_id->page_id]->rows[row_id->row_id - 1]);
  
  uint32_t offset = 0;

  memcpy(buffer + offset, &row->id.page_id, sizeof(row->id.page_id));
  offset += sizeof(row->id.page_id);

  memcpy(buffer + offset, &row->id.row_id, sizeof(row->id.row_id));
  offset += sizeof(row->id.row_id);

  memcpy(buffer + offset, &row->row_length, sizeof(row->row_length));
  offset += sizeof(row->row_length);

  memcpy(buffer + offset, &row->null_bitmap_size, sizeof(row->null_bitmap_size));
  offset += sizeof(row->null_bitmap_size);

  if (row->null_bitmap && row->null_bitmap_size > 0) {
    memcpy(buffer + offset, row->null_bitmap, row->null_bitmap_size);
    offset += row->null_bitmap_size;
  }

  for (int j = 0; j < schema->column_count; j++) {
    ColumnDefinition* col_def = &schema->columns[j];
    if (!row->values[j].is_null && col_def) {
      offset += write_column_value_to_buffer(buffer + offset, &row->values[j], col_def);
    }
  }

  return offset; 
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
    row->deleted = true;
    page->is_dirty = true;

    LOG_DEBUG("serialize_delete: Deleted row from page %u, slot %u", rid.page_id, rid.row_id);
    return true;
  }

  LOG_ERROR("serialize_delete: Page %u not found", rid.page_id);
  return false;
}

void pop_lru_page(BufferPool* pool, TableCatalogEntry tc) {
  if (pool->num_pages == 0) return;

  Page* lru_page = pool->pages[0];
  FILE* file = fopen(pool->file, "ab");

  if (lru_page->is_dirty) {
    write_page(file, lru_page->page_id, lru_page, tc); 
  }

  fclose(file);
  free(lru_page);

  for (int i = 1; i < pool->num_pages; i++) {
    pool->pages[i - 1] = pool->pages[i];
    pool->page_numbers[i - 1] = pool->page_numbers[i];
  }

  pool->pages[pool->num_pages - 1] = NULL;
  pool->page_numbers[pool->num_pages - 1] = 0;
  pool->num_pages--;

  uint32_t max_pg_n = 0;
  for (int i = 0; i < pool->num_pages; i++) {
    if (pool->page_numbers[i] > max_pg_n) {
      max_pg_n = pool->page_numbers[i];
    }
  }
  pool->next_pg_no = max_pg_n + 1;
}
