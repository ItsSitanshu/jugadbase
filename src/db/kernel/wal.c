#include "kernel/kernel.h"

void write_update_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  uint16_t* col_indices, ColumnValue* old_values, ColumnValue* new_values, 
  uint16_t num_columns, TableSchema* schema) {
  /**
  WAL Update Format:
  [2B] Page Index
  [2B] Row Index
  [2B] Number of Updated Columns
  
  For each updated column:
    [2B] Column Index
    [4B] Old Value Size
    [var] Old Value Data
    [4B] New Value Size
    [var] New Value Data
  */

  uint32_t header_size = sizeof(uint16_t) * 3;  // page_idx, row_idx, num_columns
  uint32_t data_size = 0;
  uint8_t temp_buf[1024];
  
  for (int i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[col_indices[i]];
    uint32_t old_val_size = write_column_value_to_buffer(temp_buf, &old_values[i], def);
    uint32_t new_val_size = write_column_value_to_buffer(temp_buf, &new_values[i], def);
    
    data_size += sizeof(uint16_t) +          // Column index
                  sizeof(uint32_t) * 2 +      // Old and new value sizes
                  old_val_size + new_val_size; // Actual values
  }
  
  uint32_t total_size = header_size + data_size;
  uint8_t* wal_buf = malloc(total_size);
  if (!wal_buf) {
    return; 
  }
  
  uint32_t offset = 0;
  memcpy(wal_buf + offset, &page_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &row_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &num_columns, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  for (int i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[col_indices[i]];
    
    memcpy(wal_buf + offset, &col_indices[i], sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    uint32_t old_val_size = write_column_value_to_buffer(temp_buf, &old_values[i], def);
    memcpy(wal_buf + offset, &old_val_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(wal_buf + offset, temp_buf, old_val_size);
    offset += old_val_size;
    
    uint32_t new_val_size = write_column_value_to_buffer(temp_buf, &new_values[i], def);
    memcpy(wal_buf + offset, &new_val_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(wal_buf + offset, temp_buf, new_val_size);
    offset += new_val_size;
  }
  
  wal_write(wal, WAL_UPDATE, schema_idx, wal_buf, total_size);
  
  free(wal_buf);
}

void write_delete_wal(FILE* wal, uint8_t schema_idx, uint16_t page_idx, uint16_t row_idx, 
  Row* row, TableSchema* schema) {
  /**
  WAL Delete Format:
  [2B] Page Index
  [2B] Row Index
  [2B] Number of Columns
  [2B] Null Bitmap
  
  For each column:
    [2B] Column Index
    [4B] Value Size
    [var] Value Data
  */

  uint32_t header_size = sizeof(uint16_t) * 4;  // page_idx, row_idx, num_columns, null_bitmap
  uint32_t data_size = 0;
  uint8_t temp_buf[1024];
  uint16_t num_columns = schema->column_count;
  
  for (uint16_t i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[i];
    uint32_t val_size = write_column_value_to_buffer(temp_buf, &row->values[i], def);
    
    data_size += sizeof(uint16_t) +  // Column index
                 sizeof(uint32_t) +  // Value size
                 val_size;           // Actual value
  }
  
  uint32_t total_size = header_size + data_size;
  uint8_t* wal_buf = malloc(total_size);
  if (!wal_buf) {
    return; 
  }
  
  uint32_t offset = 0;
  
  memcpy(wal_buf + offset, &page_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &row_idx, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &num_columns, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  memcpy(wal_buf + offset, &row->null_bitmap, sizeof(uint16_t));
  offset += sizeof(uint16_t);
  
  for (uint16_t i = 0; i < num_columns; i++) {
    ColumnDefinition* def = &schema->columns[i];
    
    memcpy(wal_buf + offset, &i, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    uint32_t val_size = write_column_value_to_buffer(temp_buf, &row->values[i], def);
    memcpy(wal_buf + offset, &val_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(wal_buf + offset, temp_buf, val_size);
    offset += val_size;
  }
  
  wal_write(wal, WAL_DELETE, schema_idx, wal_buf, total_size);
  
  free(wal_buf);
}