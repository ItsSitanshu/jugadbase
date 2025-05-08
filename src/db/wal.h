#ifndef WAL_H
#define WAL_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>

typedef enum {
  WAL_INSERT = 1,
  WAL_UPDATE = 2,
  WAL_DELETE = 3
} WALAction;

typedef struct {
  uint64_t lsn;       
  uint64_t txid;      
  time_t timestamp;   
  uint8_t action;      
  uint32_t table_id;  
  uint32_t payload_size;
} WALRecordHeader;

FILE* wal_open(const char* filename, const char* mode);
void wal_close(FILE* file);
uint64_t wal_write(FILE* file, WALAction action, uint64_t txid, uint32_t table_id, const void* payload, uint32_t payload_size);
int wal_read(FILE* file, WALRecordHeader* header, void* payload);
void wal_replay(const char* filename); // Replays WAL for recovery

#endif // WAL_H
