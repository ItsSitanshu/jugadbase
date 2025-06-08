#include "wal.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

FILE* wal_open(const char* filename, const char* mode) {
  return fopen(filename, mode);
}

void wal_close(FILE* file) {
  if (file) fclose(file);
}

uint64_t wal_write(FILE* file, WALAction action, uint32_t table_id, const void* payload, uint32_t payload_size) {
  if (!file) return 0;

  static uint64_t lsn_counter = 1;
  uint64_t lsn = lsn_counter++;

  WALRecordHeader header;
  memset(&header, 0, sizeof(WALRecordHeader));
  
  header.lsn = lsn;
  header.txid = global_txid;
  header.timestamp = time(NULL);
  header.action = action;
  header.table_id = table_id;
  header.payload_size = payload_size;

  fwrite(&header, sizeof(WALRecordHeader), 1, file);
  if (payload_size > 0 && payload) {
    fwrite(payload, payload_size, 1, file);
  }

  fflush(file); 
  generate_txid();

  return lsn;
}

int wal_read(FILE* file, WALRecordHeader* header, void* payload) {
  if (!file || fread(header, sizeof(WALRecordHeader), 1, file) != 1) return 0;

  if (header->payload_size > 0 && payload) {
    fread(payload, header->payload_size, 1, file);
  }
  return 1;
}

void wal_replay(const char* filename) {
  FILE* file = wal_open(filename, "rb");
  if (!file) return;

  WALRecordHeader header;
  void* payload = malloc(1024); 

  printf("Replaying WAL: %s\n", filename);
  while (wal_read(file, &header, payload)) {
      printf("LSN: %lu, TXID: %lu, Action: %d, Table: %u, Payload Size: %u\n",
              header.lsn, header.txid, header.action, header.table_id, header.payload_size);
  }

  free(payload);
  wal_close(file);
}
