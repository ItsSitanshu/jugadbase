#ifndef SECURITY_H
#define SECURITY_H

static unsigned int hash_fnv1a(const char* table_name, int MAX) {
  const unsigned int FNV_OFFSET_BASIS = 2166136261u;
  const unsigned int FNV_PRIME = 16777619u;

  unsigned int hash = FNV_OFFSET_BASIS;
  while (*table_name) {
    hash ^= (unsigned char)(*table_name++);
    hash *= FNV_PRIME;
  } 

  return hash % MAX;
}


#endif // SECURITY_H