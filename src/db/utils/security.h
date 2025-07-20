#ifndef SECURITY_H
#define SECURITY_H

#include <sodium.h>
#include <string.h> 

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

static int secure_hash_password(char hashed_password[crypto_pwhash_STRBYTES], const char* password) {
  if (crypto_pwhash_str(hashed_password, password, strlen(password),
                        crypto_pwhash_OPSLIMIT_MODERATE,
                        crypto_pwhash_MEMLIMIT_MODERATE) != 0) {
    return -1; // Hashing failed
  }
  return 0;
}


static int secure_verify_password(const char* hashed_password, const char* password) {
  if (crypto_pwhash_str_verify(hashed_password, password, strlen(password)) != 0) {
    return -1; // Password does not match
  }
  return 0;
}


static int secure_needs_rehash(const char* hashed_password) {
  return crypto_pwhash_str_needs_rehash(hashed_password,
                                        crypto_pwhash_OPSLIMIT_MODERATE,
                                        crypto_pwhash_MEMLIMIT_MODERATE);
}

#endif // SECURITY_H