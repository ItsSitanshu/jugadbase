#ifndef BIT128_H
#define BIT128_H


#include <stdint.h>
#include <stdlib.h>

typedef struct {
  uint64_t high;
  uint64_t low;
} uint128_t;

typedef struct {
  int64_t high;
  uint64_t low;
} int128_t;


#define strtouint128(str, result) \
do { \
  (result).high = 0; \
  (result).low = 0; \
  const char *ptr = str; \
  int len = 0; \
  while (*ptr >= '0' && *ptr <= '9') { \
    if (len < 19) { \
      (result).high = (result).high * 10 + (*ptr - '0'); \
    } else { \
      (result).low = (result).low * 10 + (*ptr - '0'); \
    } \
    len++; \
    ptr++; \
  } \
} while (0)



#define strtoint128(str, result) \
do { \
  int is_negative = 0; \
  if (*(str) == '-') { \
    is_negative = 1; \
    (str)++; \
  } \
  (result).high = 0; \
  (result).low = 0; \
  int i; \
  int high_digits = 0; \
  for (i = 0; (str)[i] != '\0'; i++) { \
    if (i >= 39) { \
      fprintf(stderr, "Input string too long\n"); \
      exit(EXIT_FAILURE); \
    } \
    if ((str)[i] < '0' || (str)[i] > '9') { \
      fprintf(stderr, "Invalid character in input string\n"); \
      exit(EXIT_FAILURE); \
    } \
    if ((str)[i] != '0') { \
      high_digits = i + 1; \
    } \
  } \
  for (i = 0; i < high_digits - 19; i++) { \
      (result).high = (result).high * 10 + ((str)[i] - '0'); \
  } \
  for (; i < high_digits; i++) { \
      (result).low = (result).low * 10 + ((str)[i] - '0'); \
  } \
  if (is_negative) { \
    if ((result).high != 0 || (result).low != 0) { \
      if ((result).low != 0) { \
        (result).low = -(result).low; \
        (result).high = ~(result).high; \
        if ((result).low == 0) { \
          (result).high++; \
        } else { \
            (result).high = -(result).high; \
        } \
      }\
    }\
  } \
} while (0)


#endif // BIT128_H