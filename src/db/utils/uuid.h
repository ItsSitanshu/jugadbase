#ifndef UUID_H
#define UUID_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

char* uuid() {
  srand(time(NULL));

  unsigned int num1 = rand();
  unsigned int num2 = rand();
  unsigned int num3 = rand();
  unsigned int num4 = rand();

  char uuid_str[37];
  sprintf(uuid_str, "%08x-%04x-%04x-%04x-%04x%08x", num1, num2 >> 16, num2 & 0xFFFF, num3 >> 16, num3 & 0xFFFF, num4);

  return strdup(uuid_str);
}

#endif // UUID_H