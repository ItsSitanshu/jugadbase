#ifndef ROLES_H
#define ROLES_H

#include <stdbool.h>

#include "utils/xmem.h"

typedef struct Database Database;
typedef struct ClusterManager ClusterManager;

typedef struct Role {
  int id;
  char* name;
  bool is_superuser;
  bool can_create_db;
  bool can_create_user;
} Role;

int create_role(Database* db, const char* name, bool super, bool cdb, bool cuser);
Role* get_role_by_name(Database* db, const char* name);
bool has_privilege(Database* db, const char* username, const char* privilege);
int register_role(Database* db, const char* name, const char* password);
Role* login_role(Database* db, const char* name, const char* password);
Database* get_internal_meta_db(ClusterManager* manager);

#endif // ROLES_H