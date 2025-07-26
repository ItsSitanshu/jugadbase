#include "internal/roles.h"
#include "utils/security.h"

#include "kernel/kernel.h"
#include "storage/cluster.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int create_role(Database* db, const char* name, bool super, bool cdb, bool cuser) {
  char query[512];
  snprintf(query, sizeof(query),
    "INSERT INTO jb_roles (name, is_superuser, can_create_db, can_create_user) VALUES ('%s', %s, %s, %s);",
    name,
    super ? "true" : "false",
    cdb ? "true" : "false",
    cuser ? "true" : "false"
  );

  return process(db, query).exec.code;
}

Role* get_role_by_name(Database* db, const char* name) {
  char query[256];
  snprintf(query, sizeof(query), "SELECT id, name, is_superuser, can_create_db, can_create_user FROM jb_roles WHERE name = '%s';", name);

  Result res = process_silent(db, query);
  if (res.exec.row_count == 0 || res.exec.code != 0) return NULL;

  ExecutionResult* rs = &(res.exec);

  Role* r = malloc(sizeof(Role));
  r->id = rs->rows[0].values[0].int_value;
  r->name = strdup(rs->rows[0].values[1].str_value);
  r->is_superuser = rs->rows[0].values[2].bool_value;
  r->can_create_db = rs->rows[0].values[3].bool_value;
  r->can_create_user = rs->rows[0].values[4].bool_value;

  free_result(&res);
  return r;
}

bool has_privilege(Database* db, const char* username, const char* privilege) {
  Role* role = get_role_by_name(db, username);
  if (!role) return false;

  if (strcmp(privilege, "SUPERUSER") == 0) return role->is_superuser;
  if (strcmp(privilege, "CREATE_DB") == 0) return role->can_create_db;
  if (strcmp(privilege, "CREATE_USER") == 0) return role->can_create_user;

  return false;
}

Database* get_internal_meta_db(ClusterManager* manager) {
  if (!manager) return NULL;


  Database* user_db = cluster_get_active_db(manager);
  if (!user_db) return NULL;

  if (user_db && user_db->core == NULL) {
    LOG_FATAL("user_db->core is NULL");
  }

  return user_db->core;
}

int register_role(Database* db, const char* name, const char* password) {
  LOG_DEBUG("Password entered: %s", password);

  if (get_role_by_name(db, name)) {
    return -2;
  }

  char hashed[crypto_pwhash_STRBYTES];
  if (secure_hash_password(hashed, password) != 0) {
    return -1;
  }

  char sql[1024];
  snprintf(sql, sizeof(sql),
    "INSERT INTO jb_roles (name, hashed_password) VALUES ('%s','%s');",
    name, hashed
  );

  return process(db, sql).exec.code;
}

Role* login_role(Database* db, const char* name, const char* password) {
  char sql[512];
  snprintf(sql, sizeof(sql),
    "SELECT hashed_password FROM jb_roles WHERE name = '%s';",
    name
  );

  Result r = process_silent(db, sql);
  if (r.exec.code != 0 || r.exec.row_count == 0) {
    free_result(&r);
    return NULL;
  }

  const char* stored = r.exec.rows[0].values[0].str_value;
  if (secure_verify_password(stored, password) != 0) {
    free_result(&r);
    return NULL;
  }
  free_result(&r);

  return get_role_by_name(db, name);
}
