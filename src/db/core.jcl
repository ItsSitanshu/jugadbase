-- jb.core - version b.0.4

INSERT _unsafecon INTO jb_tables (id, name, database_name, owner, created_at) VALUES
  (0, 'jb_tables', 'core', 'sudo', NOW()),
  (1, 'jb_sequences', 'core', 'sudo', NOW()),
  (2, 'jb_attribute', 'core', 'sudo', NOW()),
  (3, 'jb_attrdef', 'core', 'sudo', NOW());

INSERT _unsafecon INTO jb_sequences (id, name, current_value, increment_by, min_value, max_value, cycle) VALUES
  (0, 'jb_sequencesid', 1, 1, 0, NULL, false),
  (1, 'jb_tablesid', 3, 1, 0, NULL, false);
INSERT _unsafecon INTO jb_sequences (name, current_value, increment_by, min_value, max_value, cycle) VALUES
  ('jb_attributeid', 0, 1, 0, NULL, false),
  ('jb_attrdefid', 0, 1, 0, NULL, false);

INSERT _unsafecon INTO jb_attribute (table_id, column_name, data_type, ordinal_position, is_nullable, has_default, has_constraints, created_at) VALUES
(0, "id", 19, 0, true, false, false, NOW()),
(0, "name", 3, 1, false, false, false, NOW()),
(0, "database_name", 3, 2, true, false, false, NOW()),
(0, "owner", 3, 3, true, true, false, NOW()),
(0, "created_at", 13, 4, true, false, false, NOW());

INSERT _unsafecon INTO jb_attribute (table_id, column_name, data_type, ordinal_position, is_nullable, has_default, has_constraints, created_at) VALUES
(1, "id", 19, 0, true, false, false, NOW()),
(1, "name", 3, 1, true, false, false, NOW()),
(1, "current_value", 0, 2, false, true, false, NOW()),
(1, "increment_by", 0, 3, true, false, false, NOW()),
(1, "min_value", 0, 4, true, false, false, NOW()),
(1, "max_value", 0, 5, true, false, false, NOW()),
(1, "cycle", 4, 6, true, false, false, NOW());

INSERT _unsafecon INTO jb_attribute (table_id, column_name, data_type, ordinal_position, is_nullable, has_default, has_constraints, created_at) VALUES
(2, "id", 19, 0, true, false, false, NOW()),
(2, "table_id", 0, 1, true, false, false, NOW()),
(2, "column_name", 3, 2, true, false, false, NOW()),
(2, "data_type", 0, 3, true, false, false, NOW()),
(2, "ordinal_position", 0, 4, true, false, false, NOW()),
(2, "is_nullable", 4, 5, true, false, false, NOW()),
(2, "has_default", 4, 6, true, false, false, NOW()),
(2, "has_constraints", 4, 7, true, false, false, NOW()),
(2, "created_at", 13, 8, true, false, false, NOW());

INSERT _unsafecon INTO jb_attribute (table_id, column_name, data_type, ordinal_position, is_nullable, has_default, has_constraints, created_at) VALUES
(3, "id", 19, 0, true, false, false, NOW()),
(3, "table_id", 0, 1, false, false, false, NOW()),
(3, "column_name", 3, 2, false, false, false, NOW()),
(3, "default_expr", 3, 3, false, false, false, NOW()),
(3, "created_at", 13, 4, true, false, false, NOW());

CREATE _unsafecon TABLE jb_constraints (
  id SERIAL,
  table_id INT,
  columns TEXT[],
  name TEXT,
  constraint_type INT,
  check_expr TEXT,
  ref_table INT,
  ref_columns TEXT[],
  on_delete INT,
  on_update INT,
  is_deferrable BOOL DEFAULT false,
  is_deferred BOOL DEFAULT false,
  is_nullable BOOL DEFAULT true,
  is_primary BOOL DEFAULT false,
  is_unique BOOL DEFAULT false,
  created_at TIMESTAMP
);

CREATE TABLE jb_toast (
  id INT,
  seq INT,
  data TEXT
);

CREATE TABLE jb_indexes (
  id INT,
  table_id INT,
  name TEXT NOT NULL,
  columns TEXT[],
  is_unique BOOL DEFAULT false,
  is_primary BOOL DEFAULT false,
  created_at TIMESTAMP
);

CREATE TABLE jb_roles (
  id SERIAL,
  name TEXT NOT NULL,
  is_superuser BOOL DEFAULT false,
  can_create_db BOOL DEFAULT false,
  can_create_user BOOL DEFAULT false
);

CREATE TABLE jb_role_members (
  role_id INT,
  member_id INT
);

ALTER TABLE jb_tables ADD CONSTRAINT jb_tables_id_pk PRIMKEY (id);
ALTER TABLE jb_tables ADD CONSTRAINT jb_tables_name_unique UNIQUE (name);

ALTER TABLE jb_sequences ADD CONSTRAINT jb_sequences_id_pk PRIMKEY (id);
-- ALTER TABLE jb_sequences ADD CONSTRAINT jb_sequences_name_unique UNIQUE (name);

ALTER TABLE jb_indexes ADD CONSTRAINT jb_indexes_id_pk PRIMKEY (id);
ALTER TABLE jb_indexes ADD CONSTRAINT jb_indexes_tbable_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;

ALTER TABLE jb_constraints ADD CONSTRAINT jb_constraints_table_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;

ALTER TABLE jb_attrdef ADD CONSTRAINT jb_attrdef_id_pk PRIMKEY (id);

ALTER TABLE jb_roles ADD CONSTRAINT jb_roles_id_pk PRIMKEY (id);
ALTER TABLE jb_roles ADD CONSTRAINT jb_roles_name_unique UNIQUE (name);

ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_role_member_pk PRIMKEY (role_id, member_id);
ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_role_id_fk FRNKEY (role_id) REFERENCES jb_roles(id);
ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_member_id_fk FRNKEY (member_id) REFERENCES jb_roles(id);

ALTER TABLE jb_sequences ALTER COLUMN name SET NOT NULL;
-- ALTER TABLE jb_sequences ALTER COLUMN current_value SET DEFAULT 9;
-- ALTER TABLE jb_sequences ALTER COLUMN increment_by SET DEFAULT 1;
-- ALTER TABLE jb_sequences ALTER COLUMN min_value SET DEFAULT 1;
-- ALTER TABLE jb_sequences ALTER COLUMN cycle SET DEFAULT false;