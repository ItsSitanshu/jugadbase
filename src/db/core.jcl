-- jb.core - version b.0.2

CREATE TABLE jb_tables (
  id INT,
  name TEXT NOT NULL, 
  database_name TEXT,
  owner TEXT DEFAULT 'public',
  created_at TIMESTAMP
);

CREATE TABLE jb_sequences (
  id INT,
  name TEXT,
  current_value INT,
  increment_by INT,
  min_value INT,
  max_value INT,
  cycle BOOL
);

CREATE TABLE jb_attribute (
  id SERIAL,
  table_id INT,
  column_name TEXT,
  data_type INT,
  ordinal_position INT,
  is_nullable BOOL,
  has_default BOOL,
  has_constraints BOOL,
  created_at TIMESTAMP
);

CREATE TABLE jb_attrdef (
  id SERIAL,
  table_id INT NOT NULL,
  column_name TEXT NOT NULL,
  default_expr TEXT NOT NULL,
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


CREATE TABLE jb_constraints (
  id SERIAL,
  table_id INT,
  columns INT[],
  name TEXT,
  constraint_type INT,
  check_expr TEXT,
  ref_table TEXT,
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
ALTER TABLE jb_sequences ADD CONSTRAINT jb_sequences_name_unique UNIQUE (name);

ALTER TABLE jb_indexes ADD CONSTRAINT jb_indexes_id_pk PRIMKEY (id);
ALTER TABLE jb_indexes ADD CONSTRAINT jb_indexes_tbable_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;

ALTER TABLE jb_constraints ADD CONSTRAINT jb_constraints_id_pk PRIMKEY (id);
ALTER TABLE jb_constraints ADD CONSTRAINT jb_constraints_table_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;

ALTER TABLE jb_attrdef ADD CONSTRAINT jb_attrdef_id_pk PRIMKEY (id);

ALTER TABLE jb_roles ADD CONSTRAINT jb_roles_id_pk PRIMKEY (id);
ALTER TABLE jb_roles ADD CONSTRAINT jb_roles_name_unique UNIQUE (name);

ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_role_member_pk PRIMKEY (role_id, member_id);
ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_role_id_fk FRNKEY (role_id) REFERENCES jb_roles(id);
ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_member_id_fk FRNKEY (member_id) REFERENCES jb_roles(id);

ALTER TABLE jb_sequences ALTER COLUMN name SET NOT NULL;
ALTER TABLE jb_sequences ALTER COLUMN current_value SET DEFAULT 1;
ALTER TABLE jb_sequences ALTER COLUMN increment_by SET DEFAULT 1;
ALTER TABLE jb_sequences ALTER COLUMN min_value SET DEFAULT 1;
ALTER TABLE jb_sequences ALTER COLUMN cycle SET DEFAULT FALSE;