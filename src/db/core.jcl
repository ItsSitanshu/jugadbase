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

CREATE TABLE jb_toast (
  id INT,
  seq INT,
  data TEXT
);

CREATE TABLE jb_columns (
  id INT,
  table_id INT,
  name TEXT,
  data_type TEXT,
  is_index BOOL,
  default_expr TEXT,
  seq_key INT,
  default_value TEXT,
  ordinal_position INT
);

CREATE TABLE jb_indexes (
  id INT,
  table_id INT,
  name TEXT,
  columns TEXT[],
  is_unique BOOL,
  is_primary BOOL,
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
  is_deferrable BOOL,
  is_deferred BOOL,
  is_nullable BOOL,
  is_primary BOOL,
  is_unique BOOL,
  created_at TIMESTAMP
);

CREATE TABLE jb_attrdef (
  id INT,
  table_id INT,
  column_name TEXT,
  created_at TIMESTAMP
);

CREATE TABLE jb_roles (
  id INT,
  name TEXT,
  is_superuser BOOL,
  can_create_db BOOL,
  can_create_user BOOL
);

CREATE TABLE jb_role_members (
  role_id INT,
  member_id INT
);

-- -- jb_tables
-- ALTER TABLE jb_tables ADD CONSTRAINT jb_tables_id_pk PRIMKEY(id);
-- ALTER TABLE jb_tables ADD CONSTRAINT jb_tables_name_unique UNIQUE (name);

-- -- jb_sequences
-- ALTER TABLE jb_sequences ADD CONSTRAINT jb_sequences_id_pk PRIMKEY (id);
-- ALTER TABLE jb_sequences ADD CONSTRAINT jb_sequences_name_unique UNIQUE (name);
-- ALTER TABLE jb_sequences ALTER COLUMN name SET NOT NULL;
-- ALTER TABLE jb_sequences ALTER COLUMN current_value SET DEFAULT 1;
-- ALTER TABLE jb_sequences ALTER COLUMN increment_by SET DEFAULT 1;
-- ALTER TABLE jb_sequences ALTER COLUMN min_value SET DEFAULT 1;
-- ALTER TABLE jb_sequences ALTER COLUMN cycle SET DEFAULT FALSE;

-- -- jb_columns
-- ALTER TABLE jb_columns ADD CONSTRAINT jb_columns_id_pk PRIMKEY (id);
-- ALTER TABLE jb_columns ADD CONSTRAINT jb_columns_table_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;
-- ALTER TABLE jb_columns ADD CONSTRAINT jb_columns_seq_key_fk FRNKEY (seq_key) REFERENCES jb_sequences(id);
-- ALTER TABLE jb_columns ALTER COLUMN name SET NOT NULL;
-- ALTER TABLE jb_columns ALTER COLUMN data_type SET NOT NULL;
-- ALTER TABLE jb_columns ALTER COLUMN ordinal_position SET NOT NULL;
-- ALTER TABLE jb_columns ALTER COLUMN is_index SET DEFAULT FALSE;
-- ALTER TABLE jb_columns ALTER COLUMN default_expr SET DEFAULT '';
-- ALTER TABLE jb_columns ALTER COLUMN default_value SET DEFAULT '';

-- -- jb_indexes
-- ALTER TABLE jb_indexes ADD CONSTRAINT jb_indexes_id_pk PRIMKEY (id);
-- ALTER TABLE jb_indexes ADD CONSTRAINT jb_indexes_table_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;
-- ALTER TABLE jb_indexes ALTER COLUMN name SET NOT NULL;
-- ALTER TABLE jb_indexes ALTER COLUMN is_unique SET DEFAULT FALSE;
-- ALTER TABLE jb_indexes ALTER COLUMN is_primary SET DEFAULT FALSE;

-- -- jb_constraints
-- ALTER TABLE jb_constraints ADD CONSTRAINT jb_constraints_id_pk PRIMKEY (id);
-- ALTER TABLE jb_constraints ADD CONSTRAINT jb_constraints_table_id_fk FRNKEY (table_id) REFERENCES jb_tables(id) ON DELETE CASCADE;
-- ALTER TABLE jb_constraints ALTER COLUMN is_deferrable SET DEFAULT FALSE;
-- ALTER TABLE jb_constraints ALTER COLUMN is_deferred SET DEFAULT FALSE;
-- ALTER TABLE jb_constraints ALTER COLUMN is_nullable SET DEFAULT TRUE;
-- ALTER TABLE jb_constraints ALTER COLUMN is_primary SET DEFAULT FALSE;
-- ALTER TABLE jb_constraints ALTER COLUMN is_unique SET DEFAULT FALSE;

-- -- jb_attrdef
-- ALTER TABLE jb_attrdef ADD CONSTRAINT jb_attrdef_id_pk PRIMKEY (id);
-- ALTER TABLE jb_attrdef ALTER COLUMN table_id SET NOT NULL;
-- ALTER TABLE jb_attrdef ALTER COLUMN column_name SET NOT NULL;

-- -- jb_roles
-- ALTER TABLE jb_roles ADD CONSTRAINT jb_roles_id_pk PRIMKEY (id);
-- ALTER TABLE jb_roles ADD CONSTRAINT jb_roles_name_unique UNIQUE (name);
-- ALTER TABLE jb_roles ALTER COLUMN name SET NOT NULL;
-- ALTER TABLE jb_roles ALTER COLUMN is_superuser SET DEFAULT FALSE;
-- ALTER TABLE jb_roles ALTER COLUMN can_create_db SET DEFAULT FALSE;
-- ALTER TABLE jb_roles ALTER COLUMN can_create_user SET DEFAULT FALSE;

-- -- jb_role_members
-- ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_role_member_pk PRIMKEY (role_id, member_id);
-- ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_role_id_fk FRNKEY (role_id) REFERENCES jb_roles(id);
-- ALTER TABLE jb_role_members ADD CONSTRAINT jb_role_members_member_id_fk FRNKEY (member_id) REFERENCES jb_roles(id);