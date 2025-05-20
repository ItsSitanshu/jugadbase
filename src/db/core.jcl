-- jb.core - version b.0.1
-- This is the core metadata schema for the JugadBase system. It defines system catalog tables 
-- that track user-defined tables, columns, indexes, constraints, sequences, and access control.
-- 
-- - jb_tables: Stores metadata for all user tables including their name, schema, owner, and creation time.
-- - jb_columns: Describes each column of every table, including type, nullability, default, and order.
-- - jb_indexes: Contains definitions for indexes on tables, including uniqueness, involved columns, and type.
-- - jb_constraints: Stores CHECK, FOREIGN KEY, and UNIQUE constraints, with metadata like referenced table/columns.
-- - jb_sequences: Manages metadata for all sequences used for generating serial/incrementing values.
-- - jb_roles: Defines users or roles in the system along with privileges like superuser or create-db rights.
-- - jb_role_members: Maps role memberships (i.e., role inheritance) with self-referencing foreign keys on jb_roles.
-- 
-- Constraints such as PRIMARY KEY (PRIMKEY), FOREIGN KEY (FRNKEY), and optional defaults are used to ensure
-- relational integrity. Arrays and expression columns are used for multi-column constraints and index definitions.

CREATE TABLE jb_tables (
  id SERIAL PRIMKEY,
  name TEXT NOT NULL UNIQUE,
  database_name TEXT DEFAULT 'public',
  owner TEXT,
  created_at TIMESTAMP 
);

CREATE TABLE jb_sequences (
  id SERIAL PRIMKEY,
  name TEXT NOT NULL UNIQUE,
  current_value INT DEFAULT 1,
  increment_by INT DEFAULT 1,
  min_value INT DEFAULT 1,
  max_value INT,
  cycle BOOL DEFAULT false
);

CREATE TABLE jb_toast (
  id SERIAL,
  seq SERIAL,
  data TEXT
); 

CREATE TABLE jb_columns (
  id SERIAL PRIMKEY,
  table_id INT FRNKEY REF jb_tables(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  data_type TEXT NOT NULL,
  is_nullable BOOL DEFAULT true,
  is_array BOOL DEFAULT false,
  is_primary BOOL DEFAULT false,
  default_value TEXT,
  ordinal_position INT NOT NULL
);

CREATE TABLE jb_indexes (
  id SERIAL PRIMKEY,
  table_id INT FRNKEY REF jb_tables(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  columns TEXT[], 
  is_unique BOOL DEFAULT false,
  is_primary BOOL DEFAULT false,
  created_at TIMESTAMP 
);

CREATE TABLE jb_constraints (
  id SERIAL PRIMKEY,
  table_id INT FRNKEY REF jb_tables(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  constraint_type INT, 
  column_names TEXT[], 
  check_expr TEXT,     
  ref_table TEXT,      
  ref_columns TEXT[],  
  on_delete TEXT,
  on_update TEXT,
  is_deferrable BOOL DEFAULT false,
  is_deferred BOOL DEFAULT false,
  created_at TIMESTAMP
);

CREATE TABLE jb_attrdef (
  id SERIAL PRIMKEY,
  table_id INT NOT NULL,
  column_name TEXT NOT NULL,
  default_expr TEXT NOT NULL,
  seq_key INT FRNKEY REF jb_sequences(id),
  created_at TIMESTAMP
);

CREATE TABLE jb_roles (
  id SERIAL PRIMKEY,
  name TEXT NOT NULL UNIQUE,
  is_superuser BOOL DEFAULT false,
  can_create_db BOOL DEFAULT false,
  can_create_user BOOL DEFAULT false
);

CREATE TABLE jb_role_members (
  role_id INT PRIMKEY FRNKEY REF jb_roles(id),
  member_id INT PRIMKEY FRNKEY REF jb_roles(id),
);