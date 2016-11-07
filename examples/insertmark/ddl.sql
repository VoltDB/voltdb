-- This statement makes this DDL idempotent (on an empty database)
DROP TABLE tbl IF EXISTS;

-- Simple, narrow, KV table
CREATE TABLE tbl (
 id bigint NOT NULL,
 val bigint DEFAULT NULL,
 PRIMARY KEY (id)
);
PARTITION TABLE tbl ON COLUMN id;
