-- This file is just like int-DDL.sql, except:
--   1. None of the columns are declared NOT NULL, except the Primary Key.
--   2. The TINY column is declared as SMALLINT, rather than TINYINT.
-- Both of these make it easier to test UPSERT statements against PostgreSQL:
-- the former because of ENG-10449; the latter because PostgreSQL does not
-- support a TINYINT equivalent, which can cause some discrepancies.

CREATE TABLE P1 (
  ID INTEGER NOT NULL,
  TINY SMALLINT,
  SMALL SMALLINT,
  BIG BIGINT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  TINY SMALLINT,
  SMALL SMALLINT,
  BIG BIGINT,
  PRIMARY KEY (ID)
);
