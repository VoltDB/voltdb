-- Simple table to test partitioned save/restore
CREATE TABLE PARTITION_TESTER (
  PT_ID INTEGER DEFAULT '0' NOT NULL,
  PT_NAME VARCHAR(16) DEFAULT NULL,
  PT_INTVAL INTEGER DEFAULT NULL,
  PT_FLOATVAL FLOAT DEFAULT NULL,
  PT_POINTVAL GEOGRAPHY_POINT DEFAULT NULL,
  PT_GEOGVAL  GEOGRAPHY DEFAULT NULL,
  PRIMARY KEY (PT_ID)
);
-- PARTITION TABLE PARTITION_TESTER ON COLUMN PT_ID;

-- Simple table to test replicated save/restore
CREATE TABLE REPLICATED_TESTER (
  RT_ID INTEGER DEFAULT '0' NOT NULL,
  RT_NAME VARCHAR(32) DEFAULT NULL,
  RT_INTVAL INTEGER DEFAULT NULL,
  RT_FLOATVAL FLOAT DEFAULT NULL,
  PT_POINTVAL GEOGRAPHY_POINT DEFAULT NULL,
  PT_GEOGVAL  GEOGRAPHY DEFAULT NULL,
  PRIMARY KEY (RT_ID)
);

-- Single table, explicitly partitioned view. We snapshot this.
CREATE VIEW MATVIEW1 (PT_ID, PT_INTVAL, NUM) AS SELECT PT_ID, PT_INTVAL, COUNT(*) FROM PARTITION_TESTER GROUP BY PT_ID, PT_INTVAL;
-- Single table, randomly partitioned view. We do not snapshot this.
CREATE VIEW MATVIEW2 (PT_INTVAL, NUM) AS SELECT PT_INTVAL, COUNT(*) FROM PARTITION_TESTER GROUP BY PT_INTVAL;
-- Single table, no count(*), need to test we can round-trip the hidden column.
CREATE VIEW MATVIEW3 (PT_ID, PT_INTVAL, NUM) AS SELECT PT_ID, PT_INTVAL, SUM(PT_FLOATVAL) FROM PARTITION_TESTER GROUP BY PT_ID, PT_INTVAL;

-- TABLES BELOW HERE WILL ALL CHANGE IN saverestore-altered-ddl.sql

-- This table will become materialized
CREATE TABLE BECOMES_MATERIALIZED (
  PT_INTVAL INTEGER,
  NUM INTEGER,
  PRIMARY KEY (PT_INTVAL)
);

-- This table will cease to exist
CREATE TABLE GETS_REMOVED (
  ID INTEGER,
  INTVAL INTEGER,
  PRIMARY KEY (ID)
);

-- This table will get created
--CREATE TABLE GETS_CREATED (
--  ID INTEGER,
--  INTVAL INTEGER,
--  PRIMARY KEY (ID)
--);

-- This table will change columns
CREATE TABLE CHANGE_COLUMNS (
  ID INTEGER NOT NULL,
  BYEBYE INTEGER --, this column goes away
--  HASDEFAULT INTEGER DEFAULT '1234', --this column will appear
--  HASNULL INTEGER -- this column will appear
);
-- PARTITION TABLE CHANGE_COLUMNS ON COLUMN ID;

CREATE TABLE ENG_2025 (
 key    varchar(250) not null,
 value  varbinary(1048576) not null,
 PRIMARY KEY (key)
);

-- This table's columns change types
CREATE TABLE CHANGE_TYPES (
  ID INTEGER,
  BECOMES_INT TINYINT, -- this column becomes an int
  BECOMES_FLOAT INTEGER, -- this column becomes a float
  BECOMES_TINY INTEGER --this column becomes a tinyint
);

-- Table for super big rows that test max supported storage
CREATE TABLE JUMBO_ROW (
 PKEY          INTEGER      NOT NULL,
 STRING1       VARCHAR(1048576),
 STRING2       VARCHAR(1048564),
 PRIMARY KEY (PKEY)
);
-- PARTITION TABLE JUMBO_ROW ON COLUMN PKEY;

-- Table for super big rows that test max supported storage reached via multi-byte characters.
CREATE TABLE JUMBO_ROW_UTF8 (
 PKEY          INTEGER      NOT NULL,
 STRING1       VARCHAR(262144),
 STRING2       VARCHAR(262141),
 PRIMARY KEY (PKEY)
);
-- PARTITION TABLE JUMBO_ROW_UTF8 ON COLUMN PKEY;
