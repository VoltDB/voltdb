CREATE TABLE nocapped (
id INTEGER NOT NULL,
wage INTEGER not null,
dept INTEGER NOT NULL
);

CREATE TABLE capped0 (
id INTEGER NOT NULL,
wage INTEGER not null,
dept INTEGER NOT NULL,
CONSTRAINT tblimit0 LIMIT PARTITION ROWS 0
);

CREATE TABLE capped2 (
id INTEGER NOT NULL,
wage INTEGER not null,
dept INTEGER NOT NULL,
CONSTRAINT tblimit2 LIMIT PARTITION ROWS 2
);

CREATE TABLE capped3 (
id INTEGER NOT NULL,
wage INTEGER not null,
dept INTEGER NOT NULL,
CONSTRAINT tblimit3 LIMIT PARTITION ROWS 3
);

CREATE TABLE capped3_limit_rows_exec (
purge_me TINYINT DEFAULT 0 NOT NULL,
wage INTEGER NOT NULL PRIMARY KEY,
dept INTEGER NOT NULL,
CONSTRAINT tblimit3_exec LIMIT PARTITION ROWS 3
  EXECUTE (DELETE FROM capped3_limit_rows_exec WHERE purge_me <> 0)
);

CREATE TABLE capped3_limit_exec_complex (
wage INTEGER NOT NULL,
dept INTEGER NOT NULL PRIMARY KEY,
may_be_purged TINYINT DEFAULT 0 NOT NULL,
relevance VARCHAR(255),
priority SMALLINT,
CONSTRAINT tblimit3_exec_complex LIMIT PARTITION ROWS 3
  EXECUTE (DELETE FROM capped3_limit_exec_complex
           WHERE may_be_purged = 1
-- hsql232 ENG-8325 IN LIST:           AND relevance IN ('irrelevant', 'worthless', 'moot')
           AND (relevance = 'irrelevant' -- required workaround hsql232 ENG-8325 IN LIST
             OR relevance = 'worthless'  -- required workaround hsql232 ENG-8325 IN LIST
             OR relevance = 'moot')      -- required workaround hsql232 ENG-8325 IN LIST
           AND priority < 16384)
);

-- DELETE statement above will use this index.
CREATE INDEX CAPPED3_COMPLEX_INDEX
       ON CAPPED3_LIMIT_EXEC_COMPLEX (may_be_purged);

CREATE TABLE events_capped (
  event_id VARCHAR(36) NOT NULL,
  when_occurred TIMESTAMP NOT NULL,
  info BIGINT,
  CONSTRAINT limit_5_delete_oldest
    LIMIT PARTITION ROWS 5
    EXECUTE (
      DELETE FROM events_capped
      ORDER BY when_occurred, event_id ASC LIMIT 1)
);
PARTITION TABLE events_capped ON COLUMN event_id;
CREATE UNIQUE INDEX events_capped_when_id
       ON events_capped (when_occurred, event_id);

CREATE TABLE events_capped_offset (
  event_id VARCHAR(36) NOT NULL,
  when_occurred TIMESTAMP NOT NULL,
  info BIGINT,
  CONSTRAINT limit_5_save_newest
    LIMIT PARTITION ROWS 5
    EXECUTE (
      DELETE FROM events_capped_offset
      ORDER BY when_occurred DESC, event_id ASC offset 1)
);
PARTITION TABLE events_capped_offset ON COLUMN event_id;
CREATE UNIQUE INDEX events_capped_offset_when_id
       ON events_capped_offset (when_occurred, event_id);

CREATE TABLE capped_truncate (
  i integer,
  CONSTRAINT limit_5_truncate LIMIT PARTITION ROWS 5
  EXECUTE (DELETE FROM capped_truncate)
);


CREATE TABLE RTABLE (
    ID INTEGER DEFAULT 0 NOT NULL,
    AGE INTEGER NOT NULL,
    WAGE FLOAT NOT NULL,
    NAME VARCHAR(32) DEFAULT NULL,
    CITY VARCHAR(300) DEFAULT NULL,
    CONSTRAINT PK_RTABLE PRIMARY KEY (ID)
);

CREATE INDEX RTABLE_INDEX_1 ON RTABLE (AGE);
CREATE INDEX RTABLE_INDEX_2 ON RTABLE (NAME);
CREATE INDEX RTABLE_INDEX_3 ON RTABLE (WAGE, substring(name, 0,1));

CREATE VIEW V_RTABLE (V_AGE, V_CITY, V_CNT, V_SUM_AGE, V_MIN_WAGE)
    AS SELECT AGE, CITY, COUNT(*), SUM(WAGE), MIN(WAGE)
    FROM RTABLE  GROUP BY AGE, CITY;


CREATE TABLE PTABLE (
    ID INTEGER DEFAULT 0 NOT NULL,
    AGE INTEGER NOT NULL,
    WAGE FLOAT NOT NULL,
    NAME VARCHAR(32) DEFAULT NULL,
    CITY VARCHAR(300) DEFAULT NULL,
    CONSTRAINT PK_PTABLE PRIMARY KEY (ID)
);
PARTITION TABLE PTABLE ON COLUMN ID;

CREATE INDEX PTABLE_INDEX_1 ON PTABLE (AGE);
CREATE INDEX PTABLE_INDEX_2 ON PTABLE (NAME);
CREATE INDEX PTABLE_INDEX_3 ON PTABLE (WAGE, substring(name, 0,1));

CREATE VIEW V_PTABLE (V_AGE, V_CITY, V_CNT, V_SUM_AGE, V_MIN_WAGE)
    AS SELECT AGE, CITY, COUNT(*), SUM(WAGE), MIN(WAGE)
    FROM PTABLE  GROUP BY AGE, CITY;
