-- This file is a short version of tests/frontend/org/voltdb/fullddlfeatures/fullDDL.sql
-- It only contains DDLs that SQLcmd accepts. 
-- It is used for testing canonical DDL that roundtrips through SQLcmd and AdHoc.
-- You can add more test cases when we support more DDLs in SQLcmd.

-- CREATE INDEX
-- basic features

CREATE TABLE T1
(
    width INTEGER
,   length INTEGER
,   volume INTEGER
);

CREATE INDEX area 
ON 
T1 
(
    width * length
);

-- hash index

CREATE TABLE T3
(
    val INTEGER
,   str VARCHAR(30)
,   id INTEGER
);

CREATE INDEX abs_Hash_idx 
ON 
T3 
(
    ABS(val)
);

CREATE INDEX nomeaninghashweirdidx 
ON 
T3 
(
    ABS(id)
);

-- function in index definition

CREATE INDEX strMatch 
ON 
T3 
(
    FIELD 
    (
        str
    ,   'arbitrary'
    )
,   id
);


-- CREATE TABLE
-- test all supported SQL datatypes

CREATE TABLE T4 
(
    C1 TINYINT DEFAULT 127 NOT NULL
,   C2 SMALLINT DEFAULT 32767 NOT NULL
,   C3 INTEGER DEFAULT 2147483647 NOT NULL
,   C4 BIGINT NOT NULL
,   C5 FLOAT NOT NULL
,   C6 DECIMAL NOT NULL
,   C7 VARCHAR(32) NOT NULL
,   C8 VARBINARY(32) NOT NULL
,   C9 TIMESTAMP DEFAULT NOW NOT NULL
,   C10 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,   PRIMARY KEY 
    (
        C1
    ,   C9
    )
);

-- test maximum varchar size

CREATE TABLE T5 
(
    C VARCHAR(1048576 BYTES)
);

CREATE TABLE T6 
(
    C VARCHAR(262144)
);

-- test maximum varbinary size

CREATE TABLE T7 
(
    C VARBINARY(1048576)
);

-- test maximum limit partition rows

CREATE TABLE T8 
(   
    C INTEGER
,   LIMIT PARTITION ROWS 2147483647
);

-- column constraint

CREATE TABLE T9 
(   
    C1 INTEGER PRIMARY KEY NOT NULL
,   C2 SMALLINT UNIQUE NOT NULL
);

CREATE TABLE T10 
(
    C INTEGER DEFAULT 123 NOT NULL
,   CONSTRAINT con UNIQUE
    (
        C
    )
);

CREATE TABLE T11 
(
    C INTEGER DEFAULT 123 NOT NULL
,   CONSTRAINT pk1 PRIMARY KEY
    (
        C
    )
);

-- table constraints

CREATE TABLE T13 
(   
    C INTEGER
,   CONSTRAINT pk2 PRIMARY KEY
    (
        C
    )
);

CREATE TABLE T14 
(
    C INTEGER
,   CONSTRAINT uni1 UNIQUE
    (
        C
    )
);

CREATE TABLE T16 
(   
    C INTEGER
,   CONSTRAINT lpr1 LIMIT PARTITION ROWS 1
);

-- table constraint without keyword

CREATE TABLE T17 
(
    C INTEGER
,   PRIMARY KEY
    (   
        C
    )
);

CREATE TABLE T18 
(   
    C INTEGER
,   UNIQUE
    (   
        C
    )
);


CREATE TABLE T20 
(
    C INTEGER
,   LIMIT PARTITION ROWS 123
);


-- both column and table constraints

CREATE TABLE T22 
(
    C1 TINYINT DEFAULT 127 NOT NULL UNIQUE
,   C2 SMALLINT DEFAULT 32767 NOT NULL
,   C3 INTEGER DEFAULT 2147483647 NOT NULL
,   C4 BIGINT NOT NULL
,   C5 FLOAT NOT NULL
,   C6 DECIMAL UNIQUE NOT NULL
,   C7 VARCHAR(32) NOT NULL
,   C8 VARBINARY(32) NOT NULL
,   C9 TIMESTAMP DEFAULT NOW NOT NULL
,   C10 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,   UNIQUE 
    (
        C1
    ,   C9
    )
);

CREATE TABLE T23 
(
    C1 INTEGER NOT NULL
,   C2 SMALLINT UNIQUE
,   C3 VARCHAR(32) NOT NULL
,   C4 TINYINT NOT NULL
,   C5 TIMESTAMP NOT NULL
,   C6 BIGINT NOT NULL
,   C7 FLOAT NOT NULL
,   C8 DECIMAL NOT NULL
,   C9 INTEGER
,   CONSTRAINT hash_pk PRIMARY KEY 
    (
        C1
    ,   C5
    )
,   CONSTRAINT uni2 UNIQUE
    (
        C1
    ,   C7
    ), 
    CONSTRAINT lpr2 LIMIT PARTITION ROWS 123
);


-- CREATE VIEW

CREATE TABLE T24 
(
    C1 INTEGER
,   C2 INTEGER
);

-- Verify that the sqlcmd parsing survives two consecutive views
CREATE VIEW VT1 
(
    C1
,   C2
,   TOTAL
) 
AS 
    SELECT C1
        ,  C2
        ,  COUNT(*) 
    FROM T24 
    GROUP BY C1
          ,  C2
;

CREATE VIEW VT2 
(
    C1
,   C2
,   TOTAL
,   SUMUP
) 
AS 
    SELECT C1
        ,  C2
        ,  COUNT(*)
        ,  SUM(C2) 
    AS 
        newTble 
    FROM T24 
    WHERE T24.C1 < 1000 
    GROUP BY C1
          ,  C2
;

-- CREATE PROCEDURE
-- Verify that the sqlcmd parsing survives two consecutive create procedures

CREATE TABLE T25
(
    C1 BIGINT
,   C2 BIGINT
);

CREATE PROCEDURE FOO1 AS SELECT * FROM T25;
CREATE PROCEDURE FOO2 AS SELECT COUNT(*) FROM T25;

-- Verify that consecutive procedure/view statements survive sqlcmd parsing
CREATE PROCEDURE FOO3 AS SELECT * FROM T25;

CREATE VIEW VT3 
(
    C1
,   C2
,   TOTAL
) 
AS 
    SELECT C1
        ,  C2
        ,  COUNT(*) 
    FROM T25 
    GROUP BY C1
          ,  C2
;

CREATE PROCEDURE FOO4 AS SELECT * FROM VT3;

-- Verify that create procedure with INSERT INTO SELECT
-- survives sqlcmd 
CREATE PROCEDURE INS_T1_SELECT_T1 AS 
    INSERT INTO T1 SELECT * FROM T1;

CREATE PROCEDURE INS_T1_COLS_SELECT_T1 AS 
    INSERT INTO T1 (WIDTH, LENGTH, VOLUME) 
        SELECT WIDTH, LENGTH, VOLUME FROM T1;

