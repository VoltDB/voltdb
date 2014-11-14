-- CREATE INDEX
-- basic features

CREATE TABLE T1
(
    width INTEGER
,   length INTEGER
,   volume INTEGER
);

CREATE UNIQUE INDEX area
ON
T1
(
    width * length
);

CREATE TABLE T2
(
    width INTEGER
,   length INTEGER
,   area INTEGER NOT NULL
,   volume INTEGER
);

PARTITION TABLE T2
ON
COLUMN
    area
;

CREATE ASSUMEUNIQUE INDEX absVal
ON
T2
(
    ABS(area * 2)
,   ABS(volume / 2)
);

-- hash index

CREATE TABLE T3
(
    val INTEGER
,   str VARCHAR(30)
,   id INTEGER
);

CREATE UNIQUE INDEX abs_Hash_idx
ON
T3
(
    ABS(val)
);

CREATE UNIQUE INDEX nomeaninghashweirdidx
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


-- CREATE ROLE
-- basic

CREATE ROLE guest;

CREATE ROLE admin
WITH
    sysproc
,   adhoc
,   defaultproc;


-- CREATE PROCEDURE AS
-- as sql stmt

CREATE TABLE User
(
    age INTEGER
,   name VARCHAR(20)
);

CREATE PROCEDURE p1
ALLOW
    admin
AS
    SELECT COUNT(*)
         , name
    FROM User
    WHERE age = ?
    GROUP BY name
;

CREATE PROCEDURE p2
ALLOW
    admin
AS
    INSERT INTO User
    VALUES (?, ?)
;

-- as source code

CREATE PROCEDURE p3
ALLOW
    admin
AS
    ###
    stmt = new SQLStmt('SELECT age, name FROM User WHERE age = ?')
    transactOn = { int key ->
                   voltQueueSQL(stmt,key)
                   voltExecuteSQL(true)
	             }
    ### LANGUAGE GROOVY
;


-- CREATE PROCEDURE FROM CLASS
-- basic

CREATE PROCEDURE
ALLOW
    admin
FROM CLASS
    org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc
;


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

CREATE TABLE T12
(
    C1 INTEGER NOT NULL
,   C2 INTEGER DEFAULT 123 NOT NULL
,   CONSTRAINT au ASSUMEUNIQUE
    (
        C2
    )
);
PARTITION TABLE T12 ON COLUMN C1;

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

CREATE TABLE T15 
(
    C INTEGER
,   C2 TINYINT NOT NULL
,   CONSTRAINT assumeuni ASSUMEUNIQUE
    (
        C
    )
);
PARTITION TABLE T15 ON COLUMN C2;

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

CREATE TABLE T19 
(
    C INTEGER
,   C2 TINYINT NOT NULL
,   ASSUMEUNIQUE
    (
        C
    )
);
PARTITION TABLE T19 ON COLUMN C2;

CREATE TABLE T20
(
    C INTEGER
,   LIMIT PARTITION ROWS 123
);


-- both column and table constraints

CREATE TABLE T21 
(
    C1 TINYINT DEFAULT 127 NOT NULL
,   C2 SMALLINT DEFAULT 32767 NOT NULL
,   C3 INTEGER DEFAULT 2147483647 NOT NULL
,   C4 BIGINT NOT NULL
,   C5 FLOAT NOT NULL
,   C6 DECIMAL ASSUMEUNIQUE NOT NULL
,   C7 VARCHAR(32) NOT NULL
,   C8 VARBINARY(32) NOT NULL
,   C9 TIMESTAMP DEFAULT NOW NOT NULL
,   C10 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,   ASSUMEUNIQUE
    (
        C1
    ,   C9
    )
);
PARTITION TABLE T21 ON COLUMN C3;

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
-- basic

CREATE TABLE T24
(
    C1 INTEGER
,   C2 INTEGER
);

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


-- EXPORT TABLE
-- basic

CREATE TABLE T25
(
    id INTEGER NOT NULL
);
EXPORT TABLE T25;


-- IMPORT CLASS
-- basic

IMPORT CLASS org.voltdb_testprocs.fullddlfeatures.NoMeaningClass;
CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fullddlfeatures.testImportProc;


-- PARTITION PROCEDURE
-- basic

CREATE TABLE T26
(
    age BIGINT NOT NULL
,   gender TINYINT
);

CREATE PROCEDURE p4
ALLOW
    admin
AS
    SELECT COUNT(*)
    FROM T26
    WHERE age = ?;

PARTITION TABLE T26 ON COLUMN age;

PARTITION PROCEDURE p4
ON
TABLE
    T26
COLUMN
    age
PARAMETER
    0
;

PARTITION PROCEDURE testCreateProcFromClassProc
ON
TABLE
    T26
COLUMN
    age
;


-- PARTITION TABLE
-- basic

CREATE TABLE T27
(
    C INTEGER NOT NULL
);

PARTITION TABLE T27 ON COLUMN C;

