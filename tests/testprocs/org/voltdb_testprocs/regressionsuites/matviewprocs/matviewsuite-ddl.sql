CREATE TABLE PEOPLE (PARTITION INTEGER NOT NULL, ID INTEGER ASSUMEUNIQUE, AGE INTEGER, SALARY FLOAT, CHILDREN INTEGER, PRIMARY KEY(ID, PARTITION));
PARTITION TABLE PEOPLE ON COLUMN PARTITION;
CREATE VIEW MATPEOPLE (AGE, PARTITION, NUM, SALARIES, KIDS) AS
    SELECT AGE, PARTITION, COUNT(*), SUM(SALARY), SUM(CHILDREN)
    FROM PEOPLE WHERE AGE > 5
    GROUP BY AGE, PARTITION;

-- optimize alternative ordering of group by columns
CREATE INDEX ENG4826_A ON MATPEOPLE ( PARTITION );
-- optimize ordering by a sum
CREATE INDEX ENG4826_B ON MATPEOPLE ( SALARIES );
-- optimize ordering by one group by column and a sum
CREATE INDEX ENG4826_C ON MATPEOPLE ( AGE, SALARIES );

CREATE VIEW MATPEOPLE2 (AGE, PARTITION, NUM, MIN_SALARY, MAX_CHILDREN)
    AS SELECT AGE, PARTITION, COUNT(*), MIN(SALARY), MAX(CHILDREN)
    FROM PEOPLE
    GROUP BY AGE, PARTITION;
CREATE VIEW MATPEOPLE3(AGE, PARTITION, CNT, MAX_CHILREN)
    AS SELECT AGE, PARTITION, COUNT(*), MAX(CHILDREN)
    FROM PEOPLE
    WHERE SALARY >= 1000
    GROUP BY AGE, PARTITION;

-- view for ENG-10945 to have count(*) anywhere in materialized views
CREATE VIEW MATPEOPLE4 (AGE, PARTITION, MIN_SALARY, NUM, MAX_CHILDREN)
    AS SELECT AGE, PARTITION, MIN(SALARY), COUNT(*), MAX(CHILDREN)
    FROM PEOPLE
    GROUP BY AGE, PARTITION;

-- view to have multiple count(*) anywhere in materialized views
CREATE VIEW MATPEOPLE5 (AGE, PARTITION, MIN_SALARY, NUM, MAX_CHILDREN, NUM1, MAX_SALARY)
    AS SELECT AGE, PARTITION, MIN(SALARY), COUNT(*), MAX(CHILDREN), COUNT(*), MAX(SALARY)
    FROM PEOPLE
    GROUP BY AGE, PARTITION;

-- views for ENG-7872 testing
CREATE VIEW MATPEOPLE_COUNT (NUM) AS
    SELECT COUNT(*)
    FROM PEOPLE;
CREATE VIEW MATPEOPLE_CONDITIONAL_COUNT (NUM) AS
    SELECT COUNT(*)
    FROM PEOPLE WHERE CHILDREN > 3;
CREATE VIEW MATPEOPLE_CONDITIONAL_COUNT_SUM (NUM, KIDS) AS
    SELECT COUNT(*), SUM(CHILDREN)
    FROM PEOPLE WHERE CHILDREN <= 3;
CREATE VIEW MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX (NUM, MIN_SALARY, MAX_CHILDREN) AS
    SELECT COUNT(*), MIN(SALARY), MAX(CHILDREN)
    FROM PEOPLE WHERE CHILDREN > 1;

-- Shadow table for caching the original table state for restoring post-truncation.
CREATE TABLE WAS_PEOPLE (PARTITION INTEGER NOT NULL, ID INTEGER ASSUMEUNIQUE, AGE INTEGER, SALARY FLOAT, CHILDREN INTEGER, PRIMARY KEY(ID, PARTITION));
PARTITION TABLE WAS_PEOPLE ON COLUMN PARTITION;

CREATE PROCEDURE INDEXED_FIRST_GROUP AS SELECT AGE, SALARIES FROM MATPEOPLE ORDER BY SALARIES DESC LIMIT 1;

CREATE PROCEDURE INDEXED_MAX_GROUP     AS SELECT MAX(SALARIES) FROM MATPEOPLE;
CREATE PROCEDURE INDEXED_MAX_IN_GROUPS AS SELECT MAX(SALARIES) FROM MATPEOPLE WHERE AGE = ?;
CREATE PROCEDURE INDEXED_GROUPS        AS
    SELECT AGE, SALARIES, PARTITION, NUM, KIDS
    FROM MATPEOPLE ORDER BY AGE, SALARIES;


CREATE TABLE THINGS (ID INTEGER, PRICE INTEGER, PRIMARY KEY(ID));
CREATE VIEW MATTHINGS (PRICE, NUM) AS SELECT PRICE, COUNT(*) FROM THINGS GROUP BY PRICE;

CREATE TABLE OVERFLOWTEST ( col_pk BIGINT NOT NULL ASSUMEUNIQUE, col_1 INTEGER NOT NULL, col_integer INTEGER, col_bigint BIGINT, PRIMARY KEY (COL_1, COL_PK));
PARTITION TABLE OVERFLOWTEST ON COLUMN COL_1;
CREATE VIEW V_OVERFLOWTEST (col_1, num_rows, sum_integer, sum_bigint) AS
    SELECT col_1, count(*), sum(col_integer), sum(col_bigint) FROM overflowtest GROUP BY col_1;

-- Freeze the structure/ordering of this table and view schema to maintain the ENG-798 preconditions
CREATE TABLE ENG798 (
    c1 VARCHAR(16) NOT NULL,
    c2 BIGINT DEFAULT 0 NOT NULL,
    c3 VARCHAR(36) DEFAULT '' NOT NULL,
    c4 VARCHAR(36),
    c5 TIMESTAMP,
    c6 TINYINT DEFAULT 0,
    c7 TIMESTAMP
);
PARTITION TABLE ENG798 ON COLUMN C1;
CREATE VIEW V_ENG798(c1, c6, c2, c3, c4, c5, total)
    AS SELECT c1, c6, c2, c3, c4, c5, COUNT(*)
    FROM ENG798
    GROUP BY c1, c6, c2, c3, c4, c5;

CREATE TABLE CONTEST (
    runner_class VARCHAR(16) NOT NULL,
    finish TIMESTAMP,
    team VARCHAR(16),
    runner VARCHAR(16)
);

PARTITION TABLE CONTEST ON COLUMN runner_class;
CREATE VIEW V_RUNNING_TEAM(runner_class, team, total)
    AS SELECT runner_class, team, COUNT(*)
    FROM contest
    GROUP BY runner_class, team;

CREATE VIEW V_TEAM_MEMBERSHIP(runner_class, team, total)
    AS SELECT runner_class, team, COUNT(*)
    FROM contest
    GROUP BY runner_class, team;

-- optimize ordering by count and group by column
CREATE INDEX participation ON V_TEAM_MEMBERSHIP ( total, team );
-- optimize alternative ordering of group by columns
CREATE INDEX teamorder     ON V_TEAM_MEMBERSHIP ( team, runner_class );

CREATE VIEW V_TEAM_TIMES(team, finish, total)
    AS SELECT team, finish, COUNT(*)
    FROM contest
    GROUP BY team, finish;

-- optimize ordering by count and expression of a group by column
CREATE INDEX runners_and_times ON V_TEAM_TIMES ( total, 0-SINCE_EPOCH(MILLISECOND, finish) );
-- optimize alternative ordering of group by columns
CREATE INDEX times_and_teams ON V_TEAM_TIMES ( finish, team );

CREATE TABLE DEPT_PEOPLE (ID INTEGER, DEPT INTEGER, AGE INTEGER, SALARY FLOAT, CHILDREN INTEGER, PRIMARY KEY (ID));
CREATE INDEX PREFIX_DEPT_PEOPLE_TREE ON DEPT_PEOPLE (DEPT);
CREATE INDEX FULL_DEPT_PEOPLE_TREE ON DEPT_PEOPLE (DEPT, AGE);
CREATE VIEW DEPT_AGE_MATVIEW (DEPT, AGE, NUM, MIN_SALARY, MAX_CHILDREN)
    AS SELECT DEPT, AGE, COUNT(*), MIN(SALARY), MAX(CHILDREN)
    FROM DEPT_PEOPLE
    GROUP BY DEPT, AGE;
CREATE VIEW DEPT_AGE_FILTER_MATVIEW(DEPT, AGE, CNT, MAX_CHILDREN)
    AS SELECT DEPT, AGE, COUNT(*), MAX(CHILDREN)
    FROM DEPT_PEOPLE
    WHERE SALARY >= 1000
    GROUP BY DEPT, AGE;

-- table and views for ENG-6511
CREATE TABLE ENG6511 (pid INTEGER NOT NULL, d1 INTEGER NOT NULL, d2 INTEGER NOT NULL, v1 INTEGER, v2 INTEGER NOT NULL);
PARTITION TABLE ENG6511 ON COLUMN pid;

CREATE INDEX IDX6511expR ON ENG6511 (d1, d2, abs(v1));
CREATE INDEX IDX6511d12 ON ENG6511 (d1, d2);
CREATE INDEX IDX6511 ON ENG6511 (d1, d2, v2);
CREATE INDEX IDX6511expL ON ENG6511 (d1+1, d2*2, v2);
CREATE INDEX IDX6511expLR ON ENG6511 (d1+1, d2*2, v2-1);
CREATE INDEX IDX6511CLT ON ENG6511 (d1, d2, v1) WHERE v1 < 4;
CREATE INDEX IDX6511CGT ON ENG6511 (d1, d2, v1) WHERE v1 > 4;
CREATE INDEX IDX6511NG1 ON ENG6511 (v1);
CREATE INDEX IDX6511NG2 ON ENG6511 (v2);

CREATE VIEW VENG6511 (d1, d2, cnt, vmin, vmax) AS
SELECT d1, d2, COUNT(*), MIN(v2) AS vmin, MAX(v2) AS vmax
FROM ENG6511 GROUP BY d1, d2;

CREATE VIEW VENG6511expL (d1, d2, cnt, vmin, vmax) AS
SELECT d1+1, d2*2, COUNT(*), MIN(v2) AS vmin, MAX(v2) AS vmax
FROM ENG6511 GROUP BY d1+1, d2*2;

CREATE VIEW VENG6511expR (d1, d2, cnt, vmin, vmax) AS
SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(abs(v1)) AS vmax
FROM ENG6511 GROUP BY d1, d2;

-- count * anywhere with complex aggregation queries
CREATE VIEW VENG6511expRC (d1, d2, vmin, cnt, vmax) AS
SELECT d1, d2, MIN(abs(v1)) AS vmin, COUNT(*), MAX(abs(v1)) AS vmax
FROM ENG6511 GROUP BY d1, d2;

-- multiple count * anywhere with complex aggregation queries
CREATE VIEW VENG6511expRCM (d1, d2, vmin, cnt, vmax, cnt1, vsum) AS
SELECT d1, d2, MIN(abs(v1)) AS vmin, COUNT(*), MAX(abs(v1)), COUNT(*), SUM(abs(v1)) AS vsum
FROM ENG6511 GROUP BY d1, d2;

CREATE VIEW VENG6511expLR (d1, d2, cnt, vmin, vmax) AS
SELECT d1+1, d2*2, COUNT(*), MIN(v2-1) AS vmin, MAX(v2-1) AS vmax
FROM ENG6511 GROUP BY d1+1, d2*2;

CREATE VIEW VENG6511C (d1, d2, cnt, vmin, vmax) AS
SELECT d1, d2, COUNT(*), MIN(v1) AS vmin, MAX(v1) AS vmax FROM ENG6511
WHERE v1 > 4 GROUP BY d1, d2;

CREATE VIEW VENG6511TwoIndexes (d1, d2, cnt, vmin, vmax) AS
SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(v2) AS vmax FROM ENG6511
WHERE v1 > 4 GROUP BY d1, d2;

CREATE VIEW VENG6511NoGroup (cnt, vmin, vmax) AS
SELECT COUNT(*), MIN(v1) AS vmin, MAX(v2) AS vmax FROM ENG6511;

-- Tests for view on join queries.

CREATE TABLE CUSTOMERS (
    CUSTOMER_ID INTEGER NOT NULL,
    NAME VARCHAR(50) NOT NULL,
    ADDRESS VARCHAR(50),
    PRIMARY KEY (CUSTOMER_ID)
);

CREATE PROCEDURE UPDATECUSTOMERS AS
    UPDATE CUSTOMERS SET NAME=?, ADDRESS=? WHERE CUSTOMER_ID=? AND NAME=? AND ADDRESS=?;

CREATE TABLE ORDERS (
    ORDER_ID INTEGER NOT NULL,
    CUSTOMER_ID INTEGER NOT NULL,
    ORDER_TIME TIMESTAMP NOT NULL,
    PRIMARY KEY (ORDER_ID)
);
PARTITION TABLE ORDERS ON COLUMN ORDER_ID;

CREATE PROCEDURE UPDATEORDERS
PARTITION ON TABLE ORDERS COLUMN ORDER_ID PARAMETER 2 AS
    UPDATE ORDERS SET CUSTOMER_ID=?, ORDER_TIME=? WHERE ORDER_ID=? AND CUSTOMER_ID=? AND ORDER_TIME=?;

CREATE TABLE ORDERITEMS (
    ORDER_ID INTEGER NOT NULL,
    PID INTEGER NOT NULL,
    QTY INTEGER NOT NULL
);
PARTITION TABLE ORDERITEMS ON COLUMN ORDER_ID;
CREATE INDEX ORDERITEMS_BY_ID ON ORDERITEMS(ORDER_ID);

CREATE PROCEDURE DELETEORDERITEMS
PARTITION ON TABLE ORDERITEMS COLUMN ORDER_ID AS
    DELETE FROM ORDERITEMS WHERE ORDER_ID=? AND PID=?;

CREATE PROCEDURE UPDATEORDERITEMS
PARTITION ON TABLE ORDERITEMS COLUMN ORDER_ID PARAMETER 2 AS
    UPDATE ORDERITEMS SET PID=?, QTY=? WHERE ORDER_ID=? AND PID=? AND QTY=?;

CREATE TABLE PRODUCTS (
    PID INTEGER NOT NULL,
    PNAME VARCHAR(50) NOT NULL,
    PRICE FLOAT NOT NULL,
    PRIMARY KEY (PID)
);

CREATE PROCEDURE UPDATEPRODUCTS AS
    UPDATE PRODUCTS SET PNAME=?, PRICE=? WHERE PID=? AND PNAME=? AND PRICE=?;

-- replicated join partitioned, no partition column for view table.
CREATE VIEW ORDER_COUNT_NOPCOL (NAME, CNT) AS
    SELECT CUSTOMERS.NAME, COUNT(*)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
    GROUP BY CUSTOMERS.NAME;

CREATE PROCEDURE PROC_ORDER_COUNT_NOPCOL AS
    SELECT CUSTOMERS.NAME, COUNT(*)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
    GROUP BY CUSTOMERS.NAME
    ORDER BY 1;

-- replicated join partitioned, no group by column.
CREATE VIEW ORDER_COUNT_GLOBAL (CNT, CNTID, MINID, MAXID, SUMID) AS
    SELECT COUNT(*), COUNT(ORDERS.ORDER_ID), MIN(ORDERS.ORDER_ID),
           MAX(ORDERS.ORDER_ID), SUM(ORDERS.ORDER_ID) FROM
    CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID;

CREATE PROCEDURE PROC_ORDER_COUNT_GLOBAL AS
    SELECT COUNT(*), COUNT(ORDERS.ORDER_ID), MIN(ORDERS.ORDER_ID),
           MAX(ORDERS.ORDER_ID), SUM(ORDERS.ORDER_ID) FROM
    CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
    ORDER BY 1;

-- replicated join partitioned, no group by column with count(*) anywhere
CREATE VIEW ORDER_COUNT_GLOBAL_ANYWHERE (CNTID, MINID, MAXID, SUMID, CNT) AS
    SELECT COUNT(ORDERS.ORDER_ID), MIN(ORDERS.ORDER_ID),
           MAX(ORDERS.ORDER_ID), SUM(ORDERS.ORDER_ID), COUNT(*) FROM
    CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID;

CREATE PROCEDURE PROC_ORDER_COUNT_GLOBAL_ANYWHERE AS
    SELECT COUNT(ORDERS.ORDER_ID), MIN(ORDERS.ORDER_ID),
           MAX(ORDERS.ORDER_ID), SUM(ORDERS.ORDER_ID), COUNT(*) FROM
    CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
    ORDER BY 1;

-- four source tables, no partition column for view table.
CREATE VIEW ORDER_DETAIL_NOPCOL (NAME, CNT, SUMAMT, MINUNIT, MAXUNIT, ITEMCOUNT) AS
    SELECT
        CUSTOMERS.NAME,
        COUNT(*),
        SUM(PRODUCTS.PRICE * ORDERITEMS.QTY),
        MIN(PRODUCTS.PRICE),
        MAX(PRODUCTS.PRICE),
        COUNT(ORDERITEMS.PID)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
                   JOIN ORDERITEMS ON ORDERS.ORDER_ID = ORDERITEMS.ORDER_ID
                   JOIN PRODUCTS ON ORDERITEMS.PID = PRODUCTS.PID
    GROUP BY CUSTOMERS.NAME;

CREATE PROCEDURE PROC_ORDER_DETAIL_NOPCOL AS
    SELECT
        CUSTOMERS.NAME,
        COUNT(*),
        SUM(PRODUCTS.PRICE * ORDERITEMS.QTY),
        MIN(PRODUCTS.PRICE),
        MAX(PRODUCTS.PRICE),
        COUNT(ORDERITEMS.PID)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
                   JOIN ORDERITEMS ON ORDERS.ORDER_ID = ORDERITEMS.ORDER_ID
                   JOIN PRODUCTS ON ORDERITEMS.PID = PRODUCTS.PID
    GROUP BY CUSTOMERS.NAME
    ORDER BY 1;

-- four source tables, has partition column for view table.
CREATE VIEW ORDER_DETAIL_WITHPCOL (NAME, ORDER_ID, CNT, MINUNIT, MAXUNIT) AS
    SELECT
        CUSTOMERS.NAME,
        ORDERS.ORDER_ID,
        COUNT(*),
        MIN(PRODUCTS.PRICE),
        MAX(PRODUCTS.PRICE)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
                   JOIN ORDERITEMS ON ORDERS.ORDER_ID = ORDERITEMS.ORDER_ID
                   JOIN PRODUCTS ON ORDERITEMS.PID = PRODUCTS.PID
    GROUP BY CUSTOMERS.NAME, ORDERS.ORDER_ID;

CREATE PROCEDURE PROC_ORDER_DETAIL_WITHPCOL AS
    SELECT
        CUSTOMERS.NAME,
        ORDERS.ORDER_ID,
        COUNT(*),
        MIN(PRODUCTS.PRICE),
        MAX(PRODUCTS.PRICE)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
                   JOIN ORDERITEMS ON ORDERS.ORDER_ID = ORDERITEMS.ORDER_ID
                   JOIN PRODUCTS ON ORDERITEMS.PID = PRODUCTS.PID
    GROUP BY CUSTOMERS.NAME, ORDERS.ORDER_ID
    ORDER BY 1,2;

-- replicated join partitioned, has where predicate.
CREATE VIEW ORDER2016 (NAME, CNT) AS
    SELECT
        CUSTOMERS.NAME,
        COUNT(*)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
    WHERE ORDERS.ORDER_TIME >= '2016-01-01 00:00:00'
    GROUP BY CUSTOMERS.NAME;

CREATE PROCEDURE PROC_ORDER2016 AS
    SELECT
        CUSTOMERS.NAME,
        COUNT(*)
    FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID
    WHERE ORDERS.ORDER_TIME >= '2016-01-01 00:00:00'
    GROUP BY CUSTOMERS.NAME
    ORDER BY 1;

-- Shadow tables for caching table state to restore post-truncation.
CREATE TABLE WAS_CUSTOMERS (
    CUSTOMER_ID INTEGER NOT NULL,
    NAME VARCHAR(50) NOT NULL,
    ADDRESS VARCHAR(50),
    PRIMARY KEY (CUSTOMER_ID)
);

CREATE TABLE WAS_ORDERS (
    ORDER_ID INTEGER NOT NULL,
    CUSTOMER_ID INTEGER NOT NULL,
    ORDER_TIME TIMESTAMP NOT NULL,
    PRIMARY KEY (ORDER_ID)
);
PARTITION TABLE WAS_ORDERS ON COLUMN ORDER_ID;

CREATE TABLE WAS_ORDERITEMS (
    ORDER_ID INTEGER NOT NULL,
    PID INTEGER NOT NULL,
    QTY INTEGER NOT NULL
);
PARTITION TABLE WAS_ORDERITEMS ON COLUMN ORDER_ID;
CREATE INDEX WAS_ORDERITEMS_BY_ID ON WAS_ORDERITEMS(ORDER_ID);

CREATE TABLE WAS_PRODUCTS (
    PID INTEGER NOT NULL,
    PNAME VARCHAR(50) NOT NULL,
    PRICE FLOAT NOT NULL,
    PRIMARY KEY (PID)
);

-- a single table view case
CREATE VIEW QTYPERPRODUCT (PID, CNT, SUMQTY) AS
    SELECT PID, COUNT(*), SUM(QTY)
    FROM ORDERITEMS
    GROUP BY PID;

CREATE PROCEDURE PROC_QTYPERPRODUCT AS
    SELECT PID, COUNT(*), SUM(QTY)
    FROM ORDERITEMS
    GROUP BY PID
    ORDER BY 1;

CREATE TABLE P1_ENG_11024 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1_ENG_11024 ON COLUMN ID;

CREATE TABLE P2_ENG_11024 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P2_ENG_11024 ON COLUMN ID;

CREATE TABLE R1_ENG_11024 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE R2_ENG_11024 (
  ID INTEGER NOT NULL,
  VCHAR VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE VIEW V3_ENG_11024_JOIN (ID, RATIO) AS
  SELECT COUNT(*), MIN(T2.RATIO)
  FROM P1_ENG_11024 T1 JOIN P2_ENG_11024 T2 USING(ID);

CREATE VIEW V3_ENG_11024_1tbl (ID, RATIO) AS
  SELECT COUNT(*), MIN(RATIO)
  FROM P1_ENG_11024;

-- Repro for ENG-11042 uses same source tables as 11024
-- (both found by sqlcmd)
CREATE VIEW V16_ENG_11042 (ID, COUNT_STAR, NUM) AS
  SELECT T2.NUM, COUNT(*), MAX(T1.NUM)
  FROM R1_ENG_11024 T1 JOIN R2_ENG_11024 T2 ON T1.ID = T2.ID
  GROUP BY T2.NUM;

-- Repro for ENG-11043, also uses same source tables as 11024
CREATE VIEW V27 (NUM, ID, RATIO, VCHAR) AS
  SELECT T1.NUM, COUNT(*), MAX(T2.RATIO), MIN(T3.VCHAR)
  FROM P1_ENG_11024 T1 JOIN P2_ENG_11024 T2 ON T1.ID = T2.ID JOIN R1_ENG_11024 T3 ON T2.ID = T3.ID
  GROUP BY T1.NUM;

-- Repro for ENG-11047
CREATE VIEW V21 (NUM, VCHAR, COUNT_STAR, RATIO, ID) AS
  SELECT T2.NUM, T2.VCHAR, COUNT(*), MIN(T1.RATIO), COUNT(T2.ID)
  FROM R2_ENG_11024 T1 JOIN P2_ENG_11024 T2 ON T1.ID = T2.NUM
  GROUP BY T2.NUM, T2.VCHAR;

-- Repro for ENG-11074
-- These tables are used in the "string" suite of sqlcoverage matview join tests.
CREATE TABLE P1_ENG_11074 (
       ID INTEGER DEFAULT '0' NOT NULL,
       VCHAR VARCHAR(64 BYTES),
       VCHAR_INLINE_MAX VARCHAR(15),
       VCHAR_INLINE VARCHAR(42 BYTES),
       RATIO FLOAT NOT NULL,
       PRIMARY KEY (ID)
);
PARTITION TABLE P1_ENG_11074 ON COLUMN ID;

CREATE TABLE P2_ENG_11074 (
       ID INTEGER DEFAULT '0' NOT NULL,
       VCHAR VARCHAR(64 BYTES),
       VCHAR_INLINE_MAX VARCHAR(15),
       VCHAR_INLINE VARCHAR(42 BYTES),
       RATIO FLOAT NOT NULL,
       PRIMARY KEY (ID)
);
PARTITION TABLE P2_ENG_11074 ON COLUMN ID;

CREATE TABLE R1_ENG_11074 (
       ID INTEGER DEFAULT '0' NOT NULL,
       VCHAR VARCHAR(5000),
       VCHAR_INLINE_MAX VARCHAR(63 BYTES),
       VCHAR_INLINE VARCHAR(14),
       RATIO FLOAT NOT NULL,
       PRIMARY KEY (ID)
);

CREATE VIEW VJOIN_ENG_11074 (ID, VCHAR_INLINE) AS
SELECT COUNT(*), MIN(T2.VCHAR_INLINE) FROM P1_ENG_11074 T1 JOIN P2_ENG_11074 T2 USING(ID);

CREATE VIEW V1_ENG_11074 (ID, VCHAR_INLINE) AS
SELECT COUNT(*), MIN(T1.VCHAR_INLINE) FROM P1_ENG_11074 T1;

-- Repro for ENG-11080
CREATE VIEW V_ENG_11080 (VCHAR, COUNT_STAR, ID) AS
  SELECT   T1.VCHAR, COUNT(*),   MAX(T1.ID)
  FROM R1_ENG_11024 T1
  GROUP BY T1.VCHAR;

CREATE VIEW VJOIN_ENG_11080 (VCHAR, COUNT_STAR, ID) AS
  SELECT   T1.VCHAR, COUNT(*),   MAX(T1.ID)
  FROM R1_ENG_11024 T1 JOIN R2_ENG_11024 T2 ON T1.NUM = T2.NUM
  GROUP BY T1.VCHAR;

-- Repro for ENG-11100
CREATE VIEW V_ENG_11100 (VCHAR_INLINE_MAX, ID, VCHAR_INLINE_MAX2) AS
  SELECT   T1.VCHAR_INLINE_MAX,   COUNT(*),   MIN(T2.VCHAR_INLINE_MAX)
  FROM P1_ENG_11074 T1 JOIN R1_ENG_11074 T2 ON T1.ID  <  T2.ID
  GROUP BY T1.VCHAR_INLINE_MAX;

-- Repro for ENG-11119
CREATE TABLE T1_ENG_11119 (
        f1 VARCHAR(3) NOT NULL,
        f2 VARCHAR,
);
create index t1_idx on t1_eng_11119 (F1, f2);
PARTITION TABLE T1_eng_11119 on column f1;

CREATE TABLE T3_eng_11119
(
        f4 VARCHAR(3) NOT NULL,
        f5 VARCHAR(6),
        f6 TIMESTAMP
);
partition table T3_eng_11119 on column f4;
CREATE INDEX t3_idx on t3_eng_11119(f4, f6);

-- Repro for ENG-11203
CREATE TABLE ENG_11203_A (a INT PRIMARY KEY, b INT, c INT);
CREATE TABLE ENG_11203_B (a INT PRIMARY KEY, b INT, c INT);
CREATE VIEW V_ENG_11203_SINGLE (b, cnt, a) AS
  SELECT b, COUNT(*), MAX(a) FROM ENG_11203_A GROUP BY b;
CREATE VIEW V_ENG_11203_JOIN (b, cnt, a) AS
  SELECT X.b, COUNT(*), MAX(Y.a)
  FROM ENG_11203_A X JOIN ENG_11203_B Y
  ON X.b = Y.b
  GROUP BY X.b;

-- Repro for ENG-11314
CREATE TABLE T1_ENG_11314 (
    G0 INT DEFAULT '0' NOT NULL,
    G1 INT DEFAULT '0' NOT NULL,
    C0 INT DEFAULT '0' NOT NULL,
    C1 INT DEFAULT '0' NOT NULL,
    C2 INT DEFAULT '0' NOT NULL,
    C3 INT DEFAULT '0' NOT NULL,
    C4 INT DEFAULT '0' NOT NULL,
    C5 INT DEFAULT '0' NOT NULL,
    C6 INT DEFAULT '0' NOT NULL,
    C7 INT DEFAULT '0' NOT NULL,
    C8 INT DEFAULT '0' NOT NULL,
    C9 INT DEFAULT '0' NOT NULL,
    C10 VARCHAR DEFAULT 'abc' NOT NULL,
    C11 VARCHAR DEFAULT 'def' NOT NULL,
    PRIMARY KEY (G0, G1)
);
PARTITION TABLE T1_ENG_11314 ON COLUMN G0;

CREATE TABLE T2_ENG_11314 (
    G0 INT NOT NULL,
    G1 INT DEFAULT '0' NOT NULL
);
PARTITION TABLE T2_ENG_11314 ON COLUMN G0;

CREATE VIEW V1_ENG_11314
(G0, CNT, C0, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11)
AS
SELECT G0, COUNT(*),
       MAX(C0), MIN(C1), MAX(C2), MIN(C3), MAX(C4), MIN(C5),
       MAX(C6), MIN(C7), MAX(C8), MIN(C9), MAX(C10), MIN(C11)
FROM T1_ENG_11314 GROUP BY G0;

CREATE VIEW V2_ENG_11314
(G0, CNT, C0, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11)
AS
SELECT T1.G0, COUNT(*),
       MAX(T1.C0), MIN(T1.C1), MAX(T1.C2), MIN(T1.C3), MAX(T1.C4), MIN(T1.C5),
       MAX(T1.C6), MIN(T1.C7), MAX(T1.C8), MIN(T1.C9), MAX(T1.C10), MIN(T1.C11)
FROM T1_ENG_11314 T1 JOIN T2_ENG_11314 T2 ON T1.G0=T2.G0 GROUP BY T1.G0;

CREATE TABLE ENG11495 (
    ID      INTEGER,
    PRICE   INTEGER,
    TS      TIMESTAMP
);

CREATE TABLE TAXI_LOCATIONS (
    TAXI_ID         INT               NOT NULL,
    RECORD_TIME     TIMESTAMP         NOT NULL,
    TAXI_LOCATION   GEOGRAPHY_POINT   NOT NULL
);
PARTITION TABLE TAXI_LOCATIONS ON COLUMN TAXI_ID;

CREATE TABLE REGIONS (
    REGION_ID         INT              NOT NULL  PRIMARY KEY,
    REGION_BOUNDARY   GEOGRAPHY(1024)  NOT NULL
);

CREATE VIEW REGIONAL_TAXI_COUNT (REGION_ID, TAXI_COUNT) AS
SELECT REGIONS.REGION_ID,
       COUNT(*) FROM REGIONS JOIN TAXI_LOCATIONS ON
    CONTAINS(REGIONS.REGION_BOUNDARY, TAXI_LOCATIONS.TAXI_LOCATION)
GROUP BY REGIONS.REGION_ID;

CREATE TABLE ENG11935 (
   VAR1 varchar(64) NOT NULL,
   VAR2 varchar(5),
   VAR3 bigint,
   VAR4 varchar(256),
   VAR5 varchar(256),
   VAR6 varchar(256),
   VAR7 varchar(256),
   VAR8 bigint,
   VAR9 bigint,
   VAR10 bigint,
   VAR11 bigint,
   VAR12 bigint,
   VAR13 bigint,
   PRIMKEY varchar(256) NOT NULL,
   ASSUMEUNIQUE (PRIMKEY)
);
PARTITION TABLE ENG11935 ON COLUMN VAR1;
CREATE INDEX ENG11935_IDX ON ENG11935
  (VAR1, VAR2, VAR4, VAR5, VAR6, TRUNCATE(MINUTE, TO_TIMESTAMP(SECOND, (60 + (60 * FLOOR((VAR3 / 60000)))))),
    (CAST ((60 + (60 * FLOOR((VAR3 / 60000)))) AS integer)));

CREATE VIEW V_ENG11935 (
   VAR1,
   VAR2,
   VAR4,
   VAR5,
   VAR6,
   STATS_LOCAL_TIMESTAMP,
   VAR3,
   PER_MIN_COUNT,
   VAR7,
   VAR8,
   VAR9,
   NUMBERVAR8S,
   NUMBERVAR9S,
   VAR12,
   VAR13
) AS 
  SELECT VAR1, VAR2, VAR4, VAR5, VAR6,
          TRUNCATE(MINUTE, TO_TIMESTAMP(SECOND, 60 + 60 * FLOOR(VAR3 / 60000))),
          CAST(60 + 60 * FLOOR(VAR3 / 60000) AS INT),
          COUNT(*), MAX(VAR7), SUM(CASE WHEN VAR8 < 0 THEN 0 ELSE 20 * VAR8 END),
          SUM(CASE WHEN VAR9 < 0 THEN 0 ELSE 20 * VAR9 END),
          SUM(20 * VAR10), SUM(20 * VAR11), SUM(20 * (VAR12 * VAR10)),
          SUM(20 * (VAR13 * VAR11))
  FROM ENG11935
  GROUP BY VAR1, VAR2, VAR4, VAR5, VAR6,
           TRUNCATE(MINUTE, TO_TIMESTAMP(SECOND, 60 + 60 * FLOOR(VAR3 / 60000))),
           CAST(60 + 60 * FLOOR(VAR3 / 60000) AS INT);
