CREATE TABLE VarcharBYTES(
  ID INTEGER DEFAULT 0 NOT NULL,
  var2 VARCHAR(2 BYTES),
  var80 VARCHAR(80 BYTES),
  PRIMARY KEY (ID)
);

CREATE TABLE VarcharTB (
  ID INTEGER DEFAULT 0 NOT NULL,
  var2 VARCHAR(2),
  var80 VARCHAR(80),
  PRIMARY KEY (ID)
);

CREATE TABLE VarLength (
  ID INTEGER DEFAULT 0 NOT NULL,
  var1 VARCHAR(10),
  var2 VARCHAR(80),
  bin1 VARBINARY(10),
  bin2 VARBINARY(80),
  PRIMARY KEY (ID)
);

CREATE TABLE P1 (
  ID INTEGER DEFAULT 0 NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID INTEGER DEFAULT 0 NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);
CREATE PROCEDURE R1_PROC1 AS SELECT NUM + 0.1 FROM R1;
CREATE PROCEDURE R1_PROC2 AS SELECT NUM + 1.0E-1 FROM R1;

CREATE TABLE P2 (
  ID INTEGER DEFAULT 0 NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  CONSTRAINT P2_PK_TREE PRIMARY KEY (ID)
);
PARTITION TABLE P2 ON COLUMN ID;

CREATE TABLE R2 (
  ID INTEGER DEFAULT 0 NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  CONSTRAINT R2_PK_TREE PRIMARY KEY (ID)
);

CREATE TABLE R3 (
  ID INTEGER DEFAULT 0 NOT NULL,
  NUM INTEGER
);
create index idx1 on R3 (id);
create unique index idx2 on R3 (id,num);

-- not suppose to define index on this table
CREATE TABLE R4 (
  ID INTEGER DEFAULT 0 NOT NULL,
  NUM INTEGER
);

CREATE TABLE P1_DECIMAL (
  ID INTEGER DEFAULT 0 NOT NULL,
  CASH DECIMAL NOT NULL,
  CREDIT DECIMAL NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE R1_DECIMAL (
  ID INTEGER DEFAULT 0 NOT NULL,
  CASH DECIMAL NOT NULL,
  CREDIT DECIMAL NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE COUNT_NULL (
  TRICKY TINYINT,
  ID INTEGER DEFAULT 0 NOT NULL,
  NUM INTEGER DEFAULT 0 NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE OBJECT_DETAIL (
  OBJECT_DETAIL_ID INTEGER NOT NULL,
  NAME VARCHAR(256) NOT NULL,
  DESCRIPTION VARCHAR(1024) NOT NULL,
  PRIMARY KEY (OBJECT_DETAIL_ID)
);
PARTITION TABLE OBJECT_DETAIL ON COLUMN OBJECT_DETAIL_ID;

CREATE TABLE ASSET (
  ASSET_ID INTEGER NOT NULL,
  OBJECT_DETAIL_ID INTEGER NOT NULL,
  PRIMARY KEY (ASSET_ID)
);
PARTITION TABLE ASSET ON COLUMN ASSET_ID;

CREATE TABLE STRINGPART (
  NAME VARCHAR(9) NOT NULL,
  VAL1 INTEGER NOT NULL,
  VAL2 INTEGER ASSUMEUNIQUE,
  PRIMARY KEY(VAL1, NAME)
);
PARTITION TABLE STRINGPART ON COLUMN NAME;

CREATE TABLE test_ENG1232 (
    id bigint NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE ENG1850 (
  cid INTEGER not null,
  aid INTEGER,
  pid INTEGER,
  attr INTEGER,
  constraint pk_cid primary key (cid)
);
PARTITION TABLE ENG1850 ON COLUMN cid;

create index eng1850_tree_pid_aid on ENG1850 (
  pid, aid
);

CREATE TABLE DEFAULT_NULL (
  ID INTEGER NOT NULL,
  num1 INTEGER DEFAULT NULL,
  num2 INTEGER ,
  ratio FLOAT DEFAULT NULL,
  num3 INTEGER DEFAULT NULL,
  desc VARCHAR(300) DEFAULT NULL,
  PRIMARY KEY (ID)
);

create index idx_num3 on DEFAULT_NULL (num3);


CREATE TABLE NO_JSON (
  ID INTEGER NOT NULL,
  var1 VARCHAR(300),
  var2 VARCHAR(300),
  var3 VARCHAR(300),
  PRIMARY KEY (ID)
);
CREATE INDEX test_field ON NO_JSON (var2, field(var3,'color'));

CREATE TABLE P3 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P3 ON COLUMN ID;

-- to test inline varchar.
CREATE TABLE PWEE (
  ID INTEGER DEFAULT '0' NOT NULL,
  WEE VARCHAR(3),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);
PARTITION TABLE PWEE ON COLUMN ID;

CREATE TABLE PWEE_WITH_INDEX (
  ID INTEGER DEFAULT 0 NOT NULL,
  WEE VARCHAR(3),
  NUM INTEGER,
  PRIMARY KEY (ID)
);
CREATE INDEX my_num_idx ON PWEE_WITH_INDEX(NUM);

--
CREATE VIEW V_P3 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
SELECT wage, dept, count(*), sum(age), sum(rent) FROM P3
GROUP BY wage, dept;

-- ENG6870
CREATE TABLE ENG6870
(
        C0 BIGINT NOT NULL,
	C1 INTEGER,
	C2 INTEGER,
	C3 INTEGER,
	C4 INTEGER,
	C5 BIGINT,
	C6 INTEGER,
	C7 SMALLINT,
	C8 TINYINT,
	C9 TINYINT,
	C10 TINYINT,
	C11 TINYINT,
	C12 INTEGER,
	C13 INTEGER,
	C14 TINYINT
);
PARTITION TABLE ENG6870 ON COLUMN C0;

CREATE INDEX SIX_TreeIdx ON ENG6870 (C5);
CREATE INDEX FOURTEEN_TreeIdx ON ENG6870 (C14);

-- ENG-6926
CREATE TABLE ENG6926_IPUSER (
   IP VARCHAR(128) NOT NULL UNIQUE,
   COUNTRYCODE VARCHAR(4),
   COUNTRY VARCHAR(128),
   PROVINCECODE VARCHAR(4),
   PROVINCE VARCHAR(128),
   CITY VARCHAR(128),
   CITYCODE VARCHAR(8),
   LATITUDE FLOAT,
   LONGITUDE FLOAT,
   UPDATED TIMESTAMP DEFAULT NOW,
   PRIMARY KEY (IP)
);

CREATE TABLE ENG6926_HITS (
   IP VARCHAR(128) NOT NULL,
   HITCOUNT BIGINT,
   WEEK BIGINT NOT NULL,
   PRIMARY KEY (WEEK, IP)
);
PARTITION TABLE ENG6926_HITS ON COLUMN WEEK;

-- ************************* --
-- Begin tables for ENG-7041 --
CREATE TABLE transaction(
  txn_id BIGINT NOT NULL,
  acc_no BIGINT  NOT NULL,
  txn_amt FLOAT NOT NULL,
  txn_state VARCHAR(5) NOT NULL,
  txn_city VARCHAR(50) NOT NULL,
  txn_ts TIMESTAMP  NOT NULL,
  vendor_id INTEGER,
  PRIMARY KEY (acc_no, txn_ts, txn_id)
);
PARTITION TABLE transaction ON COLUMN acc_no;

CREATE STREAM offers_given_exp PARTITION ON COLUMN acc_no (
  acc_no BIGINT NOT NULL,
  vendor_id INTEGER,
  offer_ts TIMESTAMP NOT NULL,
  offer_text VARCHAR(200)
);

CREATE VIEW acct_vendor_totals AS
SELECT
  acc_no,
  vendor_id,
  COUNT(*) as total_visits,
  SUM(txn_amt) as total_spend
FROM transaction
GROUP BY acc_no, vendor_id;
-- End tables for ENG-7041   --
-- ************************* --

-- ************************* --
-- Table for ENG-7349        --
create table sm_idx_tbl(
       ti1 tinyint,
       ti2 tinyint,
       bi bigint
);
create index sm_idx on sm_idx_tbl(ti1, ti2);
-- End table for ENG-7349    --
-- ************************* --

-- ****************************** --
-- Stored procedures for ENG-7354 --
create procedure one_list_param as
       select id from P1 where ID in ?
       order by id;

create procedure one_string_list_param as
       select id from P1 where desc in ?
       order by id;

create procedure one_scalar_param as
       select id from P1 where ID in (?)
       order by id;

create procedure one_string_scalar_param as
       select id from P1 where desc in (?)
       order by id;
-- End stored procedures for ENG-7354 --
-- ********************************** --

-- ********************************** --
-- Stored procedure for ENG-7724      --
CREATE TABLE product_changes (
  location              VARCHAR(12) NOT NULL,
  product_id               VARCHAR(18) NOT NULL,
  start_date               TIMESTAMP,
  safety_time_promo        SMALLINT,
  safety_time_base         SMALLINT,
  POQ                      SMALLINT,
  case_size                INTEGER,
  multiple                 INTEGER,
  lead_time                SMALLINT,
  supplier                 VARCHAR(12),
  facings                  INTEGER,
  minimum_deep             FLOAT,
  maximum_deep             INTEGER,
  backroom_sfty_stck       INTEGER,
  cost                     FLOAT,
  selling_price            FLOAT,
  model                    VARCHAR(12),
  assortment_adj           FLOAT,
  safety_stock_days        SMALLINT
);
PARTITION TABLE product_changes ON COLUMN location;
CREATE INDEX product_changes_sku ON product_changes (location, product_id);


CREATE PROCEDURE voltdbSelectProductChanges AS
SELECT
  location,
  product_id,
  start_date,
  facings,
  minimum_deep,
  maximum_deep,
  backroom_sfty_stck,
  supplier,
  safety_time_base,
  selling_price,
  cost,
  supplier,
  safety_stock_days
FROM product_changes
WHERE location = ?
AND product_id = ?
ORDER by location, product_id, start_date;
PARTITION PROCEDURE voltdbSelectProductChanges ON TABLE product_changes COLUMN location PARAMETER 0;
-- ********************************** --

-- ENG-9032, ENG-9389
CREATE TABLE t1(
 a INTEGER,
 b integer);
create index t1_idx1 on t1 (a);
create index t1_idx2 on t1 (b);

CREATE TABLE t2(
 b INTEGER,
 d integer);
create unique index t2_idx1 on t2 (b);

CREATE TABLE t3(
 a INTEGER,
 x INTEGER,
 d integer);
create unique index t3_idx1 on t3 (a);
create unique index t3_idx2 on t3 (d);

CREATE TABLE t3_no_index (
 a INTEGER,
 x INTEGER,
 d integer);

-- ENG-9533
CREATE TABLE test1_eng_9533 (
  id bigint not null,
  primary key (id)
);
PARTITION TABLE test1_eng_9533 ON COLUMN ID;

CREATE TABLE test2_eng_9533 (
   T_ID bigint NOT NULL,
   T_CHAR_ID1 varchar(128),
   T_CHAR_ID2 varchar(128),
   T_INT integer,
   PRIMARY KEY (T_ID, T_CHAR_ID1, T_CHAR_ID2)
);
PARTITION TABLE test2_eng_9533 ON COLUMN T_ID;

CREATE TABLE ENG_12105 (
  ID      INTEGER NOT NULL,
  TINY    TINYINT,
  SMALL   SMALLINT,
  INT     INTEGER,
  BIG     BIGINT,
  NUM     FLOAT,
  DEC     DECIMAL,
  VCHAR   VARCHAR(500),
  VCHAR_INLINE_MAX VARCHAR(63 BYTES),
  VCHAR_INLINE     VARCHAR(14),
  TIME    TIMESTAMP,
  VARBIN  VARBINARY(100),
  PRIMARY KEY (ID)
);

CREATE TABLE ENG_539 (
  ID      INTEGER NOT NULL,
  VARBIN  VARBINARY(3),
  BIG     BIGINT,
  PRIMARY KEY (ID)
);

create table swapper_table_foo (
       i integer,
       j varchar(32),
       primary key (i)
);

create table swapper_table_bar (
       i integer,
       j varchar(32),
       primary key (i)
);

-- Tables for reproducer for ENG-13852
CREATE TABLE ENG_13852_P5 (
       ID      INTEGER  NOT NULL,
       TINY    TINYINT  NOT NULL,
       SMALL   SMALLINT NOT NULL,
       INT     INTEGER  NOT NULL,
       BIG     BIGINT   NOT NULL,
       NUM     FLOAT    NOT NULL,
       DEC     DECIMAL  NOT NULL,
       VCHAR_INLINE      VARCHAR(42 BYTES)   NOT NULL,
       VCHAR_INLINE_MAX  VARCHAR(15)         NOT NULL,
       VCHAR_OUTLINE_MIN VARCHAR(16)         NOT NULL,
       VCHAR             VARCHAR             NOT NULL,
       VCHAR_JSON        VARCHAR(4000 BYTES) NOT NULL,
       TIME    TIMESTAMP       NOT NULL,
       VARBIN  VARBINARY(100)  NOT NULL,
       POINT   GEOGRAPHY_POINT NOT NULL,
       POLYGON GEOGRAPHY,
       IPV4    VARCHAR(15),
       IPV6    VARCHAR(60),
       VBIPV4  VARBINARY(4),
       VBIPV6  VARBINARY(16)       NOT NULL,
       PRIMARY KEY (VCHAR, ID)
);

PARTITION TABLE ENG_13852_P5 ON COLUMN ID;
CREATE       UNIQUE INDEX IDX_P5_IV  ON ENG_13852_P5 (ID, VCHAR)                WHERE ID >= 0;
CREATE ASSUMEUNIQUE INDEX IDX_P5_VSI ON ENG_13852_P5 (VCHAR_INLINE, SMALL, INT) WHERE VCHAR_INLINE_MAX < 'a';
CREATE              INDEX IDX_P5_IVI ON ENG_13852_P5 (INT, VCHAR_INLINE_MAX)    WHERE INT >= 0 AND VCHAR_INLINE IS NOT NULL;

CREATE TABLE ENG_13852_R11 (
       ID      INTEGER NOT NULL PRIMARY KEY,
       TINY    TINYINT,
       SMALL   SMALLINT,
       INT     INTEGER,
       BIG     BIGINT,
       NUM     FLOAT,
       DEC     DECIMAL,
       VCHAR_INLINE      VARCHAR(42 BYTES),
       VCHAR_INLINE_MAX  VARCHAR(15),
       VCHAR_OUTLINE_MIN VARCHAR(16),
       VCHAR             VARCHAR,
       VCHAR_JSON        VARCHAR(4000 BYTES),
       TIME    TIMESTAMP,
       VARBIN  VARBINARY(100),
       POINT   GEOGRAPHY_POINT,
       POLYGON GEOGRAPHY,
       IPV4    VARCHAR(15),
       IPV6    VARCHAR(60),
       VBIPV4  VARBINARY(4),
       VBIPV6  VARBINARY(16)
);

CREATE VIEW ENG_13852_VR5 (SMALL, VCHAR, TINY,
    ID, INT, BIG, NUM, DEC,
    VCHAR_INLINE, VCHAR_INLINE_MAX, VCHAR_OUTLINE_MIN, VCHAR_JSON, TIME
    -- Will not work with SqlCoverage/PostgreSQL:
    , VARBIN, POINT, POLYGON
    , IPV4, IPV6, VBIPV4, VBIPV6
    ) AS
  SELECT SMALL, VCHAR, TINY,
    COUNT(*), COUNT(TIME), COUNT(POINT), SUM(NUM), SUM(DEC),
    MIN(VCHAR_INLINE), MAX(VCHAR_INLINE_MAX), MIN(VCHAR_OUTLINE_MIN), MAX(VCHAR_JSON), MIN(TIME)
    -- Will not work with SqlCoverage/PostgreSQL:
    , MAX(VARBIN), MIN(POINT), MAX(POLYGON)
    , MIN(IPV4), MAX(IPV6), MIN(VBIPV4), MAX(VBIPV6)
  FROM ENG_13852_R11 WHERE TINY < 64
GROUP BY SMALL, VCHAR, TINY;
CREATE INDEX IDX_ENG_13852_VR5_SVT ON ENG_13852_VR5 (SMALL, VCHAR, TINY)      WHERE SMALL >= 0;
CREATE INDEX IDX_ENG_13852_VR5_VID ON ENG_13852_VR5 (VCHAR_INLINE, INT, DEC)  WHERE VCHAR_INLINE < 'a';
CREATE INDEX IDX_ENG_13852_VR5_VS  ON ENG_13852_VR5 (VCHAR_INLINE_MAX, SMALL) WHERE SMALL >= 0 AND VCHAR_INLINE_MAX IS NOT NULL;

CREATE VIEW ENG_13852_VP5 (SMALL, VCHAR, TINY,
       ID, INT, BIG, NUM, DEC,
       VCHAR_INLINE, VCHAR_INLINE_MAX, VCHAR_OUTLINE_MIN, VCHAR_JSON, TIME
       , VARBIN, POINT, POLYGON
       , IPV4, IPV6, VBIPV4, VBIPV6
) AS
SELECT SMALL, VCHAR, TINY,
       COUNT(*), COUNT(VARBIN), COUNT(POLYGON), SUM(NUM), SUM(DEC),
       MIN(VCHAR_INLINE), MAX(VCHAR_INLINE_MAX), MIN(VCHAR_OUTLINE_MIN), MAX(VCHAR_JSON), MIN(TIME)
       , MAX(VARBIN), MIN(POINT), MAX(POLYGON)
       , MIN(IPV4), MAX(IPV6), MIN(VBIPV4), MAX(VBIPV6)
FROM ENG_13852_P5
WHERE TINY < 64
GROUP BY SMALL, VCHAR, TINY;

CREATE INDEX IDX_VP5_SVT ON ENG_13852_VP5 (SMALL, VCHAR, TINY)      WHERE SMALL >= 0;
CREATE INDEX IDX_VP5_VID ON ENG_13852_VP5 (VCHAR_INLINE, INT, DEC)  WHERE VCHAR_INLINE < 'a';
CREATE INDEX IDX_VP5_VS  ON ENG_13852_VP5 (VCHAR_INLINE_MAX, SMALL) WHERE SMALL >= 0 AND VCHAR_INLINE_MAX IS NOT NULL;
