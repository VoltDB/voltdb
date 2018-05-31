CREATE TABLE P1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE P2 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  CONSTRAINT P2_PK_TREE PRIMARY KEY (ID)
);

CREATE TABLE R2 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  CONSTRAINT R2_PK_TREE PRIMARY KEY (ID)
);

-- Partitioned on ID but no PK.
CREATE TABLE P3 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER ASSUMEUNIQUE,
  RATIO FLOAT
);
PARTITION TABLE P3 ON COLUMN ID;

CREATE TABLE R3 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER UNIQUE,
  RATIO FLOAT
);

-- multi-column unique constraint
CREATE TABLE P4 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  CONSTRAINT uniq_multi_col ASSUMEUNIQUE (RATIO, NUM)
);
PARTITION TABLE P4 ON COLUMN ID;

-- No unique constraints
CREATE TABLE P5 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT
);
PARTITION TABLE P5 ON COLUMN ID;

CREATE TABLE P1_DECIMAL (
  ID INTEGER DEFAULT '0' NOT NULL,
  CASH DECIMAL NOT NULL,
  CREDIT DECIMAL NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE R1_DECIMAL (
  ID INTEGER DEFAULT '0' NOT NULL,
  CASH DECIMAL NOT NULL,
  CREDIT DECIMAL NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE COUNT_NULL (
  TRICKY TINYINT,
  ID INTEGER DEFAULT '0' NOT NULL,
  NUM INTEGER DEFAULT '0' NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE OBJECT_DETAIL (
  OBJECT_DETAIL_ID INTEGER NOT NULL,
  NAME VARCHAR(256) NOT NULL,
  DESCRIPTION VARCHAR(1024) NOT NULL,
  PRIMARY KEY (OBJECT_DETAIL_ID)
);

CREATE TABLE ASSET (
  ASSET_ID INTEGER NOT NULL,
  OBJECT_DETAIL_ID INTEGER NOT NULL,
  PRIMARY KEY (ASSET_ID)
);

CREATE TABLE STRINGPART (
  NAME VARCHAR(9) NOT NULL,
  VAL1 INTEGER NOT NULL,
  VAL2 INTEGER,
  NUM INTEGER DEFAULT 0,
  DESC VARCHAR(512),
  PRIMARY KEY(VAL1)
);

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

create index eng1850_tree_pid_aid on ENG1850 (
  pid, aid
);

CREATE TABLE DEFAULT_NULL (
  ID INTEGER NOT NULL,
  num1 INTEGER DEFAULT NULL,
  num2 INTEGER ,
  ratio FLOAT DEFAULT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE NO_JSON (
  ID INTEGER NOT NULL,
  var1 VARCHAR(300),
  var2 VARCHAR(300),
  var3 VARCHAR(300),
  PRIMARY KEY (ID)
);
CREATE INDEX test_field ON NO_JSON (var2, field(var3,'color'));

CREATE VIEW P1_VIEW (ID, ID_COUNT, NUM_SUM) AS
SELECT ID, COUNT(*), SUM(NUM)
FROM P1
GROUP BY ID;

CREATE TABLE T_ENG_11172 ( 
  ID INTEGER NOT NULL, 
  TINY TINYINT, 
  SMALL SMALLINT, 
  INT INTEGER, 
  BIG BIGINT, 
  NUM FLOAT, 
  DEC DECIMAL, 
  VCHAR VARCHAR(500), 
  VCHAR_INLINE_MAX VARCHAR(63 BYTES), 
  VCHAR_INLINE VARCHAR(14), 
  TIME TIMESTAMP, 
  VARBIN VARBINARY(100), 
  POINT GEOGRAPHY_POINT, 
  POLYGON GEOGRAPHY, 
  PRIMARY KEY (ID)
);

CREATE TABLE ENG_11918 (
  ID INTEGER,
  INT INTEGER,
  VCHAR VARCHAR(64),
  TIME TIMESTAMP
);

CREATE TABLE ENG_13926 (
  A INTEGER,
  B INTEGER,
  C TINYINT,
  D INTEGER
);
