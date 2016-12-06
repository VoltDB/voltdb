-- Define tables and other DDL, for use with the SQL grammar generator

DROP VIEW V_P1 IF EXISTS;
DROP VIEW V_P2 IF EXISTS;
DROP VIEW V_R1 IF EXISTS;
DROP VIEW V_R2 IF EXISTS;

DROP TABLE P1 IF EXISTS;
DROP TABLE P2 IF EXISTS;
DROP TABLE R1 IF EXISTS;
DROP TABLE R2 IF EXISTS;

CREATE TABLE P1 (
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
  POINT   GEOGRAPHY_POINT,
  POLYGON GEOGRAPHY,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE P2 (
  ID      INTEGER NOT NULL,
  TINY    TINYINT,
  SMALL   SMALLINT,
  INT     INTEGER,
  BIG     BIGINT,
  NUM     FLOAT,
  DEC     DECIMAL,
  VCHAR   VARCHAR(64 BYTES),
  VCHAR_INLINE_MAX VARCHAR(15),
  VCHAR_INLINE     VARCHAR(42 BYTES),
  TIME    TIMESTAMP,
  VARBIN  VARBINARY(100),
  POINT   GEOGRAPHY_POINT,
  POLYGON GEOGRAPHY,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID      INTEGER NOT NULL,
  TINY    TINYINT,
  SMALL   SMALLINT,
  INT     INTEGER,
  BIG     BIGINT,
  NUM     FLOAT,
  DEC     DECIMAL,
  VCHAR   VARCHAR(64 BYTES),
  VCHAR_INLINE_MAX VARCHAR(15),
  VCHAR_INLINE     VARCHAR(42 BYTES),
  TIME    TIMESTAMP,
  VARBIN  VARBINARY(100),
  POINT   GEOGRAPHY_POINT,
  POLYGON GEOGRAPHY,
  PRIMARY KEY (ID)
);

CREATE TABLE R2 (
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
  POINT   GEOGRAPHY_POINT,
  POLYGON GEOGRAPHY,
  PRIMARY KEY (ID)
);

CREATE VIEW VP1 (BIG, VCHAR, TOTAL_COUNT, ID_COUNT, TINY_COUNT, SMALL_COUNT, INT_COUNT, NUM_COUNT, DEC_COUNT,
    VCHAR_INLINE_MAX_COUNT, VCHAR_INLINE_COUNT, TIME_COUNT, VARBIN_COUNT, POINT_COUNT, POLYGON_COUNT,
    ID_SUM, TINY_SUM, SMALL_SUM, INT_SUM, NUM_SUM, DEC_SUM) AS
  SELECT BIG, VCHAR, COUNT(*), COUNT(ID), COUNT(TINY), COUNT(SMALL), COUNT(INT), COUNT(NUM), COUNT(DEC),
    COUNT(VCHAR_INLINE_MAX), COUNT(VCHAR_INLINE), COUNT(TIME), COUNT(VARBIN), COUNT(POINT), COUNT(POLYGON),
    SUM(ID), SUM(TINY), SUM(SMALL), SUM(INT), SUM(NUM), SUM(DEC)
  FROM P1
  GROUP BY BIG, VCHAR;

CREATE VIEW VR1 (BIG, TOTAL_COUNT, TINY_SUM, SMALL_SUM) AS
  SELECT BIG, COUNT(*), SUM(TINY), SUM(SMALL)
  FROM R1 WHERE ID > 5
  GROUP BY BIG;

CREATE VIEW VP2 (SMALL, TINY, TOTAL_COUNT, ID_SUM, BIG_SUM) AS 
  SELECT SMALL, TINY, COUNT(*), SUM(ID), SUM(BIG)
  FROM P2
  GROUP BY SMALL, TINY;

CREATE VIEW VR2 (SMALL_ABS, TINY, TOTAL_COUNT, ID_SUM, BIG_SUM) AS 
  SELECT ABS(SMALL), TINY, COUNT(*), SUM(ID), SUM(BIG)  FROM R2 
  GROUP BY ABS(SMALL), TINY;
