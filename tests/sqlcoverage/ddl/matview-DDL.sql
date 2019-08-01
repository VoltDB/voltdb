CREATE TABLE P1 (
  ID INTEGER NOT NULL,
  TINY TINYINT NOT NULL,
  SMALL SMALLINT,
  BIG BIGINT
);
PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  TINY TINYINT NOT NULL,
  SMALL SMALLINT,
  BIG BIGINT,
  PRIMARY KEY (ID)
);

CREATE VIEW MATP1 (BIG, ID, NUM, IDCOUNT, TINYCOUNT, SMALLCOUNT, BIGCOUNT, TINYSUM, SMALLSUM) AS
  SELECT BIG, ID, COUNT(*), COUNT(ID), COUNT(TINY), COUNT(SMALL), COUNT(BIG), SUM(TINY), SUM(SMALL)
  FROM P1
  GROUP BY BIG, ID;

CREATE VIEW MATR1 (BIG, NUM, TINYSUM, SMALLSUM) AS
  SELECT BIG, COUNT(*), SUM(TINY), SUM(SMALL)
  FROM R1 WHERE ID > 5
  GROUP BY BIG;

CREATE TABLE P2 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);
PARTITION TABLE P2 ON COLUMN ID;

CREATE TABLE R2 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);

CREATE VIEW V_P2 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
    SELECT wage, dept, count(*), sum(age), sum(rent)  FROM P2
    GROUP BY wage, dept;

CREATE VIEW V_R2 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
    SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R2
    GROUP BY wage, dept;

CREATE VIEW V_R2_ABS (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS
    SELECT ABS(wage), dept, count(*), sum(age), sum(rent)  FROM R2
    GROUP BY ABS(wage), dept;

-- Materialized Views with 0, 1, 2, 3, or 4 GROUP BY columns
CREATE VIEW P2_V0 (CNT,      WAGE,      DEPT,        AGE,      RENT,      ID      ) AS
    SELECT         COUNT(*), MIN(WAGE), COUNT(DEPT), MAX(AGE), SUM(RENT), COUNT(ID) FROM P2;

CREATE VIEW R2_V0 (CNT,      WAGE,      DEPT,        AGE,      RENT,      ID      ) AS
    SELECT         COUNT(*), MIN(WAGE), COUNT(DEPT), MAX(AGE), SUM(RENT), COUNT(ID) FROM R2;

CREATE VIEW P2_V1 (WAGE, CNT,      DEPT,        AGE,      RENT,      ID    ) AS
    SELECT         WAGE, COUNT(*), COUNT(DEPT), MIN(AGE), SUM(RENT), MAX(ID) FROM P2
    GROUP BY       WAGE;

CREATE VIEW R2_V1 (WAGE, CNT,      DEPT,        AGE,      RENT,      ID    ) AS
    SELECT         WAGE, COUNT(*), COUNT(DEPT), MIN(AGE), SUM(RENT), MAX(ID) FROM R2
    GROUP BY       WAGE;

CREATE VIEW P2_V2 (WAGE, DEPT, CNT,      AGE,      RENT,      ID    ) AS
    SELECT         WAGE, DEPT, COUNT(*), MIN(AGE), SUM(RENT), MAX(ID) FROM P2
    GROUP BY       WAGE, DEPT;

CREATE VIEW R2_V2 (WAGE, DEPT, CNT,      AGE,      RENT,      ID    ) AS
    SELECT         WAGE, DEPT, COUNT(*), MIN(AGE), SUM(RENT), MAX(ID) FROM R2
    GROUP BY       WAGE, DEPT;

CREATE VIEW P2_V2A (WAGE,     DEPT, CNT,      AGE,      RENT,        ID    ) AS
    SELECT         ABS(WAGE), DEPT, COUNT(*), MAX(AGE), COUNT(RENT), MIN(ID) FROM P2
    WHERE          ABS(WAGE) < 60 AND ABS(AGE) >= 30 AND ABS(AGE) < 65
    GROUP BY       ABS(WAGE), DEPT;

CREATE VIEW R2_V2A (WAGE,     DEPT, CNT,      AGE,      RENT,        ID    ) AS
    SELECT         ABS(WAGE), DEPT, COUNT(*), MAX(AGE), COUNT(RENT), MIN(ID) FROM R2
    WHERE          ABS(WAGE) < 60 AND ABS(AGE) >= 30 AND ABS(AGE) < 65
    GROUP BY       ABS(WAGE), DEPT;

CREATE VIEW P2_V3 (WAGE, DEPT, AGE, CNT,      RENT,      ID    ) AS
    SELECT         WAGE, DEPT, AGE, COUNT(*), SUM(RENT), SUM(ID) FROM P2
    WHERE          DEPT  IN    (1, 2, 3, 5, 11, 16, 27, 43, 70, 113)
    GROUP BY       WAGE, DEPT, AGE;

CREATE VIEW R2_V3 (WAGE, DEPT, AGE, CNT,      RENT,      ID    ) AS
    SELECT         WAGE, DEPT, AGE, COUNT(*), SUM(RENT), SUM(ID) FROM R2
    WHERE          DEPT  IN    (1, 2, 3, 5, 11, 16, 27, 43, 70, 113)
    GROUP BY       WAGE, DEPT, AGE;

CREATE VIEW P2_V4 (WAGE, DEPT, AGE, RENT, CNT,          ID) AS
    SELECT         WAGE, DEPT, AGE, RENT, COUNT(*), MIN(ID) FROM P2
    WHERE          ABS(WAGE) < 60 AND ABS(AGE) BETWEEN 30 AND 64
    GROUP BY       WAGE, DEPT, AGE, RENT;

CREATE VIEW R2_V4 (WAGE, DEPT, AGE, RENT, CNT,          ID) AS
    SELECT         WAGE, DEPT, AGE, RENT, COUNT(*), MAX(ID) FROM R2
    WHERE          ABS(WAGE) < 60 AND ABS(AGE) BETWEEN 30 AND 64
    GROUP BY       WAGE, DEPT, AGE, RENT;

-- This table is for testing three table joins, since MV partitioned table
-- (or view) can only join with two more replicated tables.
CREATE TABLE R2V (
  V_G1 INTEGER NOT NULL,
  V_G2 SMALLINT,
  V_CNT SMALLINT,
  V_sum_age SMALLINT,
  V_sum_rent SMALLINT,
  PRIMARY KEY (V_G1)
);

-- Materialized Views without COUNT(*) (per ENG-14114); column names may not
-- make sense, but match those of other materialized views above (V_P2, V_R2,
-- V_R2_ABS; table R2V) that are used in the same or similar test suites.
-- "NCS" stands for "No Count Star".
CREATE VIEW V_P2_NCS (V_G1, V_G2, V_CNT,   V_sum_age, V_sum_rent) AS
    SELECT            WAGE, DEPT, MAX(ID), MIN(AGE),  SUM(RENT) FROM P2
    GROUP BY          WAGE, DEPT;

CREATE VIEW V_R2_NCS (V_G1, V_G2, V_CNT,   V_sum_age, V_sum_rent) AS
    SELECT            WAGE, DEPT, MAX(ID), MIN(AGE),  SUM(RENT) FROM R2
    GROUP BY          WAGE, DEPT;

CREATE VIEW V_R2_ABS_NCS (V_G1,      V_G2, V_CNT,   V_sum_age, V_sum_rent) AS
    SELECT                ABS(WAGE), DEPT, MAX(ID), MIN(AGE),  SUM(RENT) FROM R2
    GROUP BY              ABS(WAGE), DEPT;
