create table cte_table (
   id           bigint not null primary key,
   name         varchar(1024),
   left_rent    bigint,
   right_rent   bigint
);

create table rrt (
    id          bigint,
    name        varchar(1024),
    l           bigint,
    r           bigint
);

-- This is from the test CommonTableExpressionTest.cpp

CREATE TABLE EMPLOYEES (
    LAST_NAME VARCHAR(20) NOT NULL,
    EMP_ID INTEGER NOT NULL,
    MANAGER_ID INTEGER
);

PARTITION TABLE EMPLOYEES ON COLUMN EMP_ID;

-- We add 0*? to force a parameter to this procedure,
-- though we really don't care about its value.
CREATE PROCEDURE EETestQuery AS
WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS (
    SELECT LAST_NAME, EMP_ID + ?, MANAGER_ID, 1, LAST_NAME
    FROM EMPLOYEES
      WHERE MANAGER_ID IS NULL
  UNION ALL
    SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || ‘/’ || E.LAST_NAME
    FROM EMPLOYEES E JOIN EMP_PATH EP ON E.MANAGER_ID = EP.EMP_ID
  )
SELECT * FROM EMP_PATH;

PARTITION PROCEDURE EETestQuery ON TABLE EMPLOYEES COLUMN EMP_ID PARAMETER 0;

