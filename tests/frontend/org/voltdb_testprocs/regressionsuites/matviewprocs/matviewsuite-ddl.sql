CREATE TABLE PEOPLE (PARTITION INTEGER NOT NULL, ID INTEGER, AGE INTEGER, SALARY FLOAT, CHILDREN INTEGER, PRIMARY KEY(ID));
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

CREATE PROCEDURE INDEXED_FIRST_GROUP AS SELECT AGE, SALARIES FROM MATPEOPLE ORDER BY SALARIES DESC LIMIT 1;

CREATE PROCEDURE INDEXED_MAX_GROUP     AS SELECT MAX(SALARIES) FROM MATPEOPLE;
CREATE PROCEDURE INDEXED_MAX_IN_GROUPS AS SELECT MAX(SALARIES) FROM MATPEOPLE WHERE AGE = ?;
CREATE PROCEDURE INDEXED_GROUPS        AS
    SELECT AGE, SALARIES, PARTITION, NUM, KIDS
    FROM MATPEOPLE ORDER BY AGE, SALARIES;


CREATE TABLE THINGS (ID INTEGER, PRICE INTEGER, PRIMARY KEY(ID));
CREATE VIEW MATTHINGS (PRICE, NUM) AS SELECT PRICE, COUNT(*) FROM THINGS GROUP BY PRICE;

CREATE TABLE OVERFLOWTEST ( col_pk BIGINT NOT NULL, col_1 INTEGER NOT NULL, col_integer INTEGER, col_bigint BIGINT, PRIMARY KEY (COL_PK));
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

