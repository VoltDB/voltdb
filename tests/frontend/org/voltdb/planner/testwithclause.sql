-- creates some test-tables and data
-- DROP TABLE EMPLOYEE;
-- DROP TABLE DEPARTMENT;
-- DROP TABLE SALARYGRADE;
-- DROP TABLE BONUS;
-- DROP TABLE PROJECT;
-- DROP TABLE PROJECT_PARTICIPATION;
-- DROP TABLE ROLE;

CREATE TABLE EMPLOYEE(
   empno      INTEGER DEFAULT 0 NOT NULL PRIMARY KEY,
   name       VARCHAR(10),
   job        VARCHAR(9),
   boss       INTEGER,
   hiredate   VARCHAR(12),
   salary     DECIMAL(7, 2),
   comm       DECIMAL(7, 2),
   deptno     INTEGER
);
 
CREATE TABLE DEPARTMENT(
   deptno     INTEGER NOT NULL PRIMARY KEY,
   dept_count INTEGER NOT NULL,
   name       VARCHAR(14),
   location   VARCHAR(13)
);

CREATE TABLE SALARYGRADE(
   grade      INTEGER NOT NULL PRIMARY KEY,
   losal      INTEGER NOT NULL,
   hisal      INTEGER NOT NULL
);

CREATE TABLE BONUS (
   ename      VARCHAR(10) NOT NULL,
   job        VARCHAR(9) NOT NULL,
   sal        DECIMAL(7, 2),
   comm       DECIMAL(7, 2),
   PRIMARY KEY (ename, job)
);

CREATE TABLE PROJECT(
   projectno    INTEGER NOT NULL PRIMARY KEY,
   description  VARCHAR(100),
   start_date   VARCHAR(12),
   end_date     VARCHAR(12)
);

CREATE TABLE PROJECT_PARTICIPATION(
   projectno    INTEGER NOT NULL,
   empno        INTEGER NOT NULL,
   start_date   VARCHAR(12) NOT NULL,
   end_date     VARCHAR(12),
   role_id      INTEGER,
   PRIMARY KEY (projectno, empno, start_date)
);

CREATE TABLE ROLE(
   role_id      INTEGER NOT NULL PRIMARY KEY,
   description  VARCHAR(100)
);


