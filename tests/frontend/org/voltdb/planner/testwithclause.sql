-- creates some test-tables and data
-- DROP TABLE EMPLOYEE;
-- DROP TABLE DEPARTMENT;
-- DROP TABLE SALARYGRADE;
-- DROP TABLE BONUS;
-- DROP TABLE PROJECT;
-- DROP TABLE PROJECT_PARTICIPATION;
-- DROP TABLE ROLE;

CREATE TABLE EMPLOYEE(
   empno      INTEGER NOT NULL,
   name       VARCHAR(10),
   job        VARCHAR(9),
   boss       INTEGER,
   hiredate   VARCHAR(12),
   salary     DECIMAL(7, 2),
   comm       DECIMAL(7, 2),
   deptno     INTEGER
);
 
CREATE TABLE DEPARTMENT(
   deptno     INTEGER NOT NULL,
   dept_count INTEGER NOT NULL,
   name       VARCHAR(14),
   location   VARCHAR(13)
);

CREATE TABLE SALARYGRADE(
   grade      INTEGER NOT NULL,
   losal      INTEGER NOT NULL,
   hisal      INTEGER NOT NULL
);

CREATE TABLE BONUS (
   ename      VARCHAR(10) NOT NULL,
   job        VARCHAR(9) NOT NULL,
   sal        DECIMAL(7, 2),
   comm       DECIMAL(7, 2)
);

CREATE TABLE PROJECT(
   projectno    INTEGER NOT NULL,
   description  VARCHAR(100),
   start_date   VARCHAR(12),
   end_date     VARCHAR(12)
);

CREATE TABLE PROJECT_PARTICIPATION(
   projectno    INTEGER NOT NULL,
   empno        INTEGER NOT NULL,
   start_date   VARCHAR(12) NOT NULL,
   end_date     VARCHAR(12),
   role_id      INTEGER
);

CREATE TABLE ROLE(
   role_id      INTEGER NOT NULL,
   description  VARCHAR(100)
);

-- Primary Keys
ALTER TABLE EMPLOYEE
   ADD CONSTRAINT emp_pk
   PRIMARY KEY (empno);

ALTER TABLE DEPARTMENT
   ADD CONSTRAINT dept_pk
   PRIMARY KEY (deptno);

ALTER TABLE SALARYGRADE
   ADD CONSTRAINT salgrade_pk
   PRIMARY KEY (grade);

ALTER TABLE BONUS
   ADD CONSTRAINT bonus_pk
   PRIMARY KEY (ename, job);

ALTER TABLE PROJECT
   ADD CONSTRAINT project_pk
   PRIMARY KEY (projectno);
 
ALTER TABLE PROJECT_PARTICIPATION
   ADD CONSTRAINT participation_pk
   PRIMARY KEY (projectno, empno, start_date);

ALTER TABLE ROLE
   ADD CONSTRAINT role_pk
   PRIMARY KEY (role_id);

