drop table T if exists;
drop table AAA if exists;
drop table BBB if exists;
drop table R1 if exists;

CREATE TABLE T (
  A INTEGER,
  B INTEGER,
  C INTEGER
);

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  TINY INTEGER NOT NULL,
  BIG INTEGER NOT NULL,
  PRIMARY KEY (ID)
);

create table AAA (
  A integer,
  B integer,
  C integer
 );
 
 create table BBB (
  A integer,
  B integer,
  C integer
 );
 
-- Order By Table, from the order by suite.
--
CREATE TABLE O1 (
 PKEY          INTEGER NOT NULL,
 A_INT         INTEGER,
 PRIMARY KEY (PKEY)
);

PARTITION TABLE O1 ON COLUMN PKEY;
CREATE INDEX IDX_O1_A_INT_PKEY on O1 (A_INT, PKEY);
