drop table T if exists;
drop table R1 if exists;
drop table AAA if exists;
drop table count_output if exists;
drop table test_sum_output if exists;
drop table BBB if exists;
drop table test_output if exists;
drop table CCC if exists;
drop table test_order_by if exists;
drop table test_join if exists;
drop table XXX if exists;
drop table rank_output if exists;
drop table rank_dense_output if exists;

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
 
create table COUNT_OUTPUT (
  A integer,
  B integer,
  C integer,
  D integer
 );
 
 create table test_sum_output (
  A integer,
  B integer,
  C integer
); 
 create table BBB (
  A integer,
  B integer,
  C integer
 );
 
create table test_output (
  A integer,
  B integer,
  C integer
);
 
create table CCC (
  id integer,
  name varchar(32),
  data varchar(1024)
);

create table CCCLongAns (
  id integer,
  name varchar(32),
  data varchar(1024)
);

create table CCCShortAns (
  id integer,
  name varchar(32),
  data varchar(1024)
);

create table test_order_by (
  a integer,
  b integer
);

create table test_join (
  a   integer,
  b   integer,
  c   integer
);

create table XXX (
  id integer primary key not null,
  name varchar(32),
  data varchar(1024)
);

create table rank_output (
  A integer,
  B integer,
  C integer,
  R integer
);

create table rank_dense_output (
  A integer,
  B integer,
  C integer,
  R integer
);
