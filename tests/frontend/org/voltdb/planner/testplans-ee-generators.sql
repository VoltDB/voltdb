drop table AAA if exists;
drop table BBB if exists;

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
 
create table CCC (
  id integer,
  name varchar(32),
  data varchar(1024)
);
 
create table XXX (
  id integer primary key not null,
  name varchar(32),
  data varchar(1024)
);
