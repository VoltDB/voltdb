
CREATE TABLE O1 (
 PKEY          INTEGER NOT NULL,
 A_INT         INTEGER,
 A_INLINE_STR  VARCHAR(10),
 A_POOL_STR    VARCHAR(1024),
 PRIMARY KEY (PKEY)
);

PARTITION TABLE O1 ON COLUMN PKEY;
CREATE PROCEDURE Truncate01 AS DELETE FROM O1;

-- replicated table used in pro TestCatalogUpdateSuite test
CREATE TABLE O2 (
 PKEY          INTEGER,
 A_INT         INTEGER,
 A_INLINE_STR  VARCHAR(10),
 A_POOL_STR    VARCHAR(1024),
 PRIMARY KEY (PKEY)
);
 

CREATE TABLE O3 (
 PK1 INTEGER NOT NULL,
 PK2 INTEGER NOT NULL,
 I3  INTEGER NOT NULL,
 I4  INTEGER NOT NULL,
 PRIMARY KEY (PK1, PK2)
 );

CREATE INDEX O3_TREE ON O3 (I3 DESC);
CREATE PROCEDURE Truncate03 AS DELETE FROM O3;


create table a
(
  a integer not null
);

partition table a on column a;

create table b
(
  a integer not null
);

create procedure TruncateA as DELETE FROM A;
create procedure TruncateB as DELETE FROM B;
