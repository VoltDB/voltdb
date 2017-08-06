create table R1 (
    i integer,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));

create table RI1 (
    i integer PRIMARY KEY,
    si smallint,
    bi bigint
);
CREATE INDEX RI1_IND2 ON RI1 (i, bi);

create table R2 (
    i integer,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));

create table R3 (
    pk integer,
    vc varchar(256));

create table RTYPES (
    bi bigint,
    d decimal,
    f float,
    i integer,
    si smallint,
    ti tinyint,
    ts timestamp,
    vb VARBINARY(1024),
    vc varchar(256));

create table P1 (
    i integer not null,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));
    partition table P1 on column i;

create table P2 (
    i integer not null,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));
    partition table P2 on column i;

create table P3 (
    i integer not null,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));
    partition table P3 on column i;
