create table R1 (
    i integer,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));

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
    i integer,
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
