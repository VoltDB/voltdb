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
    bi bigint,
    ti tinyint
);
CREATE INDEX RI1_IND2 ON RI1 (bi, si);
CREATE INDEX RI1_IND1 ON RI1 (ti);
CREATE INDEX RI1_IND3_EXPR ON RI1 (ti + 1);
CREATE INDEX RI1_IND3_EXPR_PART ON RI1 (ti + 1) WHERE si is NULL;
CREATE INDEX RI1_IND4_PART ON RI1 (si) WHERE tI * 2 > 10;

create table R2 (
    i integer,
    si smallint,
    ti tinyint,
    bi bigint,
    f float not null,
    v varchar(32));

create table RI2 (
    i integer PRIMARY KEY,
    si smallint,
    bi bigint,
    ti tinyint
);
CREATE INDEX RI2_IND2 ON RI2 (i, bi);
CREATE INDEX RI2_IND1 ON RI2 (ti);
CREATE INDEX RI2_IND3 ON RI2 (ti + i);
CREATE INDEX RI2_IND4_HASH ON RI2 (ti + i);
CREATE INDEX RI2_IND5_HASH ON RI2 (ti);

create table R3 (
    pk integer,
    vc varchar(256),
    ii integer);

create table RI3 (
    pk integer,
    vc varchar(256),
    ii integer,
    iii integer);

CREATE UNIQUE INDEX RI3_IND1_HASH ON RI3 (ii);
CREATE INDEX RI3_IND2 ON RI3 (ii);

create table RI4 (
    i integer,
    ii integer);

CREATE UNIQUE INDEX RI4_IND1_HASH ON RI4 (i);
CREATE INDEX RI4_IND2 ON RI4 (i);
CREATE INDEX RI4_IND3 ON RI4 (i + ii);


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

create table PI1 (
    i integer not null PRIMARY KEY,
    si smallint,
    ii integer,
    bi bigint,
    f float not null,
    v varchar(32));
    partition table PI1 on column i;
    CREATE INDEX PI1_IND1 ON PI1 (ii);

