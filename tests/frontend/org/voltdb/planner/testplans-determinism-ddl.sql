create table tonecolumn (
  a bigint not null primary key
);

create table ttree (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create index cover2_TREE on ttree (a, b);
create index cover3_TREE on ttree (a, c, b);

create table ttree_with_key (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  d bigint not null,
  e bigint not null,
  id bigint not null,
  primary key (a, b, c)
);

create table thash (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create index cover2 on thash (a, b);
create index cover3 on thash (a, c, b);

create table tunique (
  a bigint not null unique,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create table tuniqcombo (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create unique index cover3_UNIQCOMBO on tuniqcombo (a, c, b);

create table tpk (
  a bigint not null primary key,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

CREATE TABLE eng4155 (
  id bigint NOT NULL,
  ts bigint NOT NULL,
  foo bigint NOT NULL,
  CONSTRAINT PK_id_eng4155 PRIMARY KEY (id)
);
CREATE INDEX TSINDEX ON eng4155 (ts);

create table ptree (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table ptree on column a;

create index pcover2_TREE on ptree (a, b);
create index pcover3_TREE on ptree (a, c, b);

create table phash (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table phash on column a;

create index pcover2 on phash (a, b);
create index pcover3 on phash (a, c, b);

create table punique (
  a bigint not null unique,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table punique on column a;

create table puniqcombo (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table puniqcombo on column a;

create unique index pcover3_UNIQCOMBO on puniqcombo (a, c, b);

create table ppk (
  a bigint not null primary key,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table ppk on column a;

create table floataggs (
    alpha float,
    beta  float,
    gamma float
);


create table ppkcombo (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null,
  PRIMARY KEY (a, b ,c)
);
create unique index pcover2_ppkCombo on ppkcombo (a, z);
partition table ppkcombo on column a;

