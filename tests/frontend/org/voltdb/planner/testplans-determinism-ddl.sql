create table ttree (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create index cover2_TREE on ttree (a, b);
create index cover3_TREE on ttree (a, c, b);

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
CREATE INDEX TSINDEX ON eng4155 (ts DESC);
