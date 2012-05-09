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

create table tpk (
  a bigint not null primary key,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

