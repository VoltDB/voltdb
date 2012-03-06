create table t (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  d bigint not null,
  e bigint not null
);

create index idx_1 on t (a, b, c, d);
create index idx_2_TREE on t (e, a, b, c, d);

create index cover2_TREE on t (a, b);
create index cover3_TREE on t (a, c, b);
