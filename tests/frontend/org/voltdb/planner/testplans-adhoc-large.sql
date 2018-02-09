create table t1 (
    id  bigint not null primary key,
    nm  varchar not null
);
partition table t1 on column id;
create index nmidx on t1 (nm);
