create table r1 (
    id  bigint not null primary key,
    nm  varchar not null
);

create table r1_ididx (
    id  bigint not null primary key,
    nm  varchar not null
);
partition table r1_ididx on column id;
create index index_r1_ididx on r1_ididx (id);

create table p1 (
    id  bigint not null primary key,
    nm  varchar not null
);
partition table p1 on column id;

create table p1_ididx (
    id  bigint not null primary key,
    nm  varchar not null
);
partition table p1_ididx on column id;
create index index_p1_ididx on p1_ididx(id);


