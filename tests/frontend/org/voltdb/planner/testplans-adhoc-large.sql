create table r1 (
    id  bigint not null primary key,
    aa  bigint not null
);

create table r1_aaidx (
    id  bigint not null primary key,
    aa  bigint not null
);
create index index_r1_aaidx on r1_aaidx (aa);

create table p1 (
    id  bigint not null primary key,
    aa  bigint not null
);
partition table p1 on column id;

create table p1_aaidx (
    id  bigint not null primary key,
    aa  bigint not null
);
partition table p1_aaidx on column id;
create index index_p1_aaidx on p1_aaidx(aa);


