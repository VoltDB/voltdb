create table r1 (
    id  bigint not null,
    aa  bigint not null
);

create table r1_idpk (
    id  bigint not null primary key,
    aa  bigint not null
);

create table r1_aaidx (
    id  bigint not null primary key,
    aa  bigint not null
);
create index index_r1_aaidx on r1_aaidx (aa);

create table r1_allidx (
    id  bigint not null primary key,
    aa  bigint not null
);
create unique index index_r1_allidx on r1_allidx (aa, id);

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

create table paa_aaidx (
    id  bigint not null,
    aa  bigint not null
);
partition table paa_aaidx on column aa;
create index index_paa_aaidx on p1_aaidx(aa);

