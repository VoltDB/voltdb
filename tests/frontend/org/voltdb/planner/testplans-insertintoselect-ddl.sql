CREATE TABLE target_p (bi bigint not null,
                       vc varchar(100) default 'daschund',
                       ii integer default 127,
		       ti tinyint default 7);
partition table target_p on column bi;

CREATE TABLE target_r (bi bigint not null,
vc varchar(100) default 'daschund',
ii integer default 127,
ti tinyint default 7);

CREATE TABLE source_p1 (bi bigint not null,
vc varchar(100) default 'daschund',
ii integer default 127,
ti tinyint default 7);
partition table source_p1 on column bi;

CREATE TABLE source_p2 (bi bigint not null,
vc varchar(100) default 'daschund',
ii integer default 127,
ti tinyint default 7);
partition table source_p2 on column bi;

CREATE TABLE source_r (bi bigint not null,
vc varchar(100) default 'daschund',
ii integer default 127,
ti tinyint default 7);
