create table kv (
	pkey bigint default 0 not null,
	PRIMARY KEY(pkey)
);
PARTITION TABLE kv ON COLUMN pkey;

create table indexme (
	pkey bigint default 0 not null,
	c01 varchar(63) default null,
	c02 varchar(63) default null,
	c03 varchar(63) default null,
	c04 varchar(63) default null,
	c05 varchar(63) default null,
	c06 varchar(63) default null,
	c07 varchar(63) default null,
	c08 varchar(63) default null,
	c09 varchar(63) default null,
	c10 varchar(63) default null,
	PRIMARY KEY(pkey)
);
