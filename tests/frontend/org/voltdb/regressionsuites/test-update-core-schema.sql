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
CREATE INDEX I0 ON indexme (c01,c02,c03);
CREATE INDEX I1 ON indexme (c02,c03,c04);
CREATE INDEX I2 ON indexme (c03,c04,c05);
CREATE INDEX I3 ON indexme (c04,c05,c06);
CREATE INDEX I4 ON indexme (c05,c06,c07);
CREATE INDEX I5 ON indexme (c06,c07,c08);
CREATE INDEX I6 ON indexme (c07,c08,c09);
CREATE INDEX I7 ON indexme (c08,c09,c10);