CREATE TABLE T (
	T_PKEY INTEGER NOT NULL,
	T_D1   INTEGER NOT NULL,
	T_D2   INTEGER NOT NULL,
	CONSTRAINT T_TREE PRIMARY KEY (T_PKEY,T_D1)
);

create table c (
    contestant_number   tinyint not null,
    contestant_name     varchar(50) not null,
    primary key (contestant_number)
);

create table v (
    contestant_number   tinyint not null,
    num_votes           int not null,
    primary key (contestant_number)
);
