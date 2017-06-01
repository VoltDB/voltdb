create table test (a int not null);
partition table test on column a;

create procedure tp as select top 3 * from test;

create table test2 (a int, b int not null, primary key (a, b));
partition table test2 on column b;

create procedure tp2 as select top 1 * from test2;