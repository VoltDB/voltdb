create table test (a int not null);
partition table test on column a;

create procedure tp as select top 3 * from test;

create table test2 (a int, b int not null, primary key (a, b));
partition table test2 on column b;

create procedure tp2 as select b, count(*) from test2 group by b limit 3;

create table test3 (a int, b int not null, c int not null, primary key (a, b, c));
partition table test3 on column b;

create procedure tp3 as select max(temp.b) from (select top 3 * from test3) as temp;