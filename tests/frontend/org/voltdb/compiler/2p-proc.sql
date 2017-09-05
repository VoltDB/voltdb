create table tb1 (id int not null primary key, v int);
create table tb2 (id int not null primary key, v int);
partition table tb1 on column id;
partition table tb2 on column id;

insert into tb1 values (1, 10);
insert into tb1 values (2, 20);
insert into tb1 values (3, 30);

insert into tb2 values (1, 1000);
insert into tb2 values (2, 2000);
insert into tb2 values (3, 3000);

create procedure tp2p partition on table tb1 column id and on table tb2 column id as (select tb1.v + tb2.v as total from tb1, tb2 where tb1.id = ? and tb2.id = ?);		