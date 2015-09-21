
create table data (
       pk bigint not null primary key,
       value bigint not null,
);

partition table data on column pk;

load classes procs.jar;
create procedure from class approxcountdistinct.DoCount;
