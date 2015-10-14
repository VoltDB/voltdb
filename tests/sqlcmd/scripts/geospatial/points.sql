drop table points if exists;

create table points (
  id      integer primary key not null,
  point   point not null
);

create procedure make_point as
  insert into points values (?, ?);
