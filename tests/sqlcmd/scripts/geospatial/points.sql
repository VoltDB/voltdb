drop procedure make_point if exists;
drop table points if exists;

create table points (
  id         integer primary key not null,
  location   geography_point
);

create procedure make_point as
  insert into points values (?, ?);
