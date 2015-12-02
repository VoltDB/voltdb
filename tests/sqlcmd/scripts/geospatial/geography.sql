drop table polygons if exists;

create table polygons (
  id         integer primary key not null,
  location   geography
);

create procedure make_polygon as
  insert into polygons values (?, ?);
