# This is milestone 3.
# 1. No new types.
# 2. We can create tables and insert into them.
# 3. We can do simple selects with inner joins and simple join conditions.

create table alpha (
  id bigint
);

create table beta (
  id bigint,
  local integer
);

insert into alpha (id) values (1), (2), (3);

insert into beta (local, id) (1, 100), (2, 200), (3, 300);

select * from alpha;

select * from alpha where id < 250;

select * from alpha where id < -1;

select (local, id) from beta where local < 150;
