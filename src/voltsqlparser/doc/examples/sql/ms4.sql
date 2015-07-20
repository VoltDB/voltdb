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

insert into beta values (local, id) (1, 100), (2, 200), (3, 300);

select alpha.id from alpha;

select alpha.id from alpha where alpha.id < 250;

select id from alpha;

select id from alpha, beta;

select id from alpha where id < 250;

select local, id from beta where local < 150;

select mumble.id from alpha as mumble where id < 100;

select alpha.id from alpha, beta where alpha.id < 100;

select id from alpha, beta where id < 100;

select local as mumble, id as bazzle from beta as mumble where mumble.local < 150;

Ambiguous: Use the first table, mumble.id for id < 100.
select mumble.id from alpha as mumble, beta as bazzle where id < 100;

Error expected:
select mumble.id from alpha as mumble, beta as bazzle
    where alpha.id < 100 and beta.id < 100;

    