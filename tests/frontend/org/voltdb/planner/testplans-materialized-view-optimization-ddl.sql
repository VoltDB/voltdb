create table t1(a int unique not null primary key, b int not null, a1 int, b1 int unique, c varchar(32));
create index ta on t1(a);

-- T2 is partitioned
create table t2(c0 int, b0 int not null);
create index tc on t2(c0);
partition table t2 on column b0;

-- Another table
create table t3(a int, a1 int, b int);
create index tb on t3(a1, b);

create view vt3 as select a1 a1, min(b) min_b, sum(a) sum_a, count(b) count_b, count(*) counts from t3 where abs(b) > abs(a) group by a1;

create view v2 as select distinct a1 distinct_a1, count(*) counts from t1 where b > 2 group by a1;
create view v5_1 as select distinct a1 distinct_a1, count(b1) count_b1, sum(a) sum_a, count(*) counts from t1 where b >= 2 or b1 in (3,30,300) group by a1;

-- Below 2 views are identical, since DISTINCT makes no differences
create view v5_2 as select distinct a1 distinct_a, sum(a) sum_a00, count(b) count_b00, count(*) counts from t1 group by a1;
create view v5_3 as select a1 distinct_a, sum(a) sum_a00, count(b) count_b00, count(*) counts from t1 group by a1;

-- multiple groups
create view v3 as select a a, b b, sum(a1) sum_a1, min(b1) min_b1, count(*) counts from t1 group by a, b;

-- complex group-bys
create view v4 as select a * 2 + a1 a2pa1, b - a b_minus_a, sum(a1) sum_a1, min(b1) min_b1, count(*) counts from t1 group by a * 2 + a1, b-a;

-- LIKE
create view v4_1 as select b1, count(*) counts from t1 where c in ('foo', 'bar', 'pattern') group by b1;

-- Testing on T2 with paritioning
create view vt2 as select distinct c0 distinct_c, sum(b0) sum_b, count(*) counts from t2 group by c0;
create view vt2_1 as select b0 b, sum(c0) sum_c, count(*) counts from t2 group by b0;

