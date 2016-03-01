create table table1r_el_only (
 column1_bigint      bigint not null,
 column2_integer     integer,
 column3_smallint    smallint,
 column4_tinyint     tinyint,
 column5_float       float,
 column6_decimal     decimal,
 column8_varchar     varchar(100),
 column9_timestamp   timestamp,
 column10_bigint     bigint
);
create view v_table1r_el_only (column2_integer, num_rows)
as select column2_integer as column2_integer,
      count(*) as num_rows
from table1r_el_only
group by column2_integer;

-- partition column in view good one
create table table2r_el_only (
 column1_bigint      bigint not null,
 column2_integer     integer,
 column3_smallint    smallint,
 column4_tinyint     tinyint,
 column5_float       float,
 column6_decimal     decimal,
 column8_varchar     varchar(100),
 column9_timestamp   timestamp,
 column10_bigint     bigint
);
partition table table2r_el_only on column column1_bigint;

create view v_table2r_el_only (column1_bigint, column2_integer, num_rows)
as select column1_bigint, column2_integer,
      count(*) as num_rows
from table2r_el_only
group by column1_bigint, column2_integer;

-- No partition column on export table
create table table3r_el_only (
 column1_bigint      bigint not null,
 column2_integer     integer,
 column3_smallint    smallint,
 column4_tinyint     tinyint,
 column5_float       float,
 column6_decimal     decimal,
 column8_varchar     varchar(100),
 column9_timestamp   timestamp,
 column10_bigint     bigint
);

create view v_table3r_el_only (column1_bigint, column2_integer, num_rows)
as select column1_bigint, column2_integer as column2_integer,
      count(*) as num_rows
from table3r_el_only
group by column1_bigint, column2_integer;

-- partition column on export table but not in view
create table table4r_el_only (
 column1_bigint      bigint not null,
 column2_integer     integer,
 column3_smallint    smallint,
 column4_tinyint     tinyint,
 column5_float       float,
 column6_decimal     decimal,
 column8_varchar     varchar(100),
 column9_timestamp   timestamp,
 column10_bigint     bigint
);
partition table table4r_el_only on column column1_bigint;

create view v_table4r_el_only (column2_integer, num_rows)
as select column2_integer as column2_integer,
      count(*) as num_rows
from table4r_el_only
group by column2_integer;
