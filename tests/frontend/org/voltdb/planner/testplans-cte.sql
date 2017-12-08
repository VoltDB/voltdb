create table cte_table (
   id           bigint not null primary key,
   name         varchar,
   left_rent    bigint,
   right_rent   bigint
);

create table simple_id_name (
   id           bigint not null primary key,
   name         varchar,
);
  