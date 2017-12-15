create table cte_table (
   id           bigint not null primary key,
   name         varchar(1024),
   left_rent    bigint,
   right_rent   bigint
);

create table rrt (
    id          bigint,
    name        varchar(1024),
    l           bigint,
    r           bigint
);
