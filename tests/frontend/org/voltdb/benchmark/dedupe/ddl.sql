-- column checktime is # of milliseconds since Java epoch

create table archived (
  column1         bigint not null,
  column2         bigint not null,
  column3         bigint not null,
  column4         bigint not null,
  checktime       bigint not null
);

create index idx_archived on archived (column1, column2, column3, column4);
create index idx_archived_TREE on archived (checktime, column1, column2, column3, column4);

create table unarchived (
  column1         bigint not null,
  column2         bigint not null,
  column3         bigint not null,
  column4         bigint not null,
  checktime       bigint not null
);

create index idx_unarchived on unarchived (column1, column2, column3, column4);
create index idx_unarchived_TREE on unarchived (checktime, column1, column2, column3, column4);

