-- visitTime is # of milliseconds since Java epoch

create table visit_archived (
  userId        bigint not null,
  field1        bigint not null,
  field2        bigint not null,
  field3        bigint not null,
  visitTime     bigint not null
);

create index idx_visit_archived on visit_archived (userId, field1, field2, field3);
create index idx_visit_archived_TREE on visit_archived (visitTime, userId, field1, field2, field3);

create table visit_unarchived (
  userId        bigint not null,
  field1        bigint not null,
  field2        bigint not null,
  field3        bigint not null,
  visitTime     bigint not null
);

create index idx_visit_unarchived on visit_unarchived (userId, field1, field2, field3);
create index idx_visit_unarchived_TREE on visit_unarchived (visitTime, userId, field1, field2, field3);


create table visit_el (
  userId        bigint not null,
  field1        bigint not null,
  field2        bigint not null,
  field3        bigint not null,
  visitTime     bigint not null
);

