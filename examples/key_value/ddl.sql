create table key_value (
  key_column      varchar(250) not null,
  value_column    varbinary(1048576) not null,
  primary key (key_column));

