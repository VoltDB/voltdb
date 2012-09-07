CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;
