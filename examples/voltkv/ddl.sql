CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

CREATE TABLE storeR
(
    key      varchar(250) not null
    , value    varbinary(1048576) not null
    , PRIMARY KEY (key)
);

CREATE PROCEDURE selectR AS SELECT key, value from storeR where key=?;
