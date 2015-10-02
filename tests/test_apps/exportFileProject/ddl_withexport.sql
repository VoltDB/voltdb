CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);
PARTITION TABLE store ON COLUMN key;

CREATE TABLE store_export (
  key      varchar(250) not null
, value    varbinary(1040000) not null
, stamp timestamp not null
, txnid bigint not null
, random float not null
);
export table store_export;
