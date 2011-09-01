-- Data store table
CREATE TABLE cache
(
  Key          varchar(250)               NOT NULL
, Expires      int              DEFAULT 0 NOT NULL
, Flags        int              DEFAULT 0 NOT NULL
, Value        varbinary(1048576)
, CASVersion   bigint           DEFAULT 0 NOT NULL
, CONSTRAINT PK_cache PRIMARY KEY
  (
    Key
  )
);

-- Support index for cleanup operations
CREATE INDEX IX_cache_expires ON cache ( Expires );
