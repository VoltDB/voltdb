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

PARTITION TABLE cache ON COLUMN Key;

CREATE PROCEDURE FROM CLASS voltcache.procedures.Add;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Append;
CREATE PROCEDURE FROM CLASS voltcache.procedures.CheckAndSet;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Cleanup;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Delete;
CREATE PROCEDURE FROM CLASS voltcache.procedures.IncrDecr;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Prepend;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Replace;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Set;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Get;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Gets;
CREATE PROCEDURE FROM CLASS voltcache.procedures.FlushAll;
CREATE PROCEDURE FROM CLASS voltcache.procedures.VoltCacheProcBase;
