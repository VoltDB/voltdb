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
PARTITION PROCEDURE Add ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Append;
PARTITION PROCEDURE Append ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.CheckAndSet;
PARTITION PROCEDURE CheckAndSet ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Cleanup;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Delete;
PARTITION PROCEDURE Delete ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.IncrDecr;
PARTITION PROCEDURE IncrDecr ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Prepend;
PARTITION PROCEDURE Prepend ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Replace;
PARTITION PROCEDURE Replace ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Set;
PARTITION PROCEDURE Set ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Get;
PARTITION PROCEDURE Get ON TABLE cache COLUMN Key;
CREATE PROCEDURE FROM CLASS voltcache.procedures.Gets;
CREATE PROCEDURE FROM CLASS voltcache.procedures.FlushAll;

IMPORT CLASS voltcache.procedures.VoltCacheProcBase;
