
-- Attribute for ea. supported type: non-null default
CREATE STREAM S_ALLOW_NULLS_REPL EXPORT TO TARGET S_ALLOW_NULLS_REPL (
 PKEY          INTEGER          NOT NULL,
 A_TINYINT     TINYINT          DEFAULT '1',
 A_SMALLINT    SMALLINT         DEFAULT '2',
 A_INTEGER     INTEGER          DEFAULT '3',
 A_BIGINT      BIGINT           DEFAULT '4',
 A_FLOAT       FLOAT            DEFAULT '5.1',
 A_TIMESTAMP   TIMESTAMP        DEFAULT '1970-01-01 00:00:00.6',
 A_INLINE_S1   VARCHAR(4)       DEFAULT 'abcd',
 A_INLINE_S2   VARCHAR(63)      DEFAULT 'abcdefghij',
 A_POOL_S      VARCHAR(65536)   DEFAULT 'abcdefghijklmnopqrstuvwxyz',
 A_POOL_MAX_S  VARCHAR(1048576) DEFAULT 'abcdefghijklmnopqrstuvwxyz',
 A_INLINE_B    VARBINARY(32)    DEFAULT 'ABCDEFABCDEF0123',
 A_POOL_B      VARBINARY(256)   DEFAULT 'ABCDEFABCDEF0123456789',
 A_DECIMAL     DECIMAL          DEFAULT '6',
 A_GEOGRAPHY_POINT GEOGRAPHY_POINT, -- default not supported for this type
 A_GEOGRAPHY   GEOGRAPHY,           -- default not supported for this type
);
