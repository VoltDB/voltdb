--
-- The following four tables are used to test insert and select of
-- min, max, 0, NULL and some normal values of each SQL type. These
-- tables are not designed to test any partitioning effects.
--

-- Attribute for ea. supported type: nulls allowed
CREATE TABLE ALLOW_NULLS (
 PKEY          INTEGER       NOT NULL,
 A_TINYINT     TINYINT,
 A_SMALLINT    SMALLINT,
 A_INTEGER     INTEGER,
 A_BIGINT      BIGINT,
 A_FLOAT       FLOAT,
 A_TIMESTAMP   TIMESTAMP,
 A_INLINE_S1   VARCHAR(4),
 A_INLINE_S2   VARCHAR(63),
 A_POOL_S      VARCHAR(65536),
 A_POOL_MAX_S  VARCHAR(1048576),
 A_INLINE_B    VARBINARY(32),
 A_POOL_B      VARBINARY(256),
 A_DECIMAL     DECIMAL,
 A_GEOGRAPHY_POINT GEOGRAPHY_POINT,
 A_GEOGRAPHY   GEOGRAPHY,
 PRIMARY KEY (PKEY)
);

-- Attribute for ea. supported type: non-null default
CREATE TABLE WITH_DEFAULTS (
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
 PRIMARY KEY (PKEY)
);

-- Attribute for ea. supported type: null is default
CREATE TABLE WITH_NULL_DEFAULTS (
 PKEY          INTEGER        NOT NULL,
 A_TINYINT     TINYINT,
 A_SMALLINT    SMALLINT,
 A_INTEGER     INTEGER,
 A_BIGINT      BIGINT,
 A_FLOAT       FLOAT,
 A_TIMESTAMP   TIMESTAMP,
 A_INLINE_S1   VARCHAR(4),
 A_INLINE_S2   VARCHAR(63),
 A_POOL_S      VARCHAR(65536),
 A_POOL_MAX_S  VARCHAR(1048576),
 A_INLINE_B    VARBINARY(32),
 A_POOL_B      VARBINARY(256),
 A_DECIMAL     DECIMAL,
 A_GEOGRAPHY_POINT GEOGRAPHY_POINT,
 A_GEOGRAPHY   GEOGRAPHY,
 PRIMARY KEY (PKEY)
);


--
-- This table is used to test some basic expressions and aggregations
-- against all types when nulls are present. Inserted values here are
-- chosen specifically not to test overflow or underflow.
--
CREATE TABLE EXPRESSIONS_WITH_NULLS (
 PKEY          INTEGER          NOT NULL,
 A_TINYINT     TINYINT          DEFAULT NULL,
 A_SMALLINT    SMALLINT         DEFAULT NULL,
 A_INTEGER     INTEGER          DEFAULT NULL,
 A_BIGINT      BIGINT           DEFAULT NULL,
 A_FLOAT       FLOAT            DEFAULT NULL,
 A_TIMESTAMP   TIMESTAMP        DEFAULT NULL,
 A_INLINE_S1   VARCHAR(4)       DEFAULT NULL,
 A_INLINE_S2   VARCHAR(63)      DEFAULT NULL,
 A_POOL_S      VARCHAR(65536)   DEFAULT NULL,
 A_POOL_MAX_S  VARCHAR(1048576) DEFAULT NULL,
 A_INLINE_B    VARBINARY(32)    DEFAULT NULL,
 A_POOL_B      VARBINARY(256)   DEFAULT NULL,
 A_DECIMAL     DECIMAL          DEFAULT NULL,
 A_GEOGRAPHY_POINT GEOGRAPHY_POINT,
 A_GEOGRAPHY   GEOGRAPHY,
 PRIMARY KEY (PKEY)
);


--
-- As above without any mixed nulls
--
CREATE TABLE EXPRESSIONS_NO_NULLS (
 PKEY          INTEGER      NOT NULL,
 A_TINYINT     TINYINT,
 A_SMALLINT    SMALLINT,
 A_INTEGER     INTEGER,
 A_BIGINT      BIGINT,
 A_FLOAT       FLOAT,
 A_TIMESTAMP   TIMESTAMP,
 A_INLINE_S1   VARCHAR(4),
 A_INLINE_S2   VARCHAR(63),
 A_POOL_S      VARCHAR(65536),
 A_POOL_MAX_S  VARCHAR(1048576),
 A_INLINE_B    VARBINARY(32),
 A_POOL_B      VARBINARY(256),
 A_DECIMAL     DECIMAL,
 A_GEOGRAPHY_POINT GEOGRAPHY_POINT,
 A_GEOGRAPHY   GEOGRAPHY,
 PRIMARY KEY (PKEY)
);

-- Table for super big rows that test max supported storage
CREATE TABLE JUMBO_ROW (
 PKEY          INTEGER      NOT NULL,
 STRING1       VARCHAR(1048576),
 STRING2       VARCHAR(1048564),
 PRIMARY KEY (PKEY)
);
