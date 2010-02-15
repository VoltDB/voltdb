--
-- The following four tables are used to test insert and select of
-- min, max, 0, NULL and some normal values of each SQL type. These
-- tables are not designed to test any partitioning effects.
--

-- Attribute for ea. supported types: no nulls
CREATE TABLE NO_NULLS (
 PKEY          INTEGER       NOT NULL,
 A_TINYINT     TINYINT       NOT NULL,
 A_SMALLINT    SMALLINT      NOT NULL,
 A_INTEGER     INTEGER       NOT NULL,
 A_BIGINT      BIGINT        NOT NULL,
 A_FLOAT       FLOAT         NOT NULL,
 A_TIMESTAMP   TIMESTAMP     NOT NULL,
 A_INLINE_S1   VARCHAR(4)    NOT NULL,   -- smaller than ptr
 A_INLINE_S2   VARCHAR(10)   NOT NULL,   -- bigger than ptr
 A_POOL_S      VARCHAR(1024) NOT NULL,   -- not inlined
 A_POOL_MAX_S  VARCHAR(32000) NOT NULL,  -- not inlined (max length)
 A_DECIMAL     DECIMAL       NOT NULL,   -- DECIMAL(19,12)
 PRIMARY KEY (PKEY)
);

-- Attribute for ea. supported type: nulls allowed
CREATE TABLE ALLOW_NULLS (
 PKEY          INTEGER       NOT NULL,
 A_TINYINT     TINYINT       ,
 A_SMALLINT    SMALLINT      ,
 A_INTEGER     INTEGER       ,
 A_BIGINT      BIGINT        ,
 A_FLOAT       FLOAT         ,
 A_TIMESTAMP   TIMESTAMP     ,
 A_INLINE_S1   VARCHAR(4)    ,
 A_INLINE_S2   VARCHAR(10)   ,
 A_POOL_S      VARCHAR(1024) ,
 A_POOL_MAX_S  VARCHAR(32000),
 A_DECIMAL     DECIMAL       ,
 PRIMARY KEY (PKEY)
);

-- Attribute for ea. supported type: non-null default
CREATE TABLE WITH_DEFAULTS (
 PKEY          INTEGER        NOT NULL,
 A_TINYINT     TINYINT        DEFAULT '1',
 A_SMALLINT    SMALLINT       DEFAULT '2',
 A_INTEGER     INTEGER        DEFAULT '3',
 A_BIGINT      BIGINT         DEFAULT '4',
 A_FLOAT       FLOAT          DEFAULT '5.1',
 A_TIMESTAMP   TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
 A_INLINE_S1   VARCHAR(4)     DEFAULT 'abcd',
 A_INLINE_S2   VARCHAR(10)    DEFAULT 'abcdefghij',
 A_POOL_S      VARCHAR(1024)  DEFAULT 'abcdefghijklmnopqrstuvwxyz',
 A_POOL_MAX_S  VARCHAR(32000) DEFAULT 'abcdefghijklmnopqrstuvwxyz',
 A_DECIMAL     DECIMAL        DEFAULT '6',
 PRIMARY KEY (PKEY)
);

-- Attribute for ea. supported type: null is default
CREATE TABLE WITH_NULL_DEFAULTS (
 PKEY          INTEGER        NOT NULL,
 A_TINYINT     TINYINT        DEFAULT NULL,
 A_SMALLINT    SMALLINT       DEFAULT NULL,
 A_INTEGER     INTEGER        DEFAULT NULL,
 A_BIGINT      BIGINT         DEFAULT NULL,
 A_FLOAT       FLOAT          DEFAULT NULL,
 A_TIMESTAMP   TIMESTAMP      DEFAULT NULL,
 A_INLINE_S1   VARCHAR(4)     DEFAULT NULL,
 A_INLINE_S2   VARCHAR(10)    DEFAULT NULL,
 A_POOL_S      VARCHAR(1024)  DEFAULT NULL,
 A_POOL_MAX_S  VARCHAR(32000) DEFAULT NULL,
 A_DECIMAL     DECIMAL        DEFAULT NULL,
 PRIMARY KEY (PKEY)
);


--
-- This table is used to test some basic expressions and aggregations
-- against all types when nulls are present. Inserted values here are
-- chosen specifically not to test overflow or underflow.
--
CREATE TABLE EXPRESSIONS_WITH_NULLS (
 PKEY          INTEGER        NOT NULL,
 A_TINYINT     TINYINT        DEFAULT NULL,
 A_SMALLINT    SMALLINT       DEFAULT NULL,
 A_INTEGER     INTEGER        DEFAULT NULL,
 A_BIGINT      BIGINT         DEFAULT NULL,
 A_FLOAT       FLOAT          DEFAULT NULL,
 A_TIMESTAMP   TIMESTAMP      DEFAULT NULL,
 A_INLINE_S1   VARCHAR(4)     DEFAULT NULL,
 A_INLINE_S2   VARCHAR(10)    DEFAULT NULL,
 A_POOL_S      VARCHAR(1024)  DEFAULT NULL,
 A_POOL_MAX_S  VARCHAR(32000) DEFAULT NULL,
 A_DECIMAL     DECIMAL        DEFAULT NULL,
 PRIMARY KEY (PKEY)
);


--
-- As above without any mixed nulls
--
CREATE TABLE EXPRESSIONS_NO_NULLS (
 PKEY          INTEGER      NOT NULL,
 A_TINYINT     TINYINT      ,
 A_SMALLINT    SMALLINT     ,
 A_INTEGER     INTEGER      ,
 A_BIGINT      BIGINT       ,
 A_FLOAT       FLOAT        ,
 A_TIMESTAMP   TIMESTAMP    ,
 A_INLINE_S1   VARCHAR(4)   ,
 A_INLINE_S2   VARCHAR(10)  ,
 A_POOL_S      VARCHAR(1024),
 A_POOL_MAX_S  VARCHAR(32000),
 A_DECIMAL     DECIMAL      ,
 PRIMARY KEY (PKEY)
);





