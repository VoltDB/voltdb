CREATE TABLE R7 (
  VCHAR_INLINE     VARCHAR(42 BYTES)    UNIQUE
  ,VCHAR_INLINE_MAX VARCHAR(15 BYTES)         UNIQUE
  ,VCHAR            VARCHAR(16 BYTES)         UNIQUE
  ,VCHAR_JSON       VARCHAR(4000 BYTES) UNIQUE
);

CREATE TABLE P4 (
  VCHAR_INLINE     VARCHAR(14)       DEFAULT '0',
  VCHAR_INLINE_MAX VARCHAR(63 BYTES) DEFAULT '0',
  VCHAR            VARCHAR(64 BYTES) DEFAULT '0' NOT NULL,
  VCHAR_JSON       VARCHAR(1000)     DEFAULT '0',
);
PARTITION TABLE P4 ON COLUMN VCHAR;

CREATE TABLE DML (
  MODIFIED_TUPLES  INTEGER
);
