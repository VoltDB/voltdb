-- This is the import table into which a single value will be pushed by socketimporter.
CREATE TABLE importTable (
key BIGINT NOT NULL
, value BIGINT NOT NULL
, CONSTRAINT PK_importTable PRIMARY KEY
  (
    key
  )
);
-- Partition on id
PARTITION table importTable ON COLUMN key;
