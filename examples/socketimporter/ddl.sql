-- This is the import table into which a single value will be pushed by socketimporter.
CREATE TABLE importTable (
id BIGINT NOT NULL
, CONSTRAINT PK_importTable PRIMARY KEY
  (
    id
  )
);
-- Partition on id
PARTITION table importTable ON COLUMN id;
