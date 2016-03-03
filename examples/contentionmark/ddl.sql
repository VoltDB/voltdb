-- These statements make this DDL idempotent (on an empty database)
DROP PROCEDURE Increment IF EXISTS;
DROP PROCEDURE Init IF EXISTS;
DROP TABLE counters IF EXISTS;

-- This table holds a very small number of rows which are updated as often
-- as possible.
CREATE TABLE counters (
  id BIGINT NOT NULL
, value BIGINT NOT NULL
, PRIMARY KEY (id)
);
PARTITION TABLE counters ON COLUMN id;

-- This statement is called repeatedly to increment the value field by a small amount
CREATE PROCEDURE Increment PARTITION ON TABLE counters COLUMN id PARAMETER 1 AS 
	UPDATE counters SET value = value + ? WHERE id = ?;

-- This statement adds a row with zero value for a given id, or leaves the value
-- intact if a row for that id was already present.
CREATE PROCEDURE Init PARTITION ON TABLE counters COLUMN id PARAMETER 0 AS 
	UPSERT INTO counters SELECT CAST (? AS BIGINT), COALESCE(MAX(value),0) 
	FROM counters WHERE id = ?;
