-- Table that stores values that are timestamped and
-- are partitioned by UUID.
--
-- Limit table to 82,500 rows per partition.
-- If an insert wil exceed the limit, then
-- execute a delete statement that will purge
-- the oldest 1500 rows that are older than 30s.
--
-- With insert rate of 20k/s and 8 partitions,
-- this will store a bit more about 33s worth of rows.
-- When the table is full, the row limit trigger will
-- attempt to delete the oldest 1500 rows of data.
--
-- Create a unique constraint (implemented as an index)
-- that lets us evaluate the DELETE's ORDER BY and WHERE
-- clause efficiently.
CREATE TABLE timedata
(
  uuid VARCHAR(36) NOT NULL,
  val BIGINT NOT NULL,
  update_ts TIMESTAMP NOT NULL,
  CONSTRAINT update_ts_uuid UNIQUE (update_ts, uuid),
  CONSTRAINT row_limit LIMIT PARTITION ROWS 82500
    EXECUTE (DELETE FROM timedata
             WHERE update_ts
                   < TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - 30)
             ORDER BY update_ts, uuid LIMIT 1500)
);

-- Partition this table to get parallelism.
PARTITION TABLE timedata ON COLUMN uuid;

-- Ordered index on value field allows for fast maximum value retrieval.
-- Used in procedure windowing.MaxValue below.
CREATE INDEX val_index ON timedata (val);

-- Pre-aggregate the sum and counts of the rows by second.
-- This allows for fast computation of averages by ranges of seconds.
-- See the ddlwindowing.Average procedure SQL below for more.
CREATE VIEW agg_by_second
(
  second_ts,
  count_values,
  sum_values
)
AS SELECT TRUNCATE(SECOND, update_ts), COUNT(*), SUM(val)
   FROM timedata
   GROUP BY TRUNCATE(SECOND, update_ts);

-- Find the average value over all tuples across all partitions for
-- the last N seconds, where N is a parameter the user supplies.
--
-- Uses the materialized view so it has to scan fewer tuples. For
-- example, if tuples are being inserted at a rate of 20k/sec and
-- there are 8 partitions, then each partition will have 2,500 rows
-- for one second's worth of data.  To compute the average for the
-- last 10s, each partition would need to scan 25,000 rows. In this
-- case, it needs to scan 1 row per second times the number of
-- partitions, or 80 rows. That's a tremendous advantage of
-- pre-aggregating the table sums and counts by second.
CREATE PROCEDURE Average AS
    SELECT SUM(sum_values) / SUM(count_values)
    FROM agg_by_second
    WHERE second_ts >= TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?);

-- Find the maximum value across all rows and partitions.
CREATE PROCEDURE MaxValue AS
    SELECT MAX(val)
    FROM timedata;

-- Find the age of the oldest row in a partition
-- in milliseconds.
--
-- The WHERE predicate here always returns true,
-- Since column uuid is NOT NULL.
-- It's just here to provide a place for the parameter
-- used to partition the procedure, so it can be called
-- in the "run everywhere" pattern.
CREATE PROCEDURE AgeOfOldest
  PARTITION ON TABLE timedata COLUMN uuid
AS
  SELECT SINCE_EPOCH(MILLISECOND, NOW)
         - SINCE_EPOCH(MILLISECOND, update_ts) AS age_ms
  FROM timedata
  WHERE uuid IS NOT NULL OR uuid = ?
  ORDER BY update_ts LIMIT 1;

-- A procedure that finds the age of the youngest tuple
-- in a partition, similar to the procedure above.
CREATE PROCEDURE AgeOfYoungest
  PARTITION ON TABLE timedata COLUMN uuid
AS
  SELECT SINCE_EPOCH(MILLISECOND, NOW)
         - SINCE_EPOCH(MILLISECOND, update_ts) AS age_ms
  FROM timedata
  WHERE uuid IS NOT NULL OR uuid = ?
  ORDER BY update_ts DESC LIMIT 1;
