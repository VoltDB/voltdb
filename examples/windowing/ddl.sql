-- Table that stores values that are timestamped and
-- are partitioned by UUID.
CREATE TABLE timedata
(
  uuid VARCHAR(36) NOT NULL,
  val BIGINT NOT NULL,
  update_ts TIMESTAMP NOT NULL,
  CONSTRAINT PK_timedate PRIMARY KEY (uuid, update_ts)
);

-- Partition this table to get parallelism.
PARTITION TABLE timedata ON COLUMN uuid;

-- Ordered index on timestamp value allows for quickly finding timestamp
-- values as well as quickly finding rows by offset.
-- Used by all 4 of the deleting stored procedures.
CREATE INDEX uptate_ts_index ON timedata (update_ts);

-- Ordered index on value field allows for fast maximum value retrieval.
-- Used in procedure windowing.MaxValue below.
CREATE INDEX val_index ON timedata (val);

-- Pre-aggregate the sum and counts of the rows by second.
-- This allows for fast computation of averages by ranges of seconds.
-- See the windowing.Average procedure SQL below for more.
CREATE VIEW agg_by_second
(
  second_ts,
  count_values,
  sum_values
)
AS SELECT TRUNCATE(SECOND, update_ts), COUNT(*), SUM(val)
   FROM timedata
   GROUP BY TRUNCATE(SECOND, update_ts);

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES windowing-procs.jar;

-- stored procedures
CREATE PROCEDURE PARTITION ON TABLE timedata COLUMN uuid FROM CLASS windowing.DeleteAfterDate;
CREATE PROCEDURE PARTITION ON TABLE timedata COLUMN uuid FROM CLASS windowing.DeleteOldestToTarget;
CREATE PROCEDURE PARTITION ON TABLE timedata COLUMN uuid FROM CLASS windowing.InsertAndDeleteAfterDate;
CREATE PROCEDURE PARTITION ON TABLE timedata COLUMN uuid FROM CLASS windowing.InsertAndDeleteOldestToTarget;

-- Find the average value over all tuples across all partitions for the last
-- N seconds, where N is a parameter the user supplies.
--
-- Uses the materialized view so it has to scan fewer tuples. For example,
-- If tuples are being inserted at a rate of 15k/sec and there are 4 partitions,
-- then to compute the average for the last 10s, VoltDB would need to scan
-- 150k rows. In this case, it needs to scan 1 row per second times the number of
-- partitions, or 40 rows. That's a tremendous advantage of pre-aggregating the
-- table sums and counts by second.
CREATE PROCEDURE Average AS
    SELECT SUM(sum_values) / SUM(count_values)
    FROM agg_by_second
    WHERE second_ts >= DATEADD(SECOND, CAST(? as INTEGER), NOW);

-- Find the maximum value across all rows and partitions.
CREATE PROCEDURE MaxValue AS
    SELECT MAX(val)
    FROM timedata;
