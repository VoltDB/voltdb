CREATE TABLE timedata
(
  uuid VARCHAR(36) NOT NULL,
  val BIGINT NOT NULL,
  update_ts TIMESTAMP NOT NULL,
  PRIMARY KEY (uuid)
);
PARTITION TABLE timedata ON COLUMN uuid;

CREATE INDEX uptate_ts_index ON timedata (update_ts);
CREATE INDEX val_index ON timedata (val);

CREATE VIEW agg_by_second
(
  second_ts,
  count_values,
  sum_values
)
AS SELECT TRUNCATE(SECOND, update_ts), COUNT(*), SUM(val)
   FROM timedata
   GROUP BY TRUNCATE(SECOND, update_ts);

-- stored procedures
CREATE PROCEDURE FROM CLASS windowing.DeleteAfterDate;
PARTITION PROCEDURE DeleteAfterDate ON TABLE timedata COLUMN uuid;

CREATE PROCEDURE FROM CLASS windowing.DeleteOldestToTarget;
PARTITION PROCEDURE DeleteOldestToTarget ON TABLE timedata COLUMN uuid;

CREATE PROCEDURE FROM CLASS windowing.InsertAndDeleteAfterDate;
PARTITION PROCEDURE InsertAndDeleteAfterDate ON TABLE timedata COLUMN uuid;

CREATE PROCEDURE FROM CLASS windowing.InsertAndDeleteOldestToTarget;
PARTITION PROCEDURE InsertAndDeleteOldestToTarget ON TABLE timedata COLUMN uuid;

CREATE PROCEDURE windowing.Average AS
    SELECT SUM(sum_values) / SUM(count_values)
    FROM agg_by_second
    WHERE second_ts >= TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?);

CREATE PROCEDURE windowing.MaxValue AS
    SELECT MAX(val)
    FROM timedata;
