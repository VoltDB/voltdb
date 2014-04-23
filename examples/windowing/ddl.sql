CREATE TABLE timedata
(
  uuid VARCHAR(36) NOT NULL,
  val BIGINT NOT NULL,
  update_ts TIMESTAMP NOT NULL,
  PRIMARY KEY (uuid)
);
CREATE INDEX uptate_ts_index ON timedata (update_ts);
CREATE INDEX val_index ON timedata (val);
PARTITION TABLE timedata ON COLUMN uuid;

CREATE VIEW agg_by_second
(
  second_ts,
  count_values,
  sum_values,
  max_value
)
AS SELECT TRUNCATE(SECOND, update_ts), COUNT(*), SUM(val), MAX(val)
   FROM timedata
   GROUP BY TRUNCATE(SECOND, update_ts);

-- stored procedures
CREATE PROCEDURE FROM CLASS windowing.DeleteAfterDate;
PARTITION PROCEDURE DeleteAfterDate ON TABLE timedata COLUMN uuid;
