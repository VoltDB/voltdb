CREATE TABLE timedata
(
  uuid varchar(36) NOT NULL,
  val integer NOT NULL,
  update_ts timestamp NOT NULL,
  PRIMARY KEY (uuid)
);
CREATE INDEX uptate_ts_index ON timedata (update_ts);
PARTITION TABLE timedata ON COLUMN uuid;

-- stored procedures
CREATE PROCEDURE FROM CLASS windowing.DeleteRows;
PARTITION PROCEDURE DeleteRows ON TABLE timedata COLUMN uuid;
