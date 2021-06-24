-- Table that stores calls that are missing either a
-- begin or end timestamp.
CREATE TABLE opencalls
(
  agent_id INTEGER NOT NULL,
  phone_no VARCHAR(20 BYTES) NOT NULL,
  call_id BIGINT NOT NULL,
  start_ts TIMESTAMP DEFAULT NULL,
  end_ts TIMESTAMP DEFAULT NULL,
  PRIMARY KEY (call_id, agent_id, phone_no)
);
-- Partition this table to get parallelism.
PARTITION TABLE opencalls ON COLUMN agent_id;

-- Stores completed calls for as long as memory holds
-- In a real app, this data would likely be exported to
-- a historical store.
CREATE TABLE completedcalls
(
  agent_id INTEGER NOT NULL,
  phone_no VARCHAR(20 BYTES) NOT NULL,
  call_id BIGINT NOT NULL,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  duration INTEGER NOT NULL,
  PRIMARY KEY (call_id, agent_id, phone_no)
);
PARTITION TABLE completedcalls ON COLUMN agent_id;

-- Ordered index on timestamp value allows for quickly finding timestamp
-- values as well as quickly finding rows by offset.
CREATE INDEX end_ts_index ON completedcalls (end_ts);

CREATE TABLE stddevbyagent
(
  agent_id INTEGER NOT NULL,
  curdate TIMESTAMP NOT NULL,
  n BIGINT NOT NULL,
  sumk BIGINT NOT NULL,
  qk FLOAT NOT NULL,
  stddev FLOAT NOT NULL,
  PRIMARY KEY (curdate, agent_id)
);
PARTITION TABLE stddevbyagent ON COLUMN agent_id;

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES callcenter-procs.jar;

-- stored procedures
CREATE PROCEDURE PARTITION ON TABLE opencalls COLUMN agent_id FROM CLASS callcenter.BeginCall;
CREATE PROCEDURE PARTITION ON TABLE opencalls COLUMN agent_id FROM CLASS callcenter.EndCall;

CREATE PROCEDURE TopStdDev
AS
  SELECT agent_id, stddev, (sumk / n) AS average
  FROM stddevbyagent
  WHERE curdate = TRUNCATE(DAY, NOW)
  ORDER BY stddev desc
  LIMIT ?;
