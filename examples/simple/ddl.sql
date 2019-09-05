-- load java stored procedure classes from pre-compiled jar file
LOAD CLASSES procedures/procedures.jar;


-- use this command to execute the following DDL as a batch in a single transaction
file -inlinebatch END_OF_BATCH

-- simple example table
CREATE TABLE app_session (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);
-- partitioning this table will make it fast and scalable
PARTITION TABLE app_session ON COLUMN deviceid;
-- create an index to allow faster access to the table based on a given deviceid
CREATE INDEX app_session_idx ON app_session (deviceid);


-- this view summarizes how many sessions have been inserted for each app / device combination
CREATE VIEW app_usage AS
SELECT appid, deviceid, count(*) as ct
FROM app_session
GROUP BY appid, deviceid;

-- you can declare any SQL statement as a procedure
CREATE PROCEDURE apps_by_unique_devices AS
SELECT appid, COUNT(deviceid) as unique_devices, SUM(ct) as total_sessions
FROM app_usage
GROUP BY appid
ORDER BY unique_devices DESC;

-- another example of making a procedure from a SQL statement, in this case to insert into the table without a ts value, the default to set it to now
CREATE PROCEDURE insert_session PARTITION ON TABLE app_session COLUMN deviceid PARAMETER 1 AS
INSERT INTO app_session (appid, deviceid) VALUES (?,?);

-- create a procedure from a java class
CREATE PROCEDURE PARTITION ON TABLE app_session COLUMN deviceid FROM CLASS simple.SelectDeviceSessions;

-- execute the batch of DDL statements
END_OF_BATCH
