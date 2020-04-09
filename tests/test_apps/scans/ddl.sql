CREATE TABLE narrow_p
(
  p integer not null,
  val bigint NOT NULL
);
PARTITION TABLE narrow_p ON COLUMN p;

CREATE TABLE narrow_index_p
(
  p integer not null,
  val bigint NOT NULL ASSUMEUNIQUE
);
PARTITION TABLE narrow_index_p ON COLUMN p;

-- make sure to load the Java code for the procedures, before creating them
LOAD CLASSES scans.jar;

-- stored procedures
CREATE PROCEDURE FROM CLASS scans.procedures.MinSeqScan;
CREATE PROCEDURE FROM CLASS scans.procedures.MinIndexScan;
