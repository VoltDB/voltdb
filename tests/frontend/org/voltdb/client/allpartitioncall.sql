CREATE TABLE TABLE_INT_PARTITION
(
  id    bigint    NOT NULL,
  value_string	   varchar(50) NOT NULL,
  value_number1    bigint   NOT NULL,
  value_number2    bigint   NOT NULL
);
PARTITION TABLE TABLE_INT_PARTITION ON COLUMN value_number1;

CREATE TABLE TABLE_STRING_PARTITION
(
  id    bigint   NOT NULL,
  value_string    varchar(50) NOT NULL,
  value_number1   bigint   NOT NULL,
  value_number2   bigint   NOT NULL
);
PARTITION TABLE TABLE_STRING_PARTITION ON COLUMN value_string;

CREATE PROCEDURE PARTITION ON TABLE TABLE_INT_PARTITION COLUMN value_number1
FROM CLASS org.voltdb.client.PartitionIntegerTestProc;

CREATE PROCEDURE PARTITION ON TABLE TABLE_STRING_PARTITION COLUMN value_string
FROM CLASS org.voltdb.client.PartitionStringTestProc;

CREATE PROCEDURE PARTITION ON TABLE TABLE_INT_PARTITION COLUMN value_number1 PARAMETER 1
FROM CLASS org.voltdb.client.PartitionedTestProcNonZeroPartitioningParam;

CREATE PROCEDURE FROM CLASS org.voltdb.client.MultiPartitionProcedureSample;

CREATE PROCEDURE PARTITION ON TABLE TABLE_INT_PARTITION COLUMN value_number1
FROM CLASS org.voltdb.client.PartitionFailureTestProc;

CREATE DIRECTED PROCEDURE FROM CLASS org.voltdb.client.PartitionedTestProc;

CREATE PROCEDURE PartitionedSQLTestProc DIRECTED AS SELECT COUNT(*) FROM TABLE_INT_PARTITION;
