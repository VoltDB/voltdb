-- partitioned table
CREATE TABLE partitioned
(
  txnid      bigint             NOT NULL ASSUMEUNIQUE
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_p PRIMARY KEY
  (
    cid, txnid
  )
, UNIQUE ( cid, rid )
);
PARTITION TABLE partitioned ON COLUMN cid;
CREATE INDEX P_CIDINDEX ON partitioned (cid);

-- dimension table
CREATE TABLE dimension
(
  cid        tinyint            NOT NULL
, desc	     tinyint     		NOT NULL
, CONSTRAINT PK_id_d PRIMARY KEY
  (
    cid
  )
, UNIQUE ( cid )
);
CREATE UNIQUE INDEX D_DESCINDEX ON dimension (desc);

-- replicated table
CREATE TABLE replicated
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_r PRIMARY KEY
  (
    txnid
  )
, UNIQUE ( cid, rid )
);
CREATE INDEX R_CIDINDEX ON replicated (cid);

-- replicated table
CREATE TABLE adhocr
(
  id         bigint             NOT NULL
, ts         bigint             NOT NULL
, inc        bigint             NOT NULL
, jmp        bigint             NOT NULL
, CONSTRAINT PK_id_ar PRIMARY KEY (id)
);
CREATE INDEX R_TSINDEX ON adhocr (ts DESC);

-- partitioned table
CREATE TABLE adhocp
(
  id         bigint             NOT NULL
, ts         bigint             NOT NULL
, inc        bigint             NOT NULL
, jmp        bigint             NOT NULL
, CONSTRAINT PK_id_ap PRIMARY KEY (id)
);
PARTITION TABLE adhocp ON COLUMN id;
CREATE INDEX P_TSINDEX ON adhocp (ts DESC);

-- replicated table
CREATE TABLE bigr
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_br PRIMARY KEY (p,id)
);

-- partitioned table
CREATE TABLE bigp
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_bp PRIMARY KEY (p,id)
);
PARTITION TABLE bigp ON COLUMN p;

CREATE TABLE forDroppedProcedure
(
  p          integer             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_forDroppedProcedure PRIMARY KEY (p,id)
);
PARTITION TABLE forDroppedProcedure ON COLUMN p;

-- export tables
CREATE TABLE partitioned_export
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
);
PARTITION TABLE partitioned_export ON COLUMN cid;
EXPORT TABLE partitioned_export;

CREATE TABLE replicated_export
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
);
EXPORT TABLE replicated_export;

-- For loadsinglepartition
CREATE TABLE loadp
(
  cid    BIGINT NOT NULL
, txnid  BIGINT NOT NULL
, rowid  BIGINT NOT NULL
);
PARTITION TABLE loadp ON COLUMN cid;
CREATE TABLE cploadp
(
  cid    BIGINT NOT NULL
, txnid  BIGINT NOT NULL
, rowid  BIGINT NOT NULL
);
PARTITION TABLE cploadp ON COLUMN cid;


-- For loadmultiplepartition
CREATE TABLE loadmp
(
  cid    BIGINT NOT NULL
, txnid  BIGINT NOT NULL
, rowid  BIGINT NOT NULL
);
CREATE TABLE cploadmp
(
  cid    BIGINT NOT NULL
, txnid  BIGINT NOT NULL
, rowid  BIGINT NOT NULL
);

CREATE TABLE trur
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_tr PRIMARY KEY (p,id)
);

CREATE TABLE trup
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_tp PRIMARY KEY (p,id)
);
PARTITION TABLE trup ON COLUMN p;


-- base procedures you shouldn't call
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateBaseProc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReplicatedUpdateBaseProc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PoisonBaseProc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.CopyLoadPartitionedBase;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteLoadPartitionedBase;

-- real procedures
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.SetupAdHocTables;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdatePartitionedSP;
PARTITION PROCEDURE UpdatePartitionedSP ON TABLE partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdatePartitionedMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateReplicatedMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateBothMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateReplicatedMPInProcAdHoc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReadSP;
PARTITION PROCEDURE ReadSP ON TABLE partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReadMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReadSPInProcAdHoc;
PARTITION PROCEDURE ReadSPInProcAdHoc ON TABLE partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReadMPInProcAdHoc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.Summarize;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.BIGPTableInsert;
PARTITION PROCEDURE BIGPTableInsert ON TABLE bigp COLUMN p;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.BIGRTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PoisonSP;
PARTITION PROCEDURE PoisonSP ON TABLE partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PoisonMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PopulateDimension;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.CopyLoadPartitionedSP;
PARTITION PROCEDURE CopyLoadPartitionedSP ON TABLE cploadp COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.CopyLoadPartitionedMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteLoadPartitionedSP;
PARTITION PROCEDURE DeleteLoadPartitionedSP ON TABLE cploadp COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteLoadPartitionedMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteOnlyLoadTableSP;
PARTITION PROCEDURE DeleteOnlyLoadTableSP ON TABLE loadp COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteOnlyLoadTableMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPTableInsert;
PARTITION PROCEDURE TRUPTableInsert ON TABLE bigp COLUMN p;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPTruncateTableSP;
PARTITION PROCEDURE TRUPTruncateTableSP ON TABLE trup COLUMN p;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPTruncateTableMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURTruncateTable;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPScanAggTableSP;
PARTITION PROCEDURE TRUPScanAggTableSP ON TABLE trup COLUMN p;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPScanAggTableMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURScanAggTable;
