SET DR=ACTIVE;

-- partitioned table
CREATE TABLE xdcr_partitioned
(
  clusterid  bigint             NOT NULL
, txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, key        varbinary(16)      NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_p PRIMARY KEY
  (
    cid, rid
  )
);
PARTITION TABLE xdcr_partitioned ON COLUMN cid;
CREATE INDEX P_CIDINDEX ON xdcr_partitioned (cid);
CREATE ASSUMEUNIQUE INDEX P_KEYINDEX ON xdcr_partitioned (key);

-- replicated table
CREATE TABLE xdcr_replicated
(
  clusterid  bigint             NOT NULL
, txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         bigint             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, key        varbinary(16)      NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_r PRIMARY KEY
  (
    cid, rid
  )
);
CREATE INDEX R_CIDINDEX ON xdcr_replicated (cid);
CREATE UNIQUE INDEX R_KEYINDEX ON xdcr_replicated (key);

-- XDCR conflict report tables
CREATE TABLE xdcr_partitioned_conflict_expected
(
  cid            tinyint            NOT NULL
, rid            bigint             NOT NULL
, clusterid      bigint             NOT NULL
, extrid         bigint             NOT NULL
, action_type    varchar(1)         NOT NULL
, conflict_type  varchar(4)         NOT NULL
, decision       varchar(1)         NOT NULL
, ts             varchar(16)        NOT NULL
, divergence     varchar(1)         NOT NULL
, key            varbinary(16)      NOT NULL
, value          varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_xpe PRIMARY KEY
  (
    cid, rid, clusterid
  )
);
CREATE INDEX XPE_CIDINDEX ON xdcr_partitioned_conflict_expected (cid);
PARTITION TABLE xdcr_partitioned_conflict_expected ON COLUMN cid;

CREATE TABLE xdcr_partitioned_conflict_actual
(
  cid                     tinyint       NOT NULL
, rid                     bigint        NOT NULL
, clusterid               bigint        NOT NULL
, current_clusterid       bigint        NOT NULL
, current_ts              varchar(16)   NOT NULL
, row_type                varchar(3)    NOT NULL
, action_type             varchar(1)    NOT NULL
, conflict_type           varchar(4)    NOT NULL
, conflict_on_primary_key tinyint       NOT NULL
, decision                varchar(1)    NOT NULL
, ts                      varchar(16)   NOT NULL
, divergence              varchar(1)    NOT NULL
, tuple                   varbinary(1048576)
, CONSTRAINT PK_id_xpa PRIMARY KEY
  (
    cid, rid, clusterid, row_type
  )
);
CREATE INDEX XPA_CIDINDEX ON xdcr_partitioned_conflict_actual (cid);
PARTITION TABLE xdcr_partitioned_conflict_actual ON COLUMN cid;

CREATE TABLE xdcr_replicated_conflict_expected
(
  cid            tinyint            NOT NULL
, rid            bigint             NOT NULL
, clusterid      bigint             NOT NULL
, extrid         bigint             NOT NULL
, action_type    varchar(1)         NOT NULL
, conflict_type  varchar(4)         NOT NULL
, decision       varchar(1)         NOT NULL
, ts             varchar(16)        NOT NULL
, divergence     varchar(1)         NOT NULL
, key            varbinary(16)      NOT NULL
, value          varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_xre PRIMARY KEY
  (
    cid, rid, clusterid
  )
);
CREATE INDEX XRE_CIDINDEX ON xdcr_replicated_conflict_expected (cid);
PARTITION TABLE xdcr_replicated_conflict_expected ON COLUMN cid;

CREATE TABLE xdcr_replicated_conflict_actual
(
  cid                     tinyint       NOT NULL
, rid                     bigint        NOT NULL
, clusterid               bigint        NOT NULL
, current_clusterid       bigint        NOT NULL
, current_ts              varchar(16)   NOT NULL
, row_type                varchar(3)    NOT NULL
, action_type             varchar(1)    NOT NULL
, conflict_type           varchar(4)    NOT NULL
, conflict_on_primary_key tinyint       NOT NULL
, decision                varchar(1)    NOT NULL
, ts                      varchar(16)   NOT NULL
, divergence              varchar(1)    NOT NULL
, tuple                   varbinary(1048576)
, CONSTRAINT PK_id_xra PRIMARY KEY
  (
    cid, rid, clusterid, row_type
  )
);
CREATE INDEX XRA_CIDINDEX ON xdcr_replicated_conflict_actual (cid);
PARTITION TABLE xdcr_replicated_conflict_actual ON COLUMN cid;

DR TABLE xdcr_partitioned;
DR TABLE xdcr_replicated;

-- base procedures you shouldn't call
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertBaseProc;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.ReplicatedInsertBaseProc;

-- real procedures
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertPartitionedSP;
PARTITION PROCEDURE InsertPartitionedSP ON TABLE xdcr_partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.UpdatePartitionedSP;
PARTITION PROCEDURE UpdatePartitionedSP ON TABLE xdcr_partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.DeletePartitionedSP;
PARTITION PROCEDURE DeletePartitionedSP ON TABLE xdcr_partitioned COLUMN cid;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertXdcrPartitionedActualSP;
PARTITION PROCEDURE InsertXdcrPartitionedActualSP ON TABLE xdcr_partitioned_conflict_actual COLUMN cid;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertXdcrPartitionedExpectedSP;
PARTITION PROCEDURE InsertXdcrPartitionedExpectedSP ON TABLE xdcr_partitioned_conflict_expected COLUMN cid;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertReplicatedMP;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.UpdateReplicatedMP;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.DeleteReplicatedMP;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertXdcrReplicatedActualSP;
PARTITION PROCEDURE InsertXdcrReplicatedActualSP ON TABLE xdcr_replicated_conflict_actual COLUMN cid;
CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.InsertXdcrReplicatedExpectedSP;
PARTITION PROCEDURE InsertXdcrReplicatedExpectedSP ON TABLE xdcr_replicated_conflict_expected COLUMN cid;

CREATE PROCEDURE FROM CLASS xdcrSelfCheck.procedures.Summarize;