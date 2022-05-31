LOAD CLASSES txnid.jar;

-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
file -inlinebatch END_OF_BATCH

-- partitioned table
CREATE TABLE partitioned
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         timestamp             NOT NULL
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
, desc         tinyint             NOT NULL
, CONSTRAINT PK_id_d PRIMARY KEY
  (
    cid
  )
, UNIQUE ( cid )
);
CREATE UNIQUE INDEX D_DESCINDEX ON dimension (desc);

CREATE VIEW partview (
    cid,
    entries,
    maximum,
    minimum,
    summation
) AS SELECT
    d.desc,
    COUNT(*),
    MAX(cnt),
    MIN(cnt),
    SUM(cnt)
FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid  GROUP BY d.desc;

-- replicated table
CREATE TABLE replicated
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         timestamp             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_r PRIMARY KEY
  (
    cid, txnid
  )
, UNIQUE ( cid, rid )
);
CREATE INDEX R_CIDINDEX ON replicated (cid);

CREATE VIEW replview (
    cid,
    entries,
    maximum,
    minimum,
    summation
) AS SELECT
    d.desc,
    COUNT(*),
    MAX(cnt),
    MIN(cnt),
    SUM(cnt)
FROM replicated r INNER JOIN dimension d ON r.cid=d.cid GROUP BY d.desc;

-- replicated table
CREATE TABLE adhocr
(
  id         bigint             NOT NULL
, ts         timestamp             NOT NULL
, inc        bigint             NOT NULL
, jmp        bigint             NOT NULL
, CONSTRAINT PK_id_ar PRIMARY KEY (id)
);
CREATE INDEX R_TSINDEX ON adhocr (ts);

-- partitioned table
CREATE TABLE adhocp
(
  id         bigint             NOT NULL
, ts         timestamp             NOT NULL
, inc        bigint             NOT NULL
, jmp        bigint             NOT NULL
, CONSTRAINT PK_id_ap PRIMARY KEY (id)
);
PARTITION TABLE adhocp ON COLUMN id;
CREATE INDEX P_TSINDEX ON adhocp (ts);

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

--  nibble delete replicated table
CREATE TABLE nibdr
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, ts         timestamp          DEFAULT NOW NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_nr PRIMARY KEY (p,id)
) USING TTL 30 Seconds on column ts ;
CREATE INDEX NIBR_TSINDEX ON nibdr (ts);

-- nibble delete partitioned table
CREATE TABLE nibdp
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, ts         timestamp          DEFAULT NOW NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_np PRIMARY KEY (p,id)
) USING TTL 30 Seconds on column ts ;
PARTITION TABLE nibdp ON COLUMN p;
CREATE INDEX NIBP_TSINDEX ON nibdp (ts);

CREATE TABLE forDroppedProcedure
(
  p          integer             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_forDroppedProcedure PRIMARY KEY (p,id)
);
PARTITION TABLE forDroppedProcedure ON COLUMN p;

-- export tables
CREATE STREAM partitioned_export PARTITION ON COLUMN cid export to target partitioned_target
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         timestamp             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
);

CREATE STREAM partitioned_export2 PARTITION ON COLUMN cid
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         timestamp          NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
);

CREATE VIEW ex_partview (
    cid,
    entries,
    maximum,
    minimum,
    summation
) AS SELECT
    cid,
    COUNT(*),
    MAX(cnt),
    MIN(cnt),
    SUM(cnt)
FROM partitioned_export GROUP BY cid;

CREATE VIEW ex_part_mig_view MIGRATE TO TARGET abc (
    cid,
    timespan,
    entries,
    maximum,
    minimum,
    summation
) AS SELECT
    cid,
    TRUNCATE(minute, ts),
    COUNT(*),
    MAX(cnt),
    MIN(cnt),
    SUM(cnt)
FROM partitioned_export2 GROUP BY cid, TRUNCATE(minute, ts)
USING TTL 30 SECONDS ON COLUMN timespan;

CREATE INDEX MATVIEW_PK_INDEX ON EX_PART_MIG_VIEW (TIMESPAN) WHERE NOT MIGRATING;
CREATE INDEX wints on ex_part_mig_view (timespan) WHERE NOT MIGRATING;

CREATE TABLE ex_partview_shadow (
    cid tinyint not null,
    entries int,
    maximum bigint,
    minimum bigint,
    summation bigint,
    primary key(cid)
);
PARTITION TABLE ex_partview_shadow ON COLUMN cid;

CREATE STREAM replicated_export export to target replicated_target
(
  txnid      bigint             NOT NULL
, prevtxnid  bigint             NOT NULL
, ts         timestamp             NOT NULL
, cid        tinyint            NOT NULL
, cidallhash bigint             NOT NULL
, rid        bigint             NOT NULL
, cnt        bigint             NOT NULL
, adhocinc   bigint             NOT NULL
, adhocjmp   bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
);


CREATE TABLE T_PAYMENT50 (
   SEQ_NO varchar(32 BYTES) NOT NULL,
   PID varchar(20 BYTES) NOT NULL,
   UID varchar(12 BYTES),
   CLT_NUM varchar(10 BYTES),
   DD_APDATE timestamp,
   ACCT_NO varchar(32 BYTES) NOT NULL,
   AUTH_TYPE varchar(1 BYTES),
   DEV_TYPE varchar(1 BYTES),
   TRX_CODE varchar(10 BYTES),
   AUTH_ID_TYPE varchar(1 BYTES),
   AUTH_ID varchar(32 BYTES),
   PHY_ID_TYPE varchar(2 BYTES),
   PHY_ID varchar(250 BYTES),
   CLIENT_IP varchar(32 BYTES),
   ACCT_TYPE varchar(1 BYTES),
   ACCT_BBK varchar(4 BYTES),
   TRX_CURRENCY varchar(2 BYTES),
   TRX_AMOUNT decimal,
   MCH_BBK varchar(4 BYTES),
   MCH_NO varchar(10 BYTES),
   BLL_NO varchar(10 BYTES),
   BLL_DATE varchar(8 BYTES),
   EXT_DATA varchar(20 BYTES),
   LBS_DISTANCE decimal,
   SAFE_DISTANCE_FLAG varchar(2 BYTES),
   LBS varchar(64 BYTES),
   LBS_CITY varchar(6 BYTES),
   LBS_COUNTRY varchar(3 BYTES),
   CONSTRAINT IDX_PAYMENT50_PKEY PRIMARY KEY (PID, SEQ_NO)
);
PARTITION TABLE T_PAYMENT50 ON COLUMN PID;
CREATE INDEX IDX_PAYMENT50_ACCT_NO ON T_PAYMENT50 (PID, ACCT_NO);
CREATE INDEX IDX_PAYMENT50_CLT_NUM ON T_PAYMENT50 (PID, CLT_NUM);
CREATE INDEX IDX_PAYMENT50_TIME ON T_PAYMENT50 (DD_APDATE);
CREATE INDEX IDX_PAYMENT50_UID ON T_PAYMENT50 (PID, UID);

-- For loadsinglepartition
CREATE TABLE loadp
(
  cid    BIGINT NOT NULL
, txnid  BIGINT NOT NULL
, rowid  BIGINT NOT NULL
, CONSTRAINT pkey_id_forLoadPartitionSP PRIMARY KEY (cid)
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
, CONSTRAINT pkey_id_forLoadPartitionMP PRIMARY KEY (cid)
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

CREATE TABLE swapr
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_SWAPR PRIMARY KEY (p,id)
);

CREATE TABLE swapp
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_SWAPP PRIMARY KEY (p,id)
);
PARTITION TABLE swapp ON COLUMN p;

-- TODO: these two temp tables (tempr, tempp) will no longer be needed,
-- once SWAP TABLES, as ad hoc DML, is fully supported on master:
CREATE TABLE tempr
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_TEMPR PRIMARY KEY (p,id)
);

CREATE TABLE tempp
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_TEMPP PRIMARY KEY (p,id)
);
PARTITION TABLE tempp ON COLUMN p;

CREATE TABLE capr
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, tmstmp     timestamp            NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_cr PRIMARY KEY (p,id)
);

CREATE TABLE capp
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, tmstmp     timestamp            NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_cp PRIMARY KEY (p,id)
);
PARTITION TABLE capp ON COLUMN p;

-- import table partitioned
CREATE TABLE importp
(
  ts         timestamp             NOT NULL
, cid        tinyint            NOT NULL
, cnt        bigint             NOT NULL
, rc         bigint             NOT NULL
, CONSTRAINT PK_IMPORT_id_p PRIMARY KEY
  (
    cid
  )
, UNIQUE ( cid )
);
PARTITION TABLE importp ON COLUMN cid;
CREATE INDEX P_IMPORTCIDINDEX ON importp (cid);

-- import table replicated
CREATE TABLE importr
(
  ts         timestamp             NOT NULL
, cid        tinyint            NOT NULL
, cnt        bigint             NOT NULL
, rc         bigint             NOT NULL
, CONSTRAINT PK_IMPORT_id_r PRIMARY KEY
  (
    cid
  )
, UNIQUE ( cid )
);
CREATE INDEX R_IMPORTCIDINDEX ON importr (cid);

-- import bitmap table partitioned
CREATE TABLE importbp
(
  cid        tinyint            NOT NULL
, seq        int                NOT NULL
, bitmap     varbinary(1024)    NOT NULL
, CONSTRAINT PK_IMPORT_id_bp PRIMARY KEY
  (
    cid, seq
  )
, UNIQUE ( cid, seq )
);
PARTITION TABLE importbp ON COLUMN cid;

-- import bitmap table replicated
CREATE TABLE importbr
(
  cid        tinyint            NOT NULL
, seq        int                NOT NULL
, bitmap     varbinary(1024)    NOT NULL
, CONSTRAINT PK_IMPORT_id_br PRIMARY KEY
  (
    cid, seq
  )
, UNIQUE ( cid, seq )
);

-- TTL with migrate to stream -- partitioned
CREATE TABLE ttlmigratep MIGRATE TO TARGET ttlmigratep_target
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, ts         timestamp          DEFAULT NOW NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_mp PRIMARY KEY (p,id)
) USING TTL 30 SECONDS ON COLUMN ts;
PARTITION TABLE ttlmigratep ON COLUMN p;
CREATE INDEX ttlmigrateidxp ON ttlmigratep(ts) WHERE NOT MIGRATING;

-- TTL with migrate to stream -- replicated
CREATE TABLE ttlmigrater MIGRATE TO TARGET ttlmigrater_target
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, ts         timestamp          DEFAULT NOW NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_mr PRIMARY KEY (p,id)
) USING TTL 30 SECONDS ON COLUMN ts;
CREATE INDEX ttlmigrateidxr ON ttlmigrater(ts) WHERE NOT MIGRATING;

-- Table for scheduled task -- partitioned
CREATE TABLE taskp
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, ts         timestamp          DEFAULT NOW NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_taskp PRIMARY KEY (p,id)
);
PARTITION TABLE taskp ON COLUMN p;

-- Table for scheduled task -- replicated
CREATE TABLE taskr
(
  p          bigint             NOT NULL
, id         bigint             NOT NULL
, ts         timestamp          DEFAULT NOW NOT NULL
, value      varbinary(1048576) NOT NULL
, CONSTRAINT PK_id_taskr PRIMARY KEY (p,id)
);

-- Custom task checks for streams with pending rows but not enabled
CREATE TASK orphaned_tuples ON SCHEDULE EVERY 1 MINUTES PROCEDURE FROM CLASS txnIdSelfCheck.OrphanedTuples ON ERROR LOG;

-- base procedures you shouldn't call
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateBaseProc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReplicatedUpdateBaseProc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PoisonBaseProc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.CopyLoadPartitionedBase;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteLoadPartitionedBase;

-- real procedures
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.SetupAdHocTables;
CREATE PROCEDURE PARTITION ON TABLE partitioned COLUMN cid FROM CLASS txnIdSelfCheck.procedures.UpdatePartitionedSP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdatePartitionedMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateReplicatedMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateBothMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.UpdateReplicatedMPInProcAdHoc;
CREATE PROCEDURE PARTITION ON TABLE partitioned COLUMN cid FROM CLASS txnIdSelfCheck.procedures.ReadSP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReadMP;
CREATE PROCEDURE PARTITION ON TABLE partitioned COLUMN cid FROM CLASS txnIdSelfCheck.procedures.ReadSPInProcAdHoc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ReadMPInProcAdHoc;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.Summarize;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.Summarize_Replica;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.Summarize_Import;
CREATE PROCEDURE PARTITION ON TABLE bigp COLUMN p FROM CLASS txnIdSelfCheck.procedures.BIGPTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.BIGRTableInsert;
CREATE PROCEDURE PARTITION ON TABLE bigp COLUMN p FROM CLASS txnIdSelfCheck.procedures.GenHashMismatchOnBigP;


CREATE PROCEDURE PARTITION ON TABLE ttlmigratep COLUMN p FROM CLASS txnIdSelfCheck.procedures.TTLMIGRATEPTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TTLMIGRATERTableInsert;

CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TASKRTableInsert;
CREATE PROCEDURE PARTITION ON TABLE taskp COLUMN p FROM CLASS txnIdSelfCheck.procedures.TASKPTableInsert;

CREATE PROCEDURE PARTITION ON TABLE partitioned COLUMN cid FROM CLASS txnIdSelfCheck.procedures.PoisonSP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PoisonMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.PopulateDimension;
CREATE PROCEDURE PARTITION ON TABLE cploadp COLUMN cid FROM CLASS txnIdSelfCheck.procedures.CopyLoadPartitionedSP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.CopyLoadPartitionedMP;
CREATE PROCEDURE PARTITION ON TABLE cploadp COLUMN cid FROM CLASS txnIdSelfCheck.procedures.DeleteLoadPartitionedSP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteLoadPartitionedMP;
CREATE PROCEDURE PARTITION ON TABLE loadp COLUMN cid FROM CLASS txnIdSelfCheck.procedures.DeleteOnlyLoadTableSP;
CREATE PROCEDURE PARTITION ON TABLE T_PAYMENT50 COLUMN pid FROM CLASS txnIdSelfCheck.procedures.DeleteOnlyLoadTableSPW;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.DeleteOnlyLoadTableMP;
CREATE PROCEDURE PARTITION ON TABLE trup COLUMN p FROM CLASS txnIdSelfCheck.procedures.TRUPTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPTruncateTableMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURTruncateTable;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPSwapTablesMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURSwapTables;
CREATE PROCEDURE PARTITION ON TABLE trup COLUMN p FROM CLASS txnIdSelfCheck.procedures.TRUPScanAggTableSP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRUPScanAggTableMP;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.TRURScanAggTable;
CREATE PROCEDURE PARTITION ON TABLE capp COLUMN p FROM CLASS txnIdSelfCheck.procedures.CAPPTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.CAPRTableInsert;
CREATE PROCEDURE PARTITION ON TABLE capp COLUMN p FROM CLASS txnIdSelfCheck.procedures.CAPPCountPartitionRows;
CREATE PROCEDURE PARTITION ON TABLE importp COLUMN cid PARAMETER 3 FROM CLASS txnIdSelfCheck.procedures.ImportInsertP;
-- PARTITION PROCEDURE ImportInsertP ON TABLE importp COLUMN cid PARAMETER 3;
-- same table -- how does this work in combined statement? PARTITION PROCEDURE ImportInsertP ON TABLE importbp COLUMN cid PARAMETER 3;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.ImportInsertR;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.exceptionUDF;
CREATE PROCEDURE PARTITION ON TABLE nibdp COLUMN p FROM CLASS txnIdSelfCheck.procedures.NIBDPTableInsert;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.NIBDRTableInsert;

-- procedures used by the scheduled TASKs defined below
CREATE PROCEDURE deleteSomeP DIRECTED AS DELETE FROM taskp WHERE ts < DATEADD(SECOND, ?, NOW);
CREATE PROCEDURE deleteSomeR AS DELETE FROM taskr WHERE ts < DATEADD(SECOND, ?, NOW);

-- functions
CREATE FUNCTION add2Bigint    FROM METHOD txnIdSelfCheck.procedures.udfs.add2Bigint;
CREATE FUNCTION identityVarbin    FROM METHOD txnIdSelfCheck.procedures.udfs.identityVarbin;
CREATE FUNCTION excUDF    FROM METHOD txnIdSelfCheck.procedures.udfs.badUDF;

END_OF_BATCH
-- tasks
CREATE TASK taskDeleteP ON SCHEDULE DELAY 1 MILLISECONDS PROCEDURE deleteSomeP WITH (-100) ON ERROR LOG RUN ON PARTITIONS;
CREATE TASK taskDeleteR ON SCHEDULE DELAY 1 MILLISECONDS PROCEDURE deleteSomeR WITH (-100) ON ERROR LOG;

