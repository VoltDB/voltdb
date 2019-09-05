file -inlinebatch END_OF_BATCH

CREATE TABLE counters_ptn
(
  id int NOT NULL,
  counter bigint NOT NULL,
  PRIMARY KEY(ID)
);
PARTITION TABLE counters_ptn ON COLUMN id;

CREATE TABLE counters_rep
(
  id int NOT NULL,
  counter bigint NOT NULL,
  PRIMARY KEY(ID)
);

-- this table used to test @LoadTable procedures
-- it has the same shape as counters and the same
-- distribution but no primary key
CREATE TABLE like_counters_ptn
(
  id int NOT NULL,
  counter bigint NOT NULL
);
PARTITION TABLE like_counters_ptn ON COLUMN id;

CREATE TABLE like_counters_rep
(
  id int NOT NULL,
  counter bigint NOT NULL
);

CREATE TABLE joiner
(
    id int NOT NULL,
    PRIMARY KEY(ID)
);
PARTITION TABLE joiner ON COLUMN id;

END_OF_BATCH

LOAD CLASSES liverejoinconsistency-procs.jar;

-- The following CREATE PROCEDURE statements can all be batched.
file -inlinebatch END_OF_2ND_BATCH

CREATE PROCEDURE FROM CLASS liverejoinconsistency.procedures.Initialize;
PARAMETER 0
CREATE PROCEDURE PARTITION ON TABLE JOINER COLUMN ID FROM CLASS liverejoinconsistency.procedures.getCountFromPtn;
CREATE PROCEDURE PARTITION ON TABLE JOINER COLUMN ID FROM CLASS liverejoinconsistency.procedures.getCountFromRep;
CREATE PROCEDURE PARTITION ON TABLE JOINER COLUMN ID FROM CLASS liverejoinconsistency.procedures.getRowFromPtn;
CREATE PROCEDURE PARTITION ON TABLE JOINER COLUMN ID FROM CLASS liverejoinconsistency.procedures.getRowFromRep;
CREATE PROCEDURE PARTITION ON TABLE JOINER COLUMN ID FROM CLASS liverejoinconsistency.procedures.getCRCFromPtn;
CREATE PROCEDURE PARTITION ON TABLE JOINER COLUMN ID FROM CLASS liverejoinconsistency.procedures.getCRCFromRep;
CREATE PROCEDURE PARTITION ON TABLE COUNTERS_PTN COLUMN ID FROM CLASS liverejoinconsistency.procedures.getNextFromPtn;
CREATE PROCEDURE FROM CLASS liverejoinconsistency.procedures.MPUpdatePtn;
CREATE PROCEDURE FROM CLASS liverejoinconsistency.procedures.MPUpdateRep;

END_OF_2ND_BATCH
