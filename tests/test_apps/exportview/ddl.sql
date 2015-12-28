load classes exportview.jar;

file -inlinebatch END_OF_BATCH

CREATE TABLE EXP (
 cid                     BIGINT        NOT NULL
, value                       BIGINT        NOT NULL
);
PARTITION TABLE EXP  ON COLUMN cid;

CREATE TABLE SHADOW (
 cid                     BIGINT        NOT NULL
, value                       BIGINT        NOT NULL
);
PARTITION TABLE SHADOW  ON COLUMN cid;

EXPORT TABLE EXP;

CREATE VIEW EXPVIEW (
    cid,
    entries,
    maximum,
    minimum,
    summ
) AS SELECT
    cid,
    count(*),
    max(value),
    min(value),
    sum(value)
FROM EXP group by cid;


CREATE VIEW SHADOWVIEW (
    cid,
    entries,
    maximum,
    minimum,
    summ
) AS SELECT
    cid,
    count(*),
    max(value),
    min(value),
    sum(value)
FROM SHADOW group by cid;


CREATE PROCEDURE FROM CLASS exportbenchmark.procedures.InsertExport;

END_OF_BATCH

