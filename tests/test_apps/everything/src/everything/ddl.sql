CREATE TABLE main (
    pval TINYINT NOT NULL,
    ival BIGINT NOT NULL,
    rowtime TIMESTAMP,
    text1 VARCHAR(256) DEFAULT '' NOT NULL,
    CONSTRAINT MAIN_PK_TREE PRIMARY KEY (pval, ival)
);

CREATE INDEX TREE_MAIN_TS ON main (rowtime);
CREATE INDEX TREE_MAIN_IVAL ON main (ival);

CREATE VIEW view1(pval, total)
    AS SELECT pval, COUNT(*)
    FROM main
    GROUP BY pval;
