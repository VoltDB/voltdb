CREATE TABLE R1 (
    A INTEGER NOT NULL,
    C INTEGER NOT NULL,
    D INTEGER
);

CREATE TABLE R2 (
    A INTEGER,
    C INTEGER
);

CREATE TABLE R3 (
    A INTEGER NOT NULL,
    C INTEGER
);
CREATE INDEX IND1 ON R3 (A);

CREATE TABLE P1 (
    A INTEGER NOT NULL,
    C INTEGER
);
PARTITION TABLE P1 ON COLUMN A;

CREATE TABLE P2 (
    A INTEGER NOT NULL,
    E INTEGER NOT NULL,
    PRIMARY KEY (A)
);
PARTITION TABLE P2 ON COLUMN A;

CREATE TABLE P3 (
    A INTEGER NOT NULL,
    F INTEGER NOT NULL,
    PRIMARY KEY (A)
);
PARTITION TABLE P3 ON COLUMN A;

CREATE TABLE P4 (
    A INTEGER NOT NULL,
    G INTEGER
);
PARTITION TABLE P4 ON COLUMN A;

-- Purposely index with the non-nullable partition key listed last to
-- ensure that when index-enabled (soon), IS NOT DISTINCT FROM does the
-- right thing with null prefix keys.
CREATE INDEX P4_NULLABLE_AND_PARTITIONKEY ON P4 (G, A);

CREATE TABLE R4 (
    A INTEGER,
    G INTEGER
);

-- Compound index on nullable columns will allow wider test coverage
-- of (coming soon) indexed IS NOT DISTINCT FROM joins.
CREATE INDEX R4_NULLABLES ON R4 (G, A);

CREATE TABLE R5 (
    SI SMALLINT,
    STR varchar(32),
    BI BIGINT
);

CREATE INDEX R5_IND1 ON R5 (SI, BI);

CREATE TABLE R6 (
    A INTEGER,
    STR varchar(32),
    G INTEGER
);

CREATE INDEX R6_IND1 ON R6 (G, A);

CREATE TABLE R7 (
    A INTEGER NOT NULL,
    C INTEGER
);
CREATE INDEX R7_IND1 ON R7 (A);

CREATE TABLE R8 (
    A INTEGER NOT NULL,
    C INTEGER
);
CREATE INDEX R8_IND1 ON R8 (A);

-- ENG-8692
CREATE TABLE t1(i1 INTEGER);
CREATE TABLE t2(i2 INTEGER);
CREATE TABLE t3(i3 INTEGER);
CREATE TABLE t4(i4 INTEGER);
CREATE INDEX t4_idx ON T4 (i4);

CREATE TABLE T1_ENG_13603 (
  ID INTEGER,
  RATIO FLOAT,
  DATA BIGINT
);

CREATE TABLE T2_ENG_13603 (
  ID INTEGER
);

-- ENG-15030
create table rowkeys(dataset_id bigint, part int not null, value int, primary key (dataset_id, part, value));
partition table rowkeys on column part;
create table float_cells(attribute_id bigint, row_key int, value int, part int not null, primary key (attribute_id, part, row_key));
partition table float_cells on column part;

