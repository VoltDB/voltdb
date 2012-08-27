
-- partitioned in test on pkey
CREATE TABLE t1 (
 PKEY          INTEGER NOT NULL,
 A_INT         INTEGER,
 A_STR  VARCHAR(10),
 PRIMARY KEY (PKEY)
);

-- replicated in test
CREATE TABLE t2 (
 PKEY          INTEGER NOT NULL,
 A_INT         INTEGER,
 A_STR  VARCHAR(10),
 PRIMARY KEY (PKEY)
);

CREATE TABLE t3 (
 PKEY INTEGER NOT NULL,
 I3  INTEGER NOT NULL,
 I4  INTEGER NOT NULL,
 PRIMARY KEY (PKEY)
 );

 CREATE INDEX t3_TREE ON t3 (I3 DESC);

