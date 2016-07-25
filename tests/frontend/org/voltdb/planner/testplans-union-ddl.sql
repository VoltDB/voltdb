CREATE TABLE T1 (
	A INTEGER NOT NULL,
	DESC VARCHAR(300) NOT NULL,
	F FLOAT
);
PARTITION TABLE T1 ON COLUMN A;

CREATE TABLE T2 (
	B INTEGER NOT NULL,
	DESC VARCHAR(300)
);

CREATE TABLE T3 (
	C INTEGER NOT NULL
);

CREATE TABLE T4 (
	D INTEGER NOT NULL
);
PARTITION TABLE T4 ON COLUMN D;

CREATE TABLE T5 (
	E INTEGER NOT NULL,
    TEXT VARCHAR(5) NOT NULL
);
PARTITION TABLE T5 ON COLUMN E;

CREATE TABLE T6 (
	F INTEGER NOT NULL,
	G FLOAT
);

-- Tables T7 And T8 have identical schema
-- except T7 has a primary key index.
CREATE TABLE T7 (
       A INTEGER,
       B INTEGER,
       C INTEGER,
       PRIMARY KEY (A, B)
);

CREATE TABLE T8 (
       A INTEGER,
       B INTEGER,
       C INTEGER
);
