CREATE TABLE R1 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL,
	D INTEGER NOT NULL
);
CREATE TABLE R2 (
	A INTEGER NOT NULL,
<<<<<<< HEAD:tests/frontend/org/voltdb/planner/testsub-queries-ddl.sql
	C INTEGER NOT NULL,
	D INTEGER NOT NULL
=======
	C INTEGER NOT NULL
>>>>>>> master:tests/frontend/org/voltdb/planner/testplans-subqueries-ddl.sql
);

CREATE TABLE R3 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL
);

CREATE TABLE P1 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL,
	D INTEGER NOT NULL
	,CONSTRAINT P1_PK_TREE PRIMARY KEY(A)
);
<<<<<<< HEAD:tests/frontend/org/voltdb/planner/testsub-queries-ddl.sql
CREATE TABLE R3 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL
);
=======
PARTITION TABLE P1 ON COLUMN A;
>>>>>>> master:tests/frontend/org/voltdb/planner/testplans-subqueries-ddl.sql

CREATE TABLE P2 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL,
	D INTEGER NOT NULL
	,CONSTRAINT P2_PK_TREE PRIMARY KEY(A)
);
PARTITION TABLE P2 ON COLUMN A;
