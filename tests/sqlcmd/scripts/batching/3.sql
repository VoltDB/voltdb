CREATE TABLE t2 (p INTEGER NOT NULL, i INTEGER ASSUMEUNIQUE, a INTEGER, s FLOAT, PRIMARY KEY(i, p));

CREATE PROCEDURE testp AS SELECT a, s FROM t ORDER BY s DESC LIMIT 1;