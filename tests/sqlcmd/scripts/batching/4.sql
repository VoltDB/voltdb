
CREATE TABLE t4 (i INTEGER, p INTEGER, PRIMARY KEY(i));

CREATE VIEW mt4 (p, n) AS SELECT p, COUNT(*) FROM t4 GROUP BY p;
