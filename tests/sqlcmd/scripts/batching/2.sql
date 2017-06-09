
CREATE TABLE t2 (p INTEGER NOT NULL, a INTEGER, s FLOAT, PRIMARY KEY(p));

CREATE INDEX idx ON mt ( p );

CREATE VIEW mt2 (a, p, n, ms)
    AS SELECT a, p, COUNT(*), MIN(s)
    FROM t2
    GROUP BY a, p;
