
CREATE INDEX idx ON mt ( p );

CREATE VIEW mt2 (a, p, n, ms)
    AS SELECT a, p, COUNT(*), MIN(s)
    FROM t
    GROUP BY a, p;
