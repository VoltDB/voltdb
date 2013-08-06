-- DML, generate random data first.
--INSERT
-- test basic INSERT
INSERT INTO _table VALUES (@insertvals)

SELECT * FROM _table LHS11 ,              _table RHS WHERE                                 LHS11._variable[@columntype] = RHS._variable[@columntype]
SELECT * FROM _table LHS12 ,              _table RHS WHERE LHS12.ID = RHS.ID
SELECT * FROM _table LHS13 ,              _table RHS WHERE LHS13.ID = RHS.ID AND     RHS.NUM = 2 
SELECT * FROM _table LHS14 ,              _table RHS WHERE LHS14.ID = RHS.ID AND   LHS14.NUM = 2
SELECT * FROM _table LHS15 ,              _table RHS WHERE LHS15.ID = RHS.ID AND   LHS15._variable[@columntype] < 45 AND LHS15._variable[@columntype] = RHS._variable[@columntype]


SELECT * FROM P1 LEFT  JOIN R1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
SELECT * FROM R1 RIGHT JOIN P1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
SELECT * FROM P1 RIGHT JOIN R1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
SELECT * FROM R1 LEFT  JOIN P1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
