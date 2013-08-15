-- DML, generate random data first.
--INSERT
-- test basic INSERT
INSERT INTO _table VALUES (@insertvals)

SELECT * FROM _table LHS11 ,              _table RHS WHERE                                 LHS11._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM _table LHS12 ,              _table RHS WHERE LHS12.@id_col = RHS.@id_col
SELECT * FROM _table LHS13 ,              _table RHS WHERE LHS13.@id_col = RHS.@id_col AND     RHS.@num_col = 2 
SELECT * FROM _table LHS14 ,              _table RHS WHERE LHS14.@id_col = RHS.@id_col AND   LHS14.@num_col = 2
SELECT * FROM _table LHS15 ,              _table RHS WHERE LHS15.@id_col = RHS.@id_col AND   LHS15._variable[@col_type] < 45 AND LHS15._variable[@col_type] = RHS._variable

-- Still triggers wrong answer from mis-partitioning.
-- SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col AND __[@rhs].@id_col = 2
-- SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col AND __[@lhs].@id_col = 2

SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs]._variable[@col_type] = __[@rhs]._variable
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col AND __[@rhs].@num_col = 2
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col AND __[@lhs].@num_col = 2
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col WHERE __[@lhs]._variable[@col_type] < 45 AND __[@lhs]._variable[@col_type] = __[@rhs]._variable


--TODO: ENG-4929 Investigate why these get a planner NullPointerException for a timeout.
-- SELECT *                 FROM _table[@lhs] @join_type JOIN _table[@rhs] USING(@id_col, @num_col) WHERE @id_col > 10 AND @num_col < 30 AND @id_col = @num_col
-- it's not (just) the select * that fails:
-- SELECT @id_col, @num_col FROM _table[@lhs] @join_type JOIN _table[@rhs] USING(@id_col, @num_col) WHERE @id_col > 10 AND @num_col < 30 AND @id_col = @num_col

SELECT * FROM P1 LEFT  JOIN R1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
SELECT * FROM R1 RIGHT JOIN P1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
SELECT * FROM P1 RIGHT JOIN R1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM
SELECT * FROM R1 LEFT  JOIN P1 USING(ID, NUM) WHERE ID > 10 AND NUM < 30 AND ID = NUM

