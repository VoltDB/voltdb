-- BASIC SQL Coverage cases.  These represent more-or-less the
-- simplest possible templates for SQL statements that cover the SQL
-- keywords that we want to support for a version 1 release.
--
-- Required preprocessor template:
-- @insert_vals: the list of types of the table columns for INSERT
-- @cmp_type
-- @assign_col
-- @assign_type

-- DML, generate random data first.
--INSERT
-- test basic INSERT
INSERT INTO _table VALUES (@insert_vals)


SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs]._variable[@col_type] = __[@rhs]._variable
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col AND __[@rhs].@num_col = 2 
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col AND __[@lhs].@num_col = 2
SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] ON __[@lhs].@id_col = __[@rhs].@id_col WHERE __[@lhs]._variable[@col_type] < 45 AND __[@lhs]._variable[@col_type] = __[@rhs]._variable

SELECT * FROM _table[@lhs] @join_type JOIN _table[@rhs] USING(@id_col, @num_column) WHERE __[@rhs].@id_col > 10 AND __[@lhs].@num_column < 30 AND __[@rhs].@id_col = __[@lhs].@num_column
