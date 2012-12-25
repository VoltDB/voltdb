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
--SELECT * FROM P1 INNER JOIN R1 ON P1.RATIO = R1.ID
-- test SET Operations
SELECT _variable FROM _table[@lhs] UNION SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] UNION ALL SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] EXCEPT SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] EXCEPT ALL SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] INTERSECT SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] INTERSECT ALL SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] UNION SELECT _variable FROM _table[@mhs] UNION SELECT _variable FROM _table[@rhs]
(SELECT _variable FROM _table[@lhs] EXCEPT SELECT _variable FROM _table[@mhs]) UNION SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] (UNION SELECT _variable FROM _table[@mhs] INTERSECT SELECT _variable FROM _table[@rhs])
