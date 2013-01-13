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
INSERT INTO _table VALUES (1000, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1001, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1010, 'desc_1010', 1010, 1010.5)
INSERT INTO _table VALUES (1011, 'desc_1010', 1010, 1010.5)
INSERT INTO P1 VALUES (1020, 'desc_1020', 1020, 1020.5)
INSERT INTO R1 VALUES (1020, 'desc_1020', 1020, 1020.5)

-- test SET Operations
SELECT _variable FROM _table[@lhs] @set_op @optional_all SELECT _variable FROM _table[@rhs]
SELECT _variable FROM _table[@lhs] @set_op SELECT _variable FROM _table[@mhs] @set_op SELECT _variable FROM _table[@rhs]
(SELECT _variable FROM _table[@lhs] @set_op  SELECT _variable FROM _table[@mhs]) @set_op SELECT _variable FROM _table[@rhs]
SELECT  _variable FROM _table[@lhs] @set_op (SELECT _variable FROM _table[@mhs]  @set_op SELECT _variable FROM _table[@rhs])
