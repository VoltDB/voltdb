-- This is a set of SELECT statements that tests the bare minimum
-- necessary to start to think that maybe we actually support the
-- subset of SQL that we claim to support.
--
-- In the brave new meta-template world (yeah, okay, kill me), these
-- statements expect that the includer will have set the template
-- @cmp_type to be the _value[???] type that makes sense for the
-- config under test.  So, if the configuration is pushing on string
-- operations, you'll want a line: {@cmp_type = "_value[string]"} in
-- the template file that includes this one.

--SELECT
-- test select expressions
--- test simple projection
SELECT _variable FROM @from_tables
SELECT _variable, _variable FROM @from_tables
--- test column alias
SELECT _variable AS DUDE FROM @from_tables
--- test *
SELECT * FROM @from_tables
--- test simple arithmetic expressions (+, -, *, /) with constant
SELECT _variable _math _value[int:0,100] FROM @from_tables
--- test DISTINCT
SELECT DISTINCT _variable FROM @from_tables
--- test ALL
SELECT ALL _variable FROM @from_tables
--- test aggregate functions (COUNT, SUM, MAX, MIN, AVG)
SELECT _agg(_variable) FROM @from_tables
--- count(*), baby
SELECT COUNT(*) FROM @from_tables
-- test where expressions
--- test comparison operators (<, <=, =, >=, >)
SELECT * FROM @from_tables WHERE _variable _cmp @cmp_type
--- test arithmetic operators (+, -, *, /) with comparison ops
SELECT * FROM @from_tables WHERE (_variable _math _value[int:0,100]) _cmp @cmp_type
--- test logic operators (AND) with comparison ops
SELECT * FROM @from_tables WHERE (_variable _cmp @cmp_type) _logic (_variable _cmp _variable)
-- test GROUP BY
SELECT _variable FROM @from_tables GROUP BY _variable
-- test ORDER BY
SELECT _variable[@order], _variable FROM @from_tables ORDER BY _variable[@order] _sortorder
-- test LIMIT (with ORDER BY)
SELECT _variable[@order] FROM @from_tables ORDER BY _variable[@order] LIMIT _value[int:1,10]
-- test OFFSET (with ORDER BY)
SELECT _variable[@order] FROM @from_tables ORDER BY _variable[@order] LIMIT _value[int:1,10] OFFSET _value[int:1,10]
-- test GROUP BY count(*)
SELECT _variable[@order], COUNT(*) AS FOO FROM @from_tables GROUP BY _variable[@order]
-- test GROUP BY ORDER BY COUNT(*) with LIMIT
SELECT _variable[@order], COUNT(*) AS FOO FROM @from_tables GROUP BY _variable[@order] ORDER BY FOO DESC, _variable[@order] LIMIT _value[int:1,3]
-- test GROUP BY ORDER BY COUNT(*) with OFFSET
SELECT _variable[@order], COUNT(*) AS FOO FROM @from_tables GROUP BY _variable[@order] ORDER BY FOO DESC, _variable[@order] LIMIT _value[int:1,3] OFFSET _value[int:1,3]
-- test INNER JOIN (we'll do more two-table join fun separately, this just checks syntax)
SELECT * FROM _table INNER JOIN _table ON _variable = _variable
