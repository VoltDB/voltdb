-- This is a set of SELECT statements that tests the bare minimum
-- necessary to start to think that maybe we actually support the
-- subset of SQL that we claim to support.
--
-- In the brave new meta-template world (yeah, okay, kill me), these
-- statements expect that the includer will have set the template macros like
-- @comparabletype to have a value that makes sense for the schema under test.
-- So, if the configuration is pushing on string
-- operations, you'll want a line: {@comparabletype = "string"} in
-- the template file that includes this one.
--
-- Required preprocessor macros (with example values):
-- {@aftermath = " _math _value[int:1,3]"}
-- {@agg = "_numagg"}
-- {@columntype = "decimal"}
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@fromtables = "_table"}
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@idcol = "ID"}
-- {@optionalfn = "_numfun"}


--SELECT
-- test select expressions
--- test simple projection
SELECT _variable[#arg @comparabletype] FROM @fromtables A
SELECT _variable[#arg @comparabletype], _variable FROM @fromtables A
--- test column alias
SELECT _variable[#arg @columntype] AS DUDE FROM @fromtables A
--- test *
SELECT * FROM @fromtables A
--- test simple arithmetic expressions (+, -, *, /) with constant
SELECT _variable[#arg @columntype] @aftermath AS LITTLE_MATH FROM @fromtables A

--- test DISTINCT
SELECT DISTINCT _variable[#arg @comparabletype] FROM @fromtables A
--- test ALL
SELECT ALL _variable[#arg @comparabletype] FROM @fromtables A
--- test aggregate functions (COUNT, SUM, MAX, MIN, AVG)
SELECT @agg(_variable[#arg @comparabletype]) FROM @fromtables A

--- count(*), baby
-- TODO: migrate cases like this that are not columntype/comparabletype-specific to their own template/suite
SELECT COUNT(*) FROM @fromtables A

-- test where expressions
--- test comparison operators (<, <=, =, >=, >)
SELECT * FROM @fromtables A WHERE _maybe _variable[#arg @comparabletype] _cmp @comparableconstant
--- test arithmetic operators (+, -, *, /) with comparison ops
SELECT * FROM @fromtables A WHERE (_variable[#arg @comparabletype] @aftermath) _cmp @comparableconstant
--- test logic operators (AND) with comparison ops
SELECT * FROM @fromtables A WHERE (_variable[#arg @comparabletype] _cmp @comparableconstant) _logicop @columnpredicate
-- test GROUP BY
SELECT _variable[#grouped @columntype] FROM @fromtables A GROUP BY __[#grouped]

{_optionallimitoffset |= ""}
{_optionallimitoffset |= "LIMIT _value[int:1,3]"}
{_optionallimitoffset |= "LIMIT _value[int:1,3] OFFSET _value[int:1,3]"}
-- OFFSET without LIMIT not supported -- is that SQL standard?
--{_optionallimitoffset |= "                      OFFSET _value[int:1,3]"}

-- test ORDER BY with optional LIMIT/OFFSET
SELECT _variable[#order], _variable FROM @fromtables A ORDER BY __[#order] _sortorder _optionallimitoffset

-- test GROUP BY count(*)
SELECT _variable[#grouped], COUNT(*) AS FOO FROM @fromtables A GROUP BY __[#grouped]
-- test GROUP BY ORDER BY COUNT(*) with optional LIMIT/OFFSET
SELECT _variable[#grouped], COUNT(*) AS FOO FROM @fromtables A GROUP BY __[#grouped] ORDER BY 2, 1 _optionallimitoffset

-- test INNER JOIN (we'll do more two-table join fun separately, this just checks syntax)
SELECT * FROM @fromtables LHS INNER JOIN @fromtables RHS ON LHS.@idcol = RHS.@idcol
-- TODO: If the intent is to support a schema with multiple id-partitioned tables,
-- this statement is looking for trouble -- might want to migrate it to its own template/suite.
SELECT * FROM @fromtables LHS INNER JOIN @fromtables RHS ON LHS._variable[@columntype] = RHS._variable[@comparabletype]
