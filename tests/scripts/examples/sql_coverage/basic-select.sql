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
SELECT _variable[#arg @comparabletype] B1 FROM @fromtables
SELECT _variable[#arg @comparabletype], _variable B2 FROM @fromtables
--- test column alias
SELECT _variable[#arg @columntype] AS B3 FROM @fromtables A
--- test *
SELECT * FROM @fromtables B4
--- test simple arithmetic expressions (+, -, *, /) with constant
SELECT _variable[#arg @columntype] @aftermath AS B5LITTLEMATH FROM @fromtables A

--- test DISTINCT
SELECT DISTINCT _variable[#arg @comparabletype] B6 FROM @fromtables A
SELECT DISTINCT _variable[#arg @comparabletype] B6 FROM @fromtables A ORDER BY 1 LIMIT 5
SELECT DISTINCT _variable[#arg @comparabletype] B6_1, _variable B6_2  FROM @fromtables A
SELECT DISTINCT _variable[#arg @comparabletype] B6_1, _variable B6_2  FROM @fromtables A ORDER BY 1, 2 LIMIT 5

--- test ALL
SELECT ALL _variable[#arg @comparabletype] B7 FROM @fromtables A

--- test aggregate functions (COUNT, SUM, MAX, MIN, AVG)
---- TODO: Re-simplify when the ENG-6176 optimized MIN(NULL) wrong answer bug is fixed.
---- SELECT  @agg(_variable[#arg @comparabletype]) B8 FROM @fromtables A --->
{_countormax |= "COUNT"}
{_countormax |= "MAX"}
SELECT _countormax(_variable[#arg @comparabletype]) B8 FROM @fromtables A


SELECT @agg(_variable[#arg @comparabletype]) B9 FROM @fromtables A WHERE _maybe A._variable[#arg @comparabletype] _cmp @comparableconstant

--- count(*), baby
-- TODO: migrate cases like this that are not columntype/comparabletype-specific to their own template/suite
SELECT COUNT(*) FROM @fromtables B10

-- test where expressions
--- test comparison operators (<, <=, =, >=, >)
SELECT * FROM @fromtables A WHERE _maybe _variable[#arg @comparabletype] _cmp @comparableconstant
--- test arithmetic operators (+, -, *, /) with comparison ops
SELECT * FROM @fromtables A WHERE (_variable[#arg @comparabletype] @aftermath) _cmp @comparableconstant
--- test logic operators (AND) with comparison ops
SELECT * FROM @fromtables A WHERE (_variable[#arg @comparabletype] _cmp @comparableconstant) _logicop @columnpredicate
-- test GROUP BY
SELECT _variable[#grouped @columntype] B14 FROM @fromtables A GROUP BY __[#grouped]

{_optionallimitoffset |= ""}
{_optionallimitoffset |= "LIMIT _value[int:1,3]"}
{_optionallimitoffset |= "LIMIT _value[int:1,3] OFFSET _value[int:1,3]"}
-- OFFSET without LIMIT not supported -- is that SQL standard?
--{_optionallimitoffset |= "                      OFFSET _value[int:1,3]"}

-- test ORDER BY with optional LIMIT/OFFSET
-- HSQL disagrees about ordering of NULLs descending, this only shows as an error on limit/offset queries,
-- so these two statements (DESC sort order and varying limit offset) are kept separate.
SELECT _variable[#order], _variable B15 FROM @fromtables A ORDER BY __[#order], 2            _optionallimitoffset
SELECT _variable[#order], _variable B16 FROM @fromtables A ORDER BY __[#order], 2 DESC

-- test GROUP BY count(*)
SELECT _variable[#grouped], COUNT(*) AS B17 FROM @fromtables A GROUP BY __[#grouped]
-- test GROUP BY ORDER BY COUNT(*) with optional LIMIT/OFFSET
SELECT _variable[#grouped], COUNT(*) AS B18 FROM @fromtables A GROUP BY __[#grouped] ORDER BY 2, 1 _optionallimitoffset

-- test INNER JOIN (we'll do more two-table join fun separately, this just checks syntax)
SELECT * FROM @fromtables LHS INNER JOIN @fromtables B19RHS ON LHS.@idcol = B19RHS.@idcol
-- TODO: If the intent is to support a schema with multiple id-partitioned tables,
-- this statement is looking for trouble -- might want to migrate it to its own template/suite.
SELECT * FROM @fromtables LHS INNER JOIN @fromtables B20RHS ON LHS._variable[@columntype] = B20RHS._variable[@comparabletype]
