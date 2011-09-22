-- Temporarily, this file holds patterns that are more complex or
-- differ from the SQL tested in the basic-template.sql file

-- DML, generate random data first.
INSERT INTO _table VALUES (@insert_vals)

-- total garbage
_value[string] _value[string] _value[string] _value[string]
_value[float] _value[float] _value[float] _value[float]
_value[int64] _value[int64] _value[int64] _value[int64]

-- alias fun
-- ticket 231
SELECT (_variable + 5) AS NUMSUM FROM @from_tables WHERE NUMSUM > @cmp_type
SELECT (_variable + 5) AS NUMSUM FROM @from_tables ORDER BY NUMSUM

-- cover some select WHERE expressions not covered by the basic templates
SELECT * FROM @from_tables WHERE _variable _cmp _value[float]
SELECT * FROM @from_tables WHERE _variable _cmp _value[string]
SELECT * FROM @from_tables WHERE _variable _cmp (_variable _math _value[int:0,1000])
SELECT _variable[@order] FROM @from_tables ORDER BY _variable[@order] LIMIT _value[int:1,10]

-- combine where and limit
SELECT _variable[@order] FROM @from_tables WHERE _variable _cmp _variable ORDER BY _variable[@order] LIMIT _value[int:1,100]
-- combine where and offset
SELECT _variable[@order] FROM @from_tables WHERE _variable _cmp _variable ORDER BY _variable[@order] LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- compare more columns
SELECT _variable FROM @from_tables WHERE (_variable _cmp _variable) _logic (_variable _cmp _variable)

-- order by with projection
SELECT _variable, ID FROM @from_tables ORDER BY ID _sortorder
-- order by on two columns
-- ENG-631
-- With multiple columns named the same thing and multiple order by columns using the same column and different
-- sort directions, this statement fails.  Commenting it out and going with one that forces the sort orders for now
--SELECT _variable[@order1], _variable[@order2], _variable FROM @from_tables ORDER BY _variable[@order1] _sortorder, _variable[@order2]
SELECT _variable[@order1], _variable[@order2], _variable FROM @from_tables ORDER BY _variable[@order1] DESC, _variable[@order2] DESC
-- order by with generic expression
SELECT _variable[@order1] _math _variable[@order2] AS FOO FROM @from_tables ORDER BY FOO

-- ticket 232
SELECT NUM FROM @from_tables GROUP BY NUM ORDER BY NUM

-- two _sortorder templates have some issue I'm not figuring out right now
-- We get non-deterministic sort order on the non-orderby columns so leaving it out for now
-- This also appears to fail due to ENG-631 if the variables are the same.  Using
-- a less generic version
--SELECT * from @from_tables ORDER BY _variable, _variable _sortorder
SELECT * from @from_tables ORDER BY _variable, _variable

-- additional aggregation fun
SELECT _agg(DISTINCT(_variable)) FROM @from_tables
-- ENG-909
SELECT _agg(DISTINCT(_variable)), _agg(_variable) FROM @from_tables
SELECT _variable, _agg(_variable) FROM @from_tables GROUP BY _variable
-- ENG-205
SELECT SUM(_variable _math _variable) FROM @from_tables
SELECT SUM(DISTINCT _variable _math _variable) FROM @from_tables
-- ENG-199.  Substituting this generic version
-- with a few specific dual aggregates that will be different
--SELECT _agg(_variable), _agg(_variable) FROM @from_tables
SELECT MIN(_variable), MAX(_variable) FROM @from_tables
SELECT COUNT(_variable), SUM(_variable) FROM @from_tables
SELECT AVG(_variable), COUNT(_variable) FROM @from_tables
SELECT MAX(_variable), SUM(_variable), COUNT(_variable) FROM @from_tables

-- additional select expression math
-- Causes frequent failures due to floating point rounding
--SELECT _variable _math _value[float] FROM @from_tables
SELECT _variable _math _variable FROM @from_tables
-- push on divide by zero
SELECT _variable / 0 from @from_tables
SELECT _variable / 0.0 from @from_tables
-- we throw an underflow exception and HSQL returns INF.
--SELECT _variable / -1e-306 from @from_tables

-- update
-- compare two cols
-- UPDATE @from_tables SET @assign_col = @assign_type WHERE _variable _cmp _variable
-- comparison with set expression
UPDATE @from_tables SET @assign_col = @assign_col _math _value[int:0,5] WHERE _variable _cmp @cmp_type
