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
SELECT _variable, _variable FROM @from_tables LIMIT _value[int:1,10]

-- combine where and limit
SELECT _variable, _variable FROM @from_tables WHERE _variable _cmp _variable LIMIT _value[int:1,100]
-- compare more columns
SELECT _variable FROM @from_tables WHERE (_variable _cmp _variable) _logic (_variable _cmp _variable)

-- order by with projection
SELECT _variable, ID FROM @from_tables ORDER BY ID _sortorder
-- order by on two columns
SELECT _variable[@order1], _variable[@order2], _variable FROM @from_tables ORDER BY _variable[@order1] _sortorder, _variable[@order2]
-- ticket 232
SELECT NUM FROM @from_tables GROUP BY NUM ORDER BY NUM

-- two _sortorder templates have some issue I'm not figuring out right now
-- We get non-deterministic sort order on the non-orderby columns so leaving it out for now
SELECT * from @from_tables ORDER BY _variable, _variable _sortorder

-- additional aggregation fun
SELECT _agg(DISTINCT(_variable)) FROM @from_tables
SELECT _agg(_variable), _agg(_variable) FROM @from_tables
SELECT _variable, _agg(_variable) FROM @from_tables GROUP BY _variable
SELECT SUM(_variable _math _variable) FROM @from_tables

-- additional select expression math
SELECT _variable _math _value[float] FROM @from_tables
SELECT _variable _math _variable FROM @from_tables
-- push on divide by zero
SELECT _variable / 0 from @from_tables
SELECT _variable / 0.0 from @from_tables
SELECT _variable / -1e-306 from @from_tables

-- update
-- compare two cols
-- UPDATE @from_tables SET @assign_col = @assign_type WHERE _variable _cmp _variable
-- comparison with set expression
UPDATE @from_tables SET @assign_col = @assign_col _math _value[int:0,5] WHERE _variable _cmp @cmp_type
