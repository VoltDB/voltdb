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
SELECT -3, @optional_fn(_variable + 5) AS NUMSUM FROM @from_tables WHERE NUMSUM > @cmp_type
SELECT -2, @optional_fn(_variable) + 5 AS NUMSUM FROM @from_tables WHERE NUMSUM > @cmp_type
SELECT -1, @optional_fn(_variable + 5) AS NUMSUM FROM @from_tables ORDER BY NUMSUM
SELECT 0, @optional_fn(_variable) + 5 AS NUMSUM FROM @from_tables ORDER BY NUMSUM

-- cover some select WHERE expressions not covered by the basic templates
SELECT 1, * FROM @from_tables WHERE @optional_fn(_variable) _cmp _value[float]
SELECT 2, * FROM @from_tables WHERE @optional_fn(_variable) _cmp _value[string]
-- Found ENG-3191 crash with this statement:
-- SELECT 3, * FROM @from_tables WHERE @optional_fn(_variable) _cmp (@optional_fn2(_variable) _math _value[int:0,1000])
-- even with:
-- SELECT 3, * FROM @from_tables WHERE             (_variable) _cmp (@optional_fn2(_variable) _math _value[int:0,1000])
-- Until ENG-3191 is fixed, keep it watered down to:
   SELECT 3, * FROM @from_tables WHERE @optional_fn(_variable) _cmp (             (_variable) _math _value[int:0,1000])

-- ENG-3196 "SELECT ABS(ID) AS Q4 FROM R1 ORDER BY (ID) LIMIT 10;" gets UNEXPECTED FAILURE tupleValueFactory: invalid column_idx.
-- SELECT @optional_fn(_variable[@order]) AS Q4 FROM @from_tables ORDER BY @optional_fn2(_variable[@order]) LIMIT _value[int:1,10]
-- so, simplify as:
   SELECT             (_variable[@order]) AS Q4 FROM @from_tables ORDER BY @optional_fn2(_variable[@order]) LIMIT _value[int:1,10]

-- Found ENG-3191 (or similar, anyway) crashes with these statements:
-- -- combine where and limit
-- SELECT @optional_fn(_variable[@order]) AS Q5 FROM @from_tables WHERE @optional_fn(_variable) _cmp @optional_fn2(_variable) ORDER BY @optional_fn(_variable[@order]) LIMIT _value[int:1,100]
-- -- combine where and offset
-- SELECT @optional_fn(_variable[@order]) AS Q6 FROM @from_tables WHERE @optional_fn(_variable) _cmp @optional_fn2(_variable) ORDER BY @optional_fn(_variable[@order]) LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- -- compare more columns
-- SELECT @optional_fn(_variable)         AS Q7 FROM @from_tables WHERE (@optional_fn(_variable) _cmp @optional_fn2(_variable)) _logic (@optional_fn2(_variable) _cmp @optional_fn(_variable))
-- Until ENG-3191 is fixed, keep them watered down to:
-- combine where and limit
-- Even simplified like this, it crashes:
-- SELECT @optional_fn(_variable[@order]) AS Q5 FROM @from_tables WHERE             (_variable) _cmp @optional_fn2(_variable) ORDER BY @optional_fn(_variable[@order]) LIMIT _value[int:1,100]
-- so, simplify it down even further
   SELECT @optional_fn(_variable[@order]) AS Q5 FROM @from_tables WHERE             (_variable) _cmp              (_variable) ORDER BY @optional_fn(_variable[@order]) LIMIT _value[int:1,100]
-- combine where and offset
   SELECT @optional_fn(_variable[@order]) AS Q6 FROM @from_tables WHERE             (_variable) _cmp              (_variable) ORDER BY @optional_fn(_variable[@order]) LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- compare more columns
   SELECT @optional_fn(_variable)         AS Q7 FROM @from_tables WHERE (            (_variable) _cmp              (_variable)) _logic (            (_variable) _cmp             (_variable))

-- order by with projection
SELECT 11, @optional_fn(_variable), ID FROM @from_tables ORDER BY ID _sortorder
-- order by on two columns
-- ENG-631
-- With multiple columns named the same thing and multiple order by columns using the same column and different
-- sort directions, this statement fails.  Commenting it out and going with one that forces the sort orders for now
--SELECT @optional_fn(_variable[@order1]), @optional_fn2(_variable[@order2]), _variable FROM @from_tables ORDER BY @optional_fn(_variable[@order1]) _sortorder, @optional_fn2(_variable[@order2])
SELECT 12, @optional_fn(_variable[@order1]), @optional_fn2(_variable[@order2]), _variable FROM @from_tables ORDER BY @optional_fn(_variable[@order1]) DESC, @optional_fn2(_variable[@order2]) DESC
-- order by with generic expression
SELECT _variable[@A], _variable[@B], @optional_fn(__[@A]) _math @optional_fn2(__[@B]) AS FOO13 FROM @from_tables ORDER BY FOO13
SELECT @optional_fn(_variable _math @optional_fn2(_variable)) AS FOO14 FROM @from_tables ORDER BY FOO14

-- ticket 232
SELECT NUM FROM @from_tables GROUP BY NUM ORDER BY NUM

-- two _sortorder templates have some issue I'm not figuring out right now
-- We get non-deterministic sort order on the non-orderby columns so leaving it out for now
-- This also appears to fail due to ENG-631 if the variables are the same.  Using
-- a less generic version
--SELECT * from @from_tables ORDER BY @optional_fn(_variable), @optional_fn2(_variable) _sortorder
SELECT 16, * from @from_tables ORDER BY @optional_fn(_variable), @optional_fn2(_variable)

-- additional aggregation fun
SELECT 17, _agg(DISTINCT(@optional_fn(_variable))) FROM @from_tables
-- ENG-909
SELECT 18, _agg(DISTINCT(@optional_fn(_variable))), _agg(@optional_fn2(_variable)) FROM @from_tables
SELECT 19, @optional_fn(_variable[@GB]), _agg(@optional_fn2(_variable)) FROM @from_tables GROUP BY @optional_fn(_variable[@GB])
-- ENG-205
SELECT _agg[@OP](_variable[@VA]) AS Q20, _agg[@OP](_variable[@VB]), __[@OP](@optional_fn(__[@VA]) _math @optional_fn2(__[@VB])) FROM @from_tables
SELECT SUM(DISTINCT @optional_fn(_variable) _math @optional_fn2(_variable)) AS Q21 FROM @from_tables
-- Can't use this statement because HSQL gets ridiculous answers for cases like impossible positive values for "SELECT SUM(DISTINCT (NUM - ABS(NUM))) AS Q22 FROM P1;"
--SELECT SUM(DISTINCT @optional_fn(_variable _math @optional_fn2(_variable))) AS Q22 FROM @from_tables, so simplify
  SELECT SUM(DISTINCT @optional_fn(_variable _math              (_variable))) AS Q22 FROM @from_tables

-- ENG-199.  Substituting this generic version
-- with a few specific dual aggregates that will be different
--SELECT _agg(@optional_fn(_variable)), _agg(@optional_fn2(_variable)) FROM @from_tables
SELECT MIN(@optional_fn(_variable)), MAX(@optional_fn2(_variable)) AS Q23 FROM @from_tables
SELECT COUNT(_variable), SUM(@optional_fn(_variable)) AS Q24 FROM @from_tables
SELECT AVG(@optional_fn(_variable)), COUNT(_variable) AS Q25 FROM @from_tables
SELECT MAX(@optional_fn(_variable)), SUM(@optional_fn2(_variable)), COUNT(_variable) AS Q26 FROM @from_tables
SELECT MAX(@optional_fn(_variable)), @optional_fn2(SUM(_variable)), COUNT(_variable) AS Q27 FROM @from_tables

-- additional select expression math
-- Causes frequent failures due to floating point rounding
--SELECT @optional_fn(_variable) _math _value[float] FROM @from_tables
SELECT @optional_fn(_variable) _math @optional_fn2(_variable) AS Q27 FROM @from_tables
-- Can't use this statement because HSQL gets ridiculous answers for cases like impossible positive values for "SELECT NUM, ABS(NUM - ABS(NUM)) AS Q28 FROM P1;"
SELECT _variable[@A], @optional_fn(_variable[@A] _math @optional_fn2(_variable)) AS Q28 FROM @from_tables

-- push on divide by zero
SELECT @optional_fn(_variable) / 0   AS Q29 FROM @from_tables
SELECT             (_variable) / 0.0 AS Q30 FROM @from_tables
SELECT @optional_fn(_variable / 0.0) AS Q31 FROM @from_tables
-- we throw an underflow exception and HSQL returns INF.
--SELECT @optional_fn(_variable) / -1e-306 from @from_tables

-- update
-- compare two cols
-- UPDATE @from_tables SET @assign_col = @assign_type WHERE @optional_fn(_variable) _cmp @optional_fn(_variable)
-- comparison with set expression
UPDATE @from_tables SET @assign_col = @assign_col _math _value[int:0,5] WHERE @optional_fn(_variable) _cmp @cmp_type

-- Save more exhaustive LIKE testing for advanced-strings.sql.
-- This is mostly just to catch the error of applying different forms of LIKE to non-strings.
SELECT * FROM @from_tables WHERE _variable _like 'abc%'
SELECT * FROM @from_tables WHERE _variable _like '%'
SELECT * FROM @from_tables WHERE _variable _like '%' ESCAPE '!' 
SELECT * FROM @from_tables WHERE _variable _like '!%' ESCAPE '!' 
