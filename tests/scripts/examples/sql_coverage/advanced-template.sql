-- Temporarily, this file holds patterns that are more complex or
-- differ from the SQL tested in the basic-template.sql file

-- Required preprocessor macros (with example values):
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@aftermath = " _math _value[int:1,3]"}
-- {@columntype = "decimal"}
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@dmltable = "_table"}
-- {@fromtables = "_table"}
-- {@optionalfn = "_numfun"}
-- {@updatecolumn = "CASH"}
-- {@updatevalue = "_value[decimal]"}

-- Reduce the explosions that result from 7 _cmp values.
{_somecmp |= " < "}
{_somecmp |= " >= "}
{_somecmp |= " = "}

-- DML, clean out and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)


-- alias fun
-- ticket 231
SELECT -8, _variable[#arg numeric] FROM @fromtables WHERE @optionalfn(__[#arg] + 5   )        > @comparableconstant
SELECT -7, _variable[#arg numeric] FROM @fromtables WHERE @optionalfn(__[#arg]       ) + 5    > @comparableconstant
SELECT -6, @optionalfn(_variable[numeric] + 5   )        NUMSUM FROM @fromtables ORDER BY NUMSUM
SELECT -5, @optionalfn(_variable[numeric]       ) + 5    NUMSUM FROM @fromtables ORDER BY NUMSUM
SELECT -4, _variable[#arg numeric] FROM @fromtables WHERE @optionalfn(__[#arg] + 5.25)        > @comparableconstant
SELECT -3, _variable[#arg numeric] FROM @fromtables WHERE @optionalfn(__[#arg]       ) + 5.25 > @comparableconstant
SELECT -2, @optionalfn(_variable[numeric] + 5.25)        NUMSUM FROM @fromtables ORDER BY NUMSUM
SELECT -1, @optionalfn(_variable[numeric]       ) + 5.25 NUMSUM FROM @fromtables ORDER BY NUMSUM

-- cover some select WHERE expressions not covered by the basic templates

SELECT 1, * FROM @fromtables WHERE @columnpredicate
SELECT 2, * FROM @fromtables WHERE @optionalfn(_variable[#some numeric]) _somecmp (            _variable[#other numeric]  _math 2)
SELECT 3, * FROM @fromtables WHERE             _variable[#some numeric]  _somecmp (@optionalfn(_variable[#other numeric]) _math 3)

-- eng-3196 "SELECT ABS(ID) AS Q4 FROM R1 ORDER BY (ID) LIMIT 10;" got UNEXPECTED FAILURE tupleValueFactory: invalid column_idx.
-- SELECT @optionalfn(_variable[#picked @columntype]) AS Q4 FROM @fromtables ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,10]
-- so, simplify as:
   SELECT            (_variable[#picked @columntype]) AS Q4 FROM @fromtables ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,10]

-- Found eng-3191 (or similar, anyway) crashed (fixed, since?) with these statements:
-- -- combine where and limit
-- SELECT @optionalfn(_variable[#picked @columntype]) AS Q5 FROM @fromtables WHERE  @optionalfn(_variable[@comparabletype]) _somecmp @optionalfn(_variable[@comparabletype]) ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,100]
-- -- combine where and offset
-- SELECT @optionalfn(_variable[#picked @columntype]) AS Q6 FROM @fromtables WHERE  @optionalfn(_variable[@comparabletype]) _somecmp @optionalfn(_variable[@comparabletype]) ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- -- compare more columns
-- SELECT @optionalfn(_variable[@comparabletype]    ) AS Q7 FROM @fromtables WHERE (@optionalfn(_variable[@comparabletype]) _somecmp @optionalfn(_variable[@comparabletype])) _logicop (@optionalfn(_variable[@comparabletype]) _somecmp @optionalfn(_variable[@comparabletype]))
-- Now that eng-3191 is fixed, we keep them watered down to reduce the number of generated combinations:
-- Even simplified like this, it crashes (or DID, anyway):
-- SELECT @optionalfn(_variable[#picked            ]) AS Q5 FROM @fromtables WHERE             (_variable[@comparabletype]) _somecmp @optionalfn(_variable[@comparabletype]) ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,100]
-- so, it was simplified even further
-- combine where and limit
   SELECT @optionalfn(_variable[#picked @columntype]) AS Q5 FROM @fromtables WHERE             (__[#picked]               ) _somecmp            (_variable[@comparabletype]) ORDER BY 1                        LIMIT _value[int:1,100]
-- combine where and offset
   SELECT @optionalfn(_variable[#picked @columntype]) AS Q6 FROM @fromtables WHERE             (_variable[@comparabletype]) _somecmp            (__[#picked]               ) ORDER BY 1                        LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- compare more columns
   SELECT @optionalfn(_variable[#picked @columntype]) AS Q7 FROM @fromtables WHERE (           (__[#picked]               ) _somecmp            (_variable[@comparabletype])) _logicop ( @columnpredicate )

-- order by with projection
SELECT 8, @optionalfn(_variable[@columntype]), ID FROM @fromtables ORDER BY ID _sortorder
-- order by on two columns
-- eng-631 With multiple columns named the same thing and multiple order by columns using the same
-- column and different sort directions, this statement failed.
-- First explicitly isolate a test for eng-631 where DESC and ASC order are non-sensically combined on
-- the same column.
-- TO avoid too much explosion, separate out SELECT function options from sort order options.
SELECT            (_variable[#order1 @columntype]),            (_variable[#order2 @columntype]), _variable FROM @fromtables Q12 ORDER BY @optionalfn(__[#order1]) _sortorder, @optionalfn(__[#order2])
SELECT @optionalfn(_variable[#order1 @columntype]), @optionalfn(_variable[#order2 @columntype]), _variable FROM @fromtables Q13 ORDER BY 1 DESC, 2 DESC

SELECT @optionalfn(_variable[@columntype]), @optionalfn(_variable[@columntype]), * FROM @fromtables AS Q14 ORDER BY 1 _sortorder, 2 _sortorder

-- additional aggregation fun
SELECT 15, @agg(DISTINCT(@optionalfn(_variable[@columntype]))) FROM @fromtables
SELECT 16, @agg(         @optionalfn(_variable[@columntype]) ) FROM @fromtables WHERE @columnpredicate
-- These test that the fixed issue eng-909 -- combining DISTINCT and non-DISTINCT aggs has not regressed.
SELECT 18, @agg(DISTINCT(@optionalfn(_variable[@columntype]))), @agg(            _variable[@columntype] ) FROM @fromtables
SELECT 19, @agg(DISTINCT(            _variable[@columntype] )), @agg(@optionalfn(_variable[@columntype])) FROM @fromtables
SELECT 20,             _variable[#GB @columntype] , @agg(@optionalfn(_variable[@columntype])) FROM @fromtables GROUP BY             __[#GB]
-- TODO: migrate likely-to-error-out cases like this to their own template/suite
SELECT 21, @optionalfn(_variable[#GB @columntype]), @agg(            _variable[@columntype] ) FROM @fromtables GROUP BY @optionalfn(__[#GB])

SELECT @agg(@optionalfn(_variable[@columntype])), @agg(@optionalfn(_variable[@columntype]))           AS Q22 FROM @fromtables
SELECT @agg(@optionalfn(_variable[@columntype])),                                           COUNT(*)  AS Q23 FROM @fromtables

-- update
-- compare two cols
-- UPDATE @fromtables SET @updatecolumn = @updatevalue WHERE @optionalfn(_variable[@columntype]) _somecmp @optionalfn(_variable[@columntype])
-- comparison with set expression
UPDATE @fromtables SET @updatecolumn = @updatesource @aftermath WHERE @optionalfn(_variable[@columntype]) _somecmp _variable[@comparabletype]

-- Save more exhaustive LIKE testing for advanced-strings.sql.
-- This is mostly just to catch the error of applying different forms of LIKE to non-strings.
-- TODO: migrate likely-to-error-out cases like this to their own template/suite
SELECT * FROM @fromtables WHERE _variable[@columntype] _maybe LIKE 'abc%'
SELECT * FROM @fromtables WHERE _variable[@columntype] _maybe LIKE '%'
SELECT * FROM @fromtables WHERE _variable[@columntype] _maybe LIKE '%' ESCAPE '!' 
SELECT * FROM @fromtables WHERE _variable[@columntype] _maybe LIKE '!%' ESCAPE '!' 

----SELECT * FROM @fromtables WHERE _inoneint
----SELECT * FROM @fromtables WHERE _inpairofints
------just too slow for now SELECT * FROM @fromtables WHERE _insomeints
----SELECT * FROM @fromtables WHERE _inonestring
----SELECT * FROM @fromtables WHERE _inpairofstrings
------just too slow for now SELECT * FROM @fromtables WHERE _insomestrings
