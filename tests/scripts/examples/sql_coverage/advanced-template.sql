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
SELECT 2, * FROM @fromtables WHERE @optionalfn(_variable[#some numeric]) _cmp (            _variable[#other numeric]  _math _value[int:1,3])
SELECT 3, * FROM @fromtables WHERE             _variable[#some numeric]  _cmp (@optionalfn(_variable[#other numeric]) _math _value[int:1,3])
SELECT 4, * FROM @fromtables WHERE             _variable[#same numeric]  _cmp (@optionalfn(       __[#same         ]) _math 2)

-- ENG-3196 "SELECT ABS(ID) AS Q4 FROM R1 ORDER BY (ID) LIMIT 10;" gets UNEXPECTED FAILURE tupleValueFactory: invalid column_idx.
-- SELECT @optionalfn(_variable[#picked @columntype]) AS Q4 FROM @fromtables ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,10]
-- so, simplify as:
   SELECT            (_variable[#picked @columntype]) AS Q4 FROM @fromtables ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,10]

-- Found ENG-3191 (or similar, anyway) crashes (fixed, since?) with these statements:
-- -- combine where and limit
-- SELECT @optionalfn(_variable[#picked @columntype]) AS Q5 FROM @fromtables WHERE  @optionalfn(_variable[@comparabletype]) _cmp @optionalfn(_variable[@comparabletype]) ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,100]
-- -- combine where and offset
-- SELECT @optionalfn(_variable[#picked @columntype]) AS Q6 FROM @fromtables WHERE  @optionalfn(_variable[@comparabletype]) _cmp @optionalfn(_variable[@comparabletype]) ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- -- compare more columns
-- SELECT @optionalfn(_variable[@comparabletype]    ) AS Q7 FROM @fromtables WHERE (@optionalfn(_variable[@comparabletype]) _cmp @optionalfn(_variable[@comparabletype])) _logicop (@optionalfn(_variable[@comparabletype]) _cmp @optionalfn(_variable[@comparabletype]))
-- Now that ENG-3191 is fixed, we keep them watered down to reduce the number of generated combinations:
-- combine where and limit
-- Even simplified like this, it crashes (or DID, anyway):
-- SELECT @optionalfn(_variable[#picked            ]) AS Q5 FROM @fromtables WHERE             (_variable[@comparabletype]) _cmp @optionalfn(_variable[@comparabletype]) ORDER BY @optionalfn(__[#picked]) LIMIT _value[int:1,100]
-- so, it was simplified even further
   SELECT @optionalfn(_variable[#picked @columntype]) AS Q5 FROM @fromtables WHERE             (__[#picked]               ) _cmp            (_variable[@comparabletype]) ORDER BY 1                        LIMIT _value[int:1,100]
-- combine where and offset
   SELECT @optionalfn(_variable[#picked @columntype]) AS Q6 FROM @fromtables WHERE             (_variable[@comparabletype]) _cmp            (__[#picked]               ) ORDER BY 1                        LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- compare more columns
   SELECT @optionalfn(_variable[#picked @columntype]) AS Q7 FROM @fromtables WHERE (           (__[#picked]               ) _cmp            (_variable[@comparabletype])) _logicop ( @columnpredicate )

-- order by with projection
SELECT 11, @optionalfn(_variable[@columntype]), ID FROM @fromtables ORDER BY ID _sortorder
-- order by on two columns
-- ENG-631
-- With multiple columns named the same thing and multiple order by columns using the same column and different
-- sort directions, this statement fails.  Commenting it out and going with one that forces the sort orders for now
--SELECT @optionalfn(_variable[#order1 @columntype]), @optionalfn(_variable[#order2 @columntype]), _variable FROM @fromtables ORDER BY @optionalfn(__[#order1]) _sortorder, @optionalfn(__[#order2])
SELECT 12, @optionalfn(_variable[#order1 @columntype]), @optionalfn(_variable[#order2 @columntype]), _variable FROM @fromtables ORDER BY 2 DESC, 3 DESC

-- two _sortorder templates have some issue I'm not figuring out right now
-- We get non-deterministic sort order on the non-orderby columns so leaving it out for now
-- This also appears to fail due to ENG-631 if the variables are the same.  Using
-- a less generic version
--SELECT * from @fromtables ORDER BY @optionalfn(_variable[@columntype]), @optionalfn(_variable[@columntype]) _sortorder
SELECT 14, * from @fromtables ORDER BY @optionalfn(_variable[@columntype]), @optionalfn(_variable[@columntype])

-- additional aggregation fun
SELECT 15, @agg(DISTINCT(@optionalfn(_variable[@columntype]))) FROM @fromtables
SELECT 16, @agg(         @optionalfn(_variable[@columntype]) ) FROM @fromtables WHERE @columnpredicate
-- These test that the fixed issue ENG-909 -- combining DISTINCT and non-DSTINCT aggs has not regressed.
SELECT 18, @agg(DISTINCT(@optionalfn(_variable[@columntype]))), @agg(            _variable[@columntype] ) FROM @fromtables
SELECT 19, @agg(DISTINCT(            _variable[@columntype] )), @agg(@optionalfn(_variable[@columntype])) FROM @fromtables
SELECT 20,             _variable[#GB @columntype] , @agg(@optionalfn(_variable[@columntype])) FROM @fromtables GROUP BY             __[#GB]
-- TODO: migrate likely-to-error-out cases like this to their own template/suite
SELECT 21, @optionalfn(_variable[#GB @columntype]), @agg(            _variable[@columntype] ) FROM @fromtables GROUP BY @optionalfn(__[#GB])

-- ENG-199.  Substituting this generic version
-- with a few specific dual aggregates that will be different
--SELECT _numagg(@optionalfn(_variable[@columntype])), _numagg(@optionalfn(_variable[@columntype])) FROM @fromtables
  SELECT @agg(@optionalfn(_variable[#picked @columntype])), @agg(@optionalfn(_variable[@comparabletype]))                     AS Q22 FROM @fromtables
  SELECT @agg(@optionalfn(_variable[#picked @columntype])),                                                COUNT(*)           AS Q23 FROM @fromtables

-- update
-- compare two cols
-- UPDATE @fromtables SET @updatecolumn = @updatevalue WHERE @optionalfn(_variable[@columntype]) _cmp @optionalfn(_variable[@columntype])
-- comparison with set expression
UPDATE @fromtables SET @updatecolumn = @updatevalue @aftermath WHERE @optionalfn(_variable[@columntype]) _cmp _variable[@comparabletype]

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
