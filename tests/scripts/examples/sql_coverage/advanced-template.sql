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
SELECT -8, A._variable[#arg numeric] FROM @fromtables A WHERE @optionalfn(A.__[#arg] + 5   )        > @comparableconstant
SELECT -7, A._variable[#arg numeric] FROM @fromtables A WHERE @optionalfn(A.__[#arg]       ) + 5    > @comparableconstant
SELECT -6, @optionalfn(A._variable[#arg numeric] + 5   )        NUMSUM FROM @fromtables A ORDER BY NUMSUM
SELECT -5, @optionalfn(A._variable[#arg numeric]       ) + 5    NUMSUM FROM @fromtables A ORDER BY NUMSUM
SELECT -4, A._variable[#arg numeric] FROM @fromtables A WHERE @optionalfn(A.__[#arg] + 5.25)        > @comparableconstant
SELECT -3, A._variable[#arg numeric] FROM @fromtables A WHERE @optionalfn(A.__[#arg]       ) + 5.25 > @comparableconstant
SELECT -2, @optionalfn(A._variable[#arg numeric] + 5.25)        NUMSUM FROM @fromtables A ORDER BY NUMSUM
SELECT -1, @optionalfn(A._variable[#arg numeric]       ) + 5.25 NUMSUM FROM @fromtables A ORDER BY NUMSUM

-- cover some select WHERE expressions not covered by the basic templates

SELECT 1, * FROM @fromtables A WHERE @columnpredicate
SELECT 2, * FROM @fromtables A WHERE @optionalfn(A._variable[#some numeric]) _somecmp (            A._variable[#other numeric]  _math 2)
SELECT 3, * FROM @fromtables A WHERE             A._variable[#some numeric]  _somecmp (@optionalfn(A._variable[#other numeric]) _math 3)

SELECT @optionalfn(A._variable[#picked @columntype]) AS Q4 FROM @fromtables A ORDER BY @optionalfn(A.__[#picked]), 1 LIMIT _value[int:1,10]

-- Found eng-3191 (or similar, anyway) crashed (fixed, since?) with these statements:
-- -- combine where and limit
-- SELECT @optionalfn(A._variable[#picked @columntype]) AS Q5 FROM @fromtables A WHERE  @optionalfn(A._variable[@comparabletype]) _somecmp @optionalfn(A._variable[@comparabletype]) ORDER BY @optionalfn(A.__[#picked]) LIMIT _value[int:1,100]
-- -- combine where and offset
-- SELECT @optionalfn(A._variable[#picked @columntype]) AS Q6 FROM @fromtables A WHERE  @optionalfn(A._variable[@comparabletype]) _somecmp @optionalfn(A._variable[@comparabletype]) ORDER BY @optionalfn(A.__[#picked]) LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- -- compare more columns
-- SELECT @optionalfn(A._variable[@comparabletype]    ) AS Q7 FROM @fromtables A WHERE (@optionalfn(A._variable[@comparabletype]) _somecmp @optionalfn(A._variable[@comparabletype])) _logicop (@optionalfn(A._variable[@comparabletype]) _somecmp @optionalfn(A._variable[@comparabletype]))
-- Now that eng-3191 is fixed, we keep them watered down to reduce the number of generated combinations:
-- Even simplified like this, it crashes (or DID, anyway):
-- SELECT @optionalfn(A._variable[#picked            ]) AS Q5 FROM @fromtables A WHERE             (A._variable[@comparabletype]) _somecmp @optionalfn(A._variable[@comparabletype]) ORDER BY @optionalfn(A.__[#picked]) LIMIT _value[int:1,100]
-- so, it was simplified even further
-- combine where and limit
   SELECT @optionalfn(A._variable[#picked @columntype]) AS Q5 FROM @fromtables A WHERE             (A.__[#picked]               ) _somecmp            (A._variable[@comparabletype]) ORDER BY 1                        LIMIT _value[int:1,100]
-- combine where and offset
   SELECT @optionalfn(A._variable[#picked @columntype]) AS Q6 FROM @fromtables A WHERE             (A._variable[@comparabletype]) _somecmp            (A.__[#picked]               ) ORDER BY 1                        LIMIT _value[int:1,100] OFFSET _value[int:1,100]
-- compare more columns
   SELECT @optionalfn(A._variable[#picked @columntype]) AS Q7 FROM @fromtables A WHERE (           (A.__[#picked]               ) _somecmp            (A._variable[@comparabletype])) _logicop ( @columnpredicate )

-- order by with projection
SELECT 8, @optionalfn(A._variable[@columntype]), ID FROM @fromtables A ORDER BY ID _sortorder
-- order by on two columns
-- eng-631 With multiple columns named the same thing and multiple order by columns using the same
-- column and different sort directions, this statement failed.
-- First explicitly isolate a test for eng-631 where DESC and ASC order are non-sensically combined on
-- the same column.
-- TO avoid too much explosion, separate out SELECT function options from sort order options.
SELECT            (A._variable[#order1 @columntype]) AS Q12,            (A._variable[#order2 @columntype]), A._variable FROM @fromtables A ORDER BY @optionalfn(A.__[#order1]) _sortorder, @optionalfn(A.__[#order2])
SELECT @optionalfn(A._variable[#order1 @columntype]) AS Q13, @optionalfn(A._variable[#order2 @columntype]), A._variable FROM @fromtables A ORDER BY 1 DESC, 2 DESC

SELECT @optionalfn(A._variable[@columntype]) AS Q14, @optionalfn(A._variable[@columntype]), * FROM @fromtables A ORDER BY 1 _sortorder, 2 _sortorder

-- additional aggregation fun
SELECT     _distinctableagg(DISTINCT @optionalfn(        A._variable[@columntype]     ))                                              AS Q15 FROM @fromtables A
SELECT     @agg(                     @optionalfn(        A._variable[@columntype]     ))                                              AS Q16 FROM @fromtables A WHERE @columnpredicate
-- These test that the fixed issue eng-909 -- combining DISTINCT and non-DISTINCT aggs has not regressed.
SELECT     _distinctableagg(DISTINCT @optionalfn(        A._variable[@columntype]     )), @agg(            A._variable[@columntype] ) AS Q18 FROM @fromtables A
SELECT     _distinctableagg(DISTINCT                     A._variable[@columntype]      ), @agg(@optionalfn(A._variable[@columntype])) AS Q19 FROM @fromtables A
SELECT 20,                                               A._variable[#GB @columntype]   , @agg(@optionalfn(A._variable[@columntype]))        FROM @fromtables A GROUP BY         A.__[#GB]
SELECT 21,                           @optionalfn(        A._variable[#GB @columntype] ) , @agg(            A._variable[@columntype] )        FROM @fromtables A GROUP BY         A.__[#GB]
SELECT 22,                           @optionalfn(@onefun(A._variable[#GB @columntype])) , @agg(            A._variable[@columntype] )        FROM @fromtables A GROUP BY @onefun(A.__[#GB])
-- multiple column group by
SELECT 23,               A._variable[#GB1 @columntype],  A._variable[#GB2 @columntype]   , @agg(           A._variable[@columntype])         FROM @fromtables A GROUP BY         A.__[#GB1], A.__[#GB2]

SELECT     @agg(                     @optionalfn(        A._variable[@columntype]     )), @agg(@optionalfn(A._variable[@columntype])) AS Q24 FROM @fromtables A
SELECT     @agg(                     @optionalfn(        A._variable[@columntype]     )), COUNT(*)                                    AS Q25 FROM @fromtables A

-- group by alias (50 - 55)
SELECT 50,                           @optionalfn(@onefun(A._variable[#GB @columntype])) as tag , @agg(            A._variable[@columntype] )        FROM @fromtables A GROUP BY tag
SELECT 51,                           A._variable[#GB @columntype] as tag ,                       @agg(            A._variable[@columntype] )        FROM @fromtables A GROUP BY tag
SELECT 52,              @optionalfn(@onefun(A._variable[#GB @columntype])) as tag1,  A._variable[@columntype] as tag2,  @agg(           A._variable[@columntype])         FROM @fromtables A GROUP BY        tag2, tag1

-- DISTINCT expression (56 - 60)
-- basic select template has covered multiple columns distinct
SELECT DISTINCT @optionalfn(A._variable[@columntype]) AS C56, A._variable FROM @fromtables A 
SELECT DISTINCT @onefun(A._variable[@columntype]) AS C56, A._variable FROM @fromtables A ORDER BY 1, 2 LIMIT 10 

-- Edge case: table aggregate with DISTINCT
SELECT DISTINCT COUNT(*) FROM @fromtables A 
SELECT DISTINCT @agg( A._variable[@columntype] ), COUNT(*)  FROM   @fromtables A 

-- DISTINCT on GROUP BY
SELECT DISTINCT   @agg(@optionalfn(A._variable[@columntype]))                                 FROM @fromtables A GROUP BY         A.__[#GB]
SELECT DISTINCT   A._variable[#GB1 @columntype],  @agg(     A._variable[@columntype])         FROM @fromtables A GROUP BY         A.__[#GB1], A.__[#GB2]
SELECT DISTINCT   A._variable[#GB1 @columntype],  @agg(     A._variable[@columntype])         FROM @fromtables A GROUP BY         A.__[#GB1], A.__[#GB2] ORDER BY 1, 2 LIMIT 5

-- update
-- compare two cols
-- UPDATE @fromtables A SET @updatecolumn = @updatevalue WHERE @optionalfn(A._variable[@columntype]) _somecmp @optionalfn(A._variable[@columntype])
-- comparison with set expression
UPDATE @dmltable A SET @updatecolumn = @updatesource @aftermath WHERE @optionalfn(_variable[@columntype]) _somecmp _variable[@comparabletype]

-- Save more exhaustive LIKE testing for advanced-strings.sql.
-- This is mostly just to catch the error of applying different forms of LIKE to non-strings.
-- TODO: migrate likely-to-error-out cases like this to their own template/suite
SELECT * FROM @fromtables Q26 WHERE Q26._variable[@columntype] _maybe LIKE 'abc%'
SELECT * FROM @fromtables Q27 WHERE Q27._variable[@columntype] _maybe LIKE '%'
SELECT * FROM @fromtables Q28 WHERE Q28._variable[@columntype] _maybe LIKE '%' ESCAPE '!' 
SELECT * FROM @fromtables Q29 WHERE Q29._variable[@columntype] _maybe LIKE '!%' ESCAPE '!' 

----SELECT * FROM @fromtables A WHERE _inoneint
----SELECT * FROM @fromtables A WHERE _inpairofints
------just too slow for now SELECT * FROM @fromtables A WHERE _insomeints
----SELECT * FROM @fromtables A WHERE _inonestring
----SELECT * FROM @fromtables A WHERE _inpairofstrings
------just too slow for now SELECT * FROM @fromtables A WHERE _insomestrings

--- Test CASE WHEN
--- CASE WHEN with expression
SELECT * FROM @fromtables Q34 WHERE CASE WHEN Q34._variable[#arg @columntype] _cmp @comparableconstant THEN Q34._variable[#numone @columntype]     ELSE Q34.__[#arg] * 10 END _cmp @comparableconstant + 10
SELECT * FROM @fromtables Q35 WHERE CASE WHEN Q35._variable[#arg @columntype] _cmp @comparableconstant THEN Q35._variable[#numone @columntype]                            END _cmp @comparableconstant + 10
SELECT __[#numone]        Q36,      CASE WHEN   A._variable[#arg @columntype] _cmp @comparableconstant THEN   A._variable[#numone @columntype]     ELSE   A.__[#arg] * 10 END FROM @fromtables A WHERE @columnpredicate
SELECT __[#arg]           Q37,      CASE WHEN   A._variable[#arg @columntype] _cmp @comparableconstant THEN   A.__[#arg]                                                  END FROM @fromtables A WHERE @columnpredicate
--- CASE WHEN like DECODE
SELECT * FROM @fromtables Q38 WHERE CASE      Q38._variable[#arg @columntype] WHEN @comparableconstant THEN Q38._variable[#numone @columntype] * 2 ELSE Q38.__[#arg] * 10 END _cmp @comparableconstant + 10
SELECT * FROM @fromtables Q39 WHERE CASE      Q39._variable[#arg @columntype] WHEN @comparableconstant THEN Q39._variable[#numone @columntype] * 2                        END _cmp @comparableconstant + 10
SELECT __[#numone]        Q40,      CASE        A._variable[#arg @columntype] WHEN @comparableconstant THEN   A._variable[#numone @columntype] * 2 ELSE   A.__[#arg] * 10 END FROM @fromtables A WHERE @columnpredicate
SELECT __[#arg]           Q41,      CASE        A._variable[#arg @columntype] WHEN @comparableconstant THEN   A._variable[#numone @columntype] * 2                        END FROM @fromtables A WHERE @columnpredicate
