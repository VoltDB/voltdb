<configure-default.sql>

--- DML: purge and regenerate random data first
DELETE FROM @dmltable
{@insertvals_not_null = "_id, _value[string], _value[int16], _value[float]"}

INSERT INTO @dmltable VALUES (@insertvals_not_null)

-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol
--SELECT SUM(ID), SUM(NUM), SUM(RATIO) FROM @fromtables

--- Define "place-holders" used in the queries below
-- TODO: uncomment this once ENG-8439 (IN ignores LIMIT in sub-query) is fixed
--{_optionaloffset |= ""}
{_optionaloffset |= "OFFSET 2"}
{_optionallimitoffset |= ""}
{_optionallimitoffset |= "LIMIT 4 _optionaloffset"}

{_optionalorderbycollimitoffset |= ""}
{_optionalorderbycollimitoffset |= "LIMIT 1000"}
{_optionalorderbycollimitoffset |= "ORDER BY __[#col] _sortorder _optionallimitoffset"}

{_groupbycoloptionalhavingagg |= "GROUP BY __[#col]"}
{_groupbycoloptionalhavingagg |= "GROUP BY __[#col] HAVING @agg(__[#agg]) _cmp A32.__[#agg]"}

{_anyorall |= "ANY"}
{_anyorall |= "ALL"}

{_colexpr |= "1+__[#col]"}
{_colexpr |= "__[#col]-1"}
{_colexpr |= "__[#col]*2"}
{_colexpr |= "__[#col]/2"}

--- Test IN/EXISTS Advanced cases
SELECT * FROM @fromtables A01 WHERE _variable[#agg @columntype] _maybe IN ( SELECT @agg(__[#agg]) FROM @fromtables B WHERE B._variable[@comparabletype] _cmp A01._variable[@comparabletype] )
SELECT * FROM @fromtables A02 WHERE EXISTS ( SELECT _variable[#GB]  FROM @fromtables B GROUP BY B.__[#GB] HAVING MAX(B._variable[@columntype]) _cmp  A02._variable[@columntype] )

SELECT * FROM @fromtables A03 LHS _jointype JOIN @fromtables RHS_10 ON LHS.@idcol = RHS_10.@idcol where LHS._variable[@columntype] _maybe IN (SELECT _variable[@columntype] FROM @fromtables IN_TABLE)
SELECT 100, * FROM @fromtables A04 WHERE _variable[#col] _maybe IN ( SELECT LHS.__[#col] FROM @fromtables LHS _jointype JOIN @fromtables RHS_11 ON LHS.@idcol = RHS_11.@idcol where LHS._variable[@columntype] _cmp A04._variable[@columntype] )
-- Uncomment after ENG-9367 is fixed (??):
--SELECT 100, * FROM @fromtables A05 WHERE _variable[#col] _maybe IN ( SELECT     __[#col] FROM @fromtables LHS _jointype JOIN @fromtables RHS_11 ON LHS.@idcol = RHS_11.@idcol where LHS._variable[@columntype] _cmp A05._variable[@columntype] )


--- Additional tests of IN
--- IN using: a simple case; set operators; a sub-query using WHERE with optional ORDER BY, LIMIT and/or OFFSET
SELECT _variable[#col] FROM @fromtables A21 WHERE __[#col] _maybe IN (SELECT __[#col] FROM @fromtables)
SELECT _variable[#col] FROM @fromtables A22 WHERE __[#col] _maybe IN (SELECT __[#col] FROM @fromtables _setop SELECT __[#col] FROM @fromtables)
SELECT _variable[#col] FROM @fromtables A23 WHERE __[#col] _maybe IN (SELECT __[#col] FROM @fromtables WHERE __[#col] _cmp 12 _optionalorderbycollimitoffset)

--- IN using multiple columns, with increasing complexity (last one with a nested sub-sub-query)
SELECT _variable[#col1], _variable[#col2] FROM @fromtables A24 WHERE  __[#col1]             _maybe IN (SELECT __[#col1]            FROM @fromtables WHERE __[#col1] _cmp 12)
SELECT _variable[#col1], _variable[#col2] FROM @fromtables A25 WHERE (__[#col1], __[#col2]) _maybe IN (SELECT __[#col1], __[#col2] FROM @fromtables WHERE __[#col2] _cmp 12)
SELECT _variable[#col1], _variable[#col2] FROM @fromtables A26 WHERE (__[#col1], __[#col2]) _maybe IN (SELECT __[#col1], __[#col2] FROM @fromtables WHERE __[#col1] _maybe IN (SELECT __[#col1] FROM @fromtables WHERE __[#col1] _cmp 12))

--- IN using (GROUP BY and) HAVING with a sub-query
--- Note: HAVING with sub-query is not currently (June 2015) supported, but VoltDB & HSQLDB return the same error
SELECT _variable[#col], @agg(_variable[#agg]) FROM @fromtables A26 GROUP BY __[#col] HAVING @agg(__[#agg]) IN (SELECT _variable[#agg] FROM @fromtables)


--- Additional tests of EXISTS
--- EXISTS using: optional ORDER BY, LIMIT and/or OFFSET; GROUP BY, with (optional) HAVING
SELECT * FROM @fromtables A31 WHERE _maybe EXISTS (SELECT _variable[#col]                        FROM @fromtables SQ WHERE SQ.__[#col] _cmp A31.__[#col] _optionalorderbycollimitoffset)
SELECT * FROM @fromtables A32 WHERE _maybe EXISTS (SELECT _variable[#col], @agg(_variable[#agg]) FROM @fromtables _groupbycoloptionalhavingagg)

--- EXISTS using set operators (UNION, INTERSECT, EXCEPT [ALL]) in the sub-query
SELECT * FROM @fromtables A33 WHERE _maybe EXISTS (SELECT _variable[#col] FROM @fromtables _setop SELECT __[#col] FROM @fromtables)
SELECT * FROM @fromtables A34 WHERE _maybe EXISTS (SELECT _variable[#col] FROM @fromtables _setop SELECT __[#col] FROM @fromtables SQ WHERE SQ.__[#col] _cmp A34.__[#col])

--- EXISTS using an implicit join between two tables
SELECT * FROM @fromtables A35 WHERE _maybe EXISTS (SELECT A._variable[#col], B.__[#col] FROM @fromtables A, @fromtables B WHERE A.__[#col] = B.__[#col] AND A.__[#col] _cmp A35.__[#col])


--- Additional tests of ANY and ALL
--- ANY / ALL with and without column expressions
SELECT * FROM @fromtables A41 WHERE _variable[#col]                 _cmp _anyorall (SELECT __[#col] FROM @fromtables SQ WHERE SQ.__[#col] _cmp A41.__[#col])
SELECT * FROM @fromtables A42 WHERE _variable[#col @comparabletype] _cmp _anyorall (SELECT _colexpr FROM @fromtables SQ WHERE SQ.__[#col] _cmp A42.__[#col])
SELECT * FROM @fromtables A43 WHERE _variable[#col @comparabletype] _cmp _anyorall (SELECT __[#col] FROM @fromtables SQ WHERE SQ._colexpr _cmp A43.__[#col])
SELECT * FROM @fromtables A44 WHERE _colexpr _cmp _anyorall (SELECT _variable[#col @comparabletype] FROM @fromtables SQ WHERE SQ.__[#col] _cmp A44.__[#col])

--- ANY / ALL with multiple columns
SELECT * FROM @fromtables A45 WHERE (_variable[#col1], _variable[#col2]) _cmp _anyorall (SELECT __[#col1], __[#col2] FROM @fromtables SQ WHERE SQ.__[#col1] _cmp A45.__[#col1])
