<grammar.sql>

{_subqueryform |= "(select * from _table)"}
{_subqueryform |= "(select     _variable[#numone float],                  _variable[#arg numeric],             _variable[#string string]             from _table[#self] order by __[#string], __[#arg], __[#numone] limit 1 offset 1)"}
{_subqueryform |= "(select   X._variable[#numone float]  __[#numone],   X._variable[#arg numeric]  __[#arg], X._variable[#string string] __[#string] from _table[#self] X _jointype join __[#self] Y on X.VCHAR = Y.VCHAR where Y._numericcolumnpredicate)"}
{_subqueryform |= "(select max(_variable[#numone float]) __[#numone], sum(_variable[#arg numeric]) __[#arg],   _variable[#string string]             from _table[#self] group by __[#string])"}
{_subqueryform |= "(select max(_variable[#numone float]) __[#numone], sum(_variable[#arg numeric]) __[#arg],   _variable[#string string]             from _table[#self] group by __[#string] order by __[#string] limit 1 offset 1)"}


-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"} 
{@agg = "_numagg"}
--- VCHAR is a varchar partition column
{@columnpredicate = "A._variable[#string string] = B.VCHAR"}
{@columntype = "numeric"}
{@comparableconstant = "44"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] _cmp _value[int16]"}
{@dmltable = "_table"}
-- {@fromtables = "_subqueryform B, _table"}
{@fromtables = "_table B, _subqueryform "}
{@idcol = "ID"}
{@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
{@numcol = "NUM"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@updatecolumn = "NUM"}
{@updatesource = "ID"}
{@updatevalue = "_value[int:0,100]"}

{@jointype = "_jointype"}


DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (_id, 'VoltDB', 10, 500.0)
INSERT INTO @dmltable VALUES (_id, 'VoltDB', 10, 500.0)
INSERT INTO @dmltable VALUES (_id, 'NewSQL', 10, 500.0)
INSERT INTO @dmltable VALUES (_id, 'NewSQL', 10, 500.0)

SELECT 1, * FROM @fromtables A WHERE @columnpredicate
SELECT A._variable[#arg @columntype] AS Q5 FROM @fromtables A WHERE @columnpredicate ORDER BY 1 LIMIT _value[int:1,100] OFFSET _value[int:1,100]

-- additional aggregation fun
SELECT     @agg(                                          A._variable[#arg @columntype]     )                                                   AS Q16 FROM @fromtables A WHERE @columnpredicate

-- These test that the fixed issue eng-909 -- combining DISTINCT and non-DISTINCT aggs has not regressed.
SELECT     _distinctableagg(DISTINCT                     A._variable[#arg @columntype]      ), @agg(            A._variable[#arg @columntype] ) AS Q18 FROM @fromtables A WHERE @columnpredicate
SELECT 20,                                               A._variable[#GB @columntype]   ,      @agg(            A._variable[#arg @columntype] )        FROM @fromtables A WHERE @columnpredicate GROUP BY         A.__[#GB]
SELECT 21,                           A._variable[#GB @columntype] ,                            @agg(            A._variable[#arg @columntype] )        FROM @fromtables A WHERE @columnpredicate GROUP BY         A.__[#GB]   ORDER BY 2       LIMIT _value[int:1,100] OFFSET _value[int:1,100]

SELECT     @agg(                     A._variable[#arg @columntype]     ), COUNT(*)                                         AS Q24 FROM @fromtables A WHERE @columnpredicate

