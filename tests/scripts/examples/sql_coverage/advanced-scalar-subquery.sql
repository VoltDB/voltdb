<configure-default.sql>

-- DML: purge and regenerate random data first
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

--- Define "place-holders" used in the queries below
--- comparison operators in 3 groups, to save execution time
{_cmp1 |= "="}
{_cmp1 |= "<>"}
{_cmp2 |= "<"}
{_cmp2 |= ">="}
{_cmp3 |= ">"}
{_cmp3 |= "<="}
--{_cmp3 |= "!="} -- Apparently, an HSQL-supported alias for the standard <>
{_cmp13 |= "_cmp1"}
{_cmp13 |= "_cmp3"}

{_optionaloffset |= ""}
{_optionaloffset |= "OFFSET 2"}
{_optionallimitoffset |= ""}
{_optionallimitoffset |= "LIMIT 4 _optionaloffset"}

{_optionalorderbyidlimitoffset |= ""}
{_optionalorderbyidlimitoffset |= "LIMIT 1000"}
{_optionalorderbyidlimitoffset |= "ORDER BY @idcol _sortorder _optionallimitoffset"}

{_optionalorderby1limitoffset |= ""}
{_optionalorderby1limitoffset |= "LIMIT 1000"}
{_optionalorderby1limitoffset |= "ORDER BY 1 _sortorder _optionallimitoffset"}

--- TODO: merge these with _grouporderbyvarlimoffhaving below, once ENG-8234 is fixed, so
--- these are no longer needed to avoid mismatches, in cases with ORDER BY foo DESC LIMIT:
{_temp0grouporderbyvarlimoffhaving |= ""}
{_temp0grouporderbyvarlimoffhaving |= "LIMIT 1000"}
{_temp0grouporderbyvarlimoffhaving |= "GROUP BY __[#ord]"}
{_temp0grouporderbyvarlimoffhaving |= "GROUP BY __[#ord] HAVING @agg(_variable[@comparabletype]) _cmp 12"}
{_temp0grouporderbyvarlimoffhaving |= "GROUP BY __[#ord] HAVING _genericagg(_variable[string])   _cmp 'Z'"}

{_grouporderbyvarlimoffhaving |= "_temp0grouporderbyvarlimoffhaving"}
{_grouporderbyvarlimoffhaving |= "ORDER BY __[#ord] _sortorder _optionallimitoffset"}

--- TODO: delete these, once ENG-8234 is fixed, so these are no longer
--- needed to avoid mismatches, in cases with ORDER BY foo DESC LIMIT:
{_tempgrouporderbyvarlimoffhaving |= "_temp0grouporderbyvarlimoffhaving"}
{_tempgrouporderbyvarlimoffhaving |= "ORDER BY __[#ord]     _optionallimitoffset"}
{_tempgrouporderbyvarlimoffhaving |= "ORDER BY __[#ord] ASC _optionallimitoffset"}

-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol

--- Test Scalar Subquery Advanced cases

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables                                                                     ) FROM @fromtables    A1 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A2.__[#agg]                   _cmp13                  __[#agg]) FROM @fromtables AS A2 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A3._variable[@comparabletype] _cmp2 _variable[@comparabletype]) FROM @fromtables    A3 _optionalorderbyidlimitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT _variable[#ord]         FROM @fromtables A11 WHERE __[#ord] _cmp1 (SELECT @agg(       __[#ord]) FROM @fromtables                                      ) _grouporderbyvarlimoffhaving
SELECT _variable[#ord numeric] FROM @fromtables A12 WHERE __[#ord] _cmp2 (SELECT @agg(_variable[#agg]) FROM @fromtables                                      ) _grouporderbyvarlimoffhaving
--- TODO: uncomment, once ENG-8292 (NPE in Hsql for HAVING query, causing sqlCoverage mismatches) is fixed
--SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#ord] _cmp3 (SELECT @agg(       __[#ord]) FROM @fromtables WHERE     __[#ord] _cmp3 A13.__[#ord]) _grouporderbyvarlimoffhaving
SELECT _variable[#ord numeric] FROM @fromtables A14 WHERE __[#ord] _cmp2 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A14.__[#agg] _cmp2     __[#agg]) _grouporderbyvarlimoffhaving

--- TODO: uncomment this, once ENG-8234 is fixed, so the mismatches disappear:
--SELECT _variable[#ord]         FROM @fromtables A15 WHERE __[#agg] _cmp3 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] _cmp3 A15.__[#sub]) _grouporderbyvarlimoffhaving
--- TODO: delete this, once ENG-8234 is fixed, so the above mismatches disappear:
SELECT _variable[#ord]         FROM @fromtables A15 WHERE __[#agg] _cmp3 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] _cmp3 A15.__[#sub]) _tempgrouporderbyvarlimoffhaving

--- Queries with scalar subqueries in the ORDER BY clause (these currently return errors, but probably should not - see ENG-8239)
--- TODO: uncomment out, if/when ENG-8239 is fixed (meanwhile, commented out to save execution time)
--SELECT @idcol FROM @fromtables A22 ORDER BY (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp1 A22.__[#agg]                  ) _sortorder _optionallimitoffset
--SELECT @idcol FROM @fromtables A23 ORDER BY (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp2 A23._variable[@comparabletype]) _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable)       FROM @fromtables                                                                      ) C0 FROM @fromtables A24 ORDER BY C0 _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp3 A25.__[#agg]                  )    FROM @fromtables A25 ORDER BY 1  _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp1 A26._variable[@comparabletype]) C0 FROM @fromtables A26 ORDER BY C0 _sortorder _optionallimitoffset

--- Queries with scalar subqueries in the GROUP BY or HAVING clause (these work)
SELECT (SELECT @agg(_variable[#agg])                 FROM @fromtables WHERE _variable[@comparabletype] _cmp1 A31._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A31 GROUP BY C0
SELECT (SELECT @agg(_variable[#agg @comparabletype]) FROM @fromtables WHERE _variable[@comparabletype] _cmp2 A32._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A32 GROUP BY C0 HAVING @agg(__[#agg]) _cmp 12
SELECT (SELECT _genericagg(_variable[#agg string])   FROM @fromtables WHERE _variable[@comparabletype] _cmp3 A33._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A33 GROUP BY C0 HAVING @agg(__[#agg]) _cmp 'Z'
--- These do not currently work (meanwhile, commented out to save execution time)
--SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp A34._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A34 GROUP BY C0 HAVING C1 _cmp 12
--SELECT _variable[#grp] C0, @agg(_variable[#agg]) C1  FROM @fromtables A35 GROUP BY C0 HAVING (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp A35._variable[@comparabletype]) _cmp 12
SELECT _variable[#grp] C0, @agg(_variable[#agg]) C1 FROM @fromtables A36 GROUP BY C0 HAVING @agg(__[#agg]) _cmp (SELECT @agg(__[#agg]) FROM @fromtables WHERE __[#grp] _cmp A36.__[#grp])

--- Queries with scalar subqueries containing a UNION (these ??)
-- TBD
