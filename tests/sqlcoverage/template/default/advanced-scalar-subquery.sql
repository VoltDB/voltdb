<configure-default.sql>

-- DML: purge and regenerate random data first
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

--- Define "place-holders" used in some of the queries below
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
{_temp0grouporderbyvarlimoffhaving |= "GROUP BY __[#ord] HAVING __[#agfcn](_variable[@comparabletype]) __[#cmp] 12"}
{_temp0grouporderbyvarlimoffhaving |= "GROUP BY __[#ord] HAVING _stringagg(_variable[string])          __[#cmp] 'Z'"}

{_grouporderbyvarlimoffhaving |= "_temp0grouporderbyvarlimoffhaving"}
{_grouporderbyvarlimoffhaving |= "ORDER BY __[#ord] _sortorder _optionallimitoffset"}

--- TODO: delete these, once ENG-8234 is fixed, so these are no longer
--- needed to avoid mismatches, in cases with ORDER BY foo DESC LIMIT:
{_tempgrouporderbyvarlimoffhaving |= "_temp0grouporderbyvarlimoffhaving"}
{_tempgrouporderbyvarlimoffhaving |= "ORDER BY __[#ord]     _optionallimitoffset"}
{_tempgrouporderbyvarlimoffhaving |= "ORDER BY __[#ord] ASC _optionallimitoffset"}

-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol
--SELECT SUM(ID), SUM(NUM), SUM(RATIO) FROM @fromtables

--- Test Scalar Subquery Advanced cases

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables                                                                    ) FROM @fromtables    A1 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A2.__[#agg]                   _cmp                   __[#agg]) FROM @fromtables AS A2 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A3._variable[@comparabletype] _cmp _variable[@comparabletype]) FROM @fromtables    A3 _optionalorderbyidlimitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT _variable[#ord]         FROM @fromtables A11 WHERE __[#ord] _symbol[#cmp _cmp] (SELECT _symbol[#agfcn @agg](       __[#ord]) FROM @fromtables                                         )    _grouporderbyvarlimoffhaving
--- This specialization avoids some query forms that fall into the edge case issue described in ENG-10554.
--- TODO: address that issue, remove this customization, and revert back to the commented-out version of query A12 that uses the normal _cmp macro.
{_a12cmpsubset |= "_eqne"}
{_a12cmpsubset |= "<"}
{_a12cmpsubset |= ">"}
{_a12cmpsubset |= "<="}
---SELECT _variable[#ord numeric] FROM @fromtables A12 WHERE __[#ord] _symbol[#cmp _cmp] (SELECT _symbol[#agfcn @agg](_variable[#agg]) FROM @fromtables                                         )    _grouporderbyvarlimoffhaving
   SELECT _variable[#ord numeric] FROM @fromtables A12 WHERE __[#ord] _symbol[#cmp _a12cmpsubset] (SELECT _symbol[#agfcn @agg](_variable[#agg]) FROM @fromtables                                         )    _grouporderbyvarlimoffhaving
--- TODO: uncomment, once ENG-8292 (NPE in Hsql for HAVING query, causing sqlCoverage mismatches) is fixed
--SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#ord] _symbol[#cmp _cmp] (SELECT _symbol[#agfcn @agg](       __[#ord]) FROM @fromtables WHERE     __[#ord] __[#cmp] A13.__[#ord])    _grouporderbyvarlimoffhaving
SELECT _variable[#ord numeric] FROM @fromtables A14 WHERE __[#ord] _symbol[#cmp _cmp] (SELECT _symbol[#agfcn @agg](_variable[#agg]) FROM @fromtables WHERE A14.__[#agg] __[#cmp]     __[#agg])    _grouporderbyvarlimoffhaving

--- TODO: uncomment this, once ENG-8234 is fixed, so the mismatches disappear:
--SELECT _variable[#ord]         FROM @fromtables A15 WHERE __[#agg] _symbol[#cmp _cmp] (SELECT _symbol[#agfcn @agg](_variable[#agg]) FROM @fromtables WHERE _variable[#sub] __[#cmp] A15.__[#sub]) _grouporderbyvarlimoffhaving
--- TODO: delete this, once ENG-8234 is fixed, so the above mismatches disappear:
SELECT _variable[#ord]         FROM @fromtables A15 WHERE __[#agg] _symbol[#cmp _cmp] (SELECT _symbol[#agfcn @agg](_variable[#agg]) FROM @fromtables WHERE _variable[#sub] __[#cmp] A15.__[#sub]) _tempgrouporderbyvarlimoffhaving

--- Queries with scalar subqueries in the ORDER BY clause (these currently return errors, but probably should not - see ENG-8239)
--- TODO: uncomment out, if/when ENG-8239 is fixed (meanwhile, commented out to save execution time)
--SELECT @idcol FROM @fromtables A22 ORDER BY (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp A22.__[#agg]                  ) _sortorder _optionallimitoffset
--SELECT @idcol FROM @fromtables A23 ORDER BY (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp A23._variable[@comparabletype]) _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable)       FROM @fromtables                                                                     ) C0 FROM @fromtables A24 ORDER BY C0  _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp A25.__[#agg]                  )    FROM @fromtables A25 ORDER BY 1   _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp A26._variable[@comparabletype]) C0 FROM @fromtables A26 ORDER BY C0  _sortorder _optionallimitoffset

--- Queries with scalar subqueries in the GROUP BY clause (these work)
SELECT (SELECT _symbol[#agfcn @agg](_variable[#agg])                 FROM @fromtables WHERE _variable[@comparabletype]              _cmp  A31._variable[@comparabletype]) C0, __[#agfcn](__[#agg]) C1 FROM @fromtables A31 GROUP BY C0
SELECT (SELECT _symbol[#agfcn @agg](_variable[#agg @comparabletype]) FROM @fromtables WHERE _variable[@comparabletype] _symbol[#cmp _cmp] A32._variable[@comparabletype]) C0, __[#agfcn](__[#agg]) C1 FROM @fromtables A32 GROUP BY C0 HAVING __[#agfcn](__[#agg]) __[#cmp] 12
SELECT (SELECT _symbol[#agfcn _stringagg](_variable[#agg string])    FROM @fromtables WHERE _variable[@comparabletype] _symbol[#cmp _cmp] A33._variable[@comparabletype]) C0, __[#agfcn](__[#agg]) C1 FROM @fromtables A33 GROUP BY C0 HAVING __[#agfcn](__[#agg]) __[#cmp] 'Z'
--- These do not currently work, due to ENG-8915 (use of 'C1' alias in HAVING; meanwhile, commented out to save execution time)
--SELECT (SELECT _symbol[#agfcn @agg](_variable[#agg @comparabletype]) FROM @fromtables WHERE _variable[@comparabletype] _symbol[#cmp _cmp] A34._variable[@comparabletype]) C0, __[#agfcn](__[#agg]) C1 FROM @fromtables A34 GROUP BY C0 HAVING C1                   __[#cmp] 12
--SELECT (SELECT _symbol[#agfcn _stringagg](_variable[#agg string])    FROM @fromtables WHERE _variable[@comparabletype] _symbol[#cmp _cmp] A35._variable[@comparabletype]) C0, __[#agfcn](__[#agg]) C1 FROM @fromtables A35 GROUP BY C0 HAVING C1                   __[#cmp] 'Z'

--- Queries with a HAVING clause, but without scalar subqueries, just for comparison with the next two patterns
--SELECT _variable[#grp] C0, _symbol[#agfcn @agg](_variable[#agg]) C1 FROM @fromtables A40 GROUP BY C0 HAVING __[#agfcn](__[#agg]) _cmp 12

--- Queries with scalar subqueries in the HAVING clause
--- These do not currently work, due to ENG-8306 (meanwhile, commented out to save execution time)
--SELECT _variable[#grp] C0, _symbol[#agfcn @agg](_variable[#agg]) C1 FROM @fromtables A41 GROUP BY C0 HAVING __[#agfcn](__[#agg]) _symbol[#cmp _cmp] (SELECT __[#agfcn](__[#agg]) FROM @fromtables WHERE __[#grp] __[#cmp] A41.__[#grp])
--- This query pattern is even "worse" (harder for VoltDB): it attempts to use a subquery in
--- place of an aggregate function (in the HAVING clause), so it may not be fixed with ENG-8306
--SELECT _variable[#grp] C0, _symbol[#agfcn @agg](_variable[#agg]) C1 FROM @fromtables A42 GROUP BY C0 HAVING (SELECT __[#agfcn](_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _symbol[#cmp _cmp] A42.__[#grp]) __[#cmp] 12
