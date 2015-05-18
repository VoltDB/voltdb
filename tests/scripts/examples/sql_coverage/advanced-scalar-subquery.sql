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

{_optionaloffset |= ""}
{_optionaloffset |= "OFFSET 2"}
{_optionallimitoffset |= ""}
{_optionallimitoffset |= "LIMIT 4 _optionaloffset"}

{_optionalorderbyidlimitoffset |= ""}
{_optionalorderbyidlimitoffset |= "LIMIT 1000"}
{_optionalorderbyidlimitoffset |= "ORDER BY @idcol _sortorder _optionallimitoffset"}
{_optionalorderbyidlimitoffset |= "GROUP BY @idcol"}
{_optionalorderbyidlimitoffset |= "GROUP BY @idcol HAVING @agg(__[#agg])                   _cmp 12"}
--- commented out, to save execution time
--{_optionalorderbyidlimitoffset |= "GROUP BY @idcol HAVING @agg(@idcol)                     _cmp 12"}
--{_optionalorderbyidlimitoffset |= "GROUP BY @idcol HAVING @agg(_variable[@comparabletype]) _cmp 12"}
{_optionalorderbyidlimitoffset |= "GROUP BY @idcol HAVING @agg(_variable[string])          _cmp 'Z'"}

{_optionalorderby1limitoffset |= ""}
{_optionalorderby1limitoffset |= "LIMIT 1000"}
{_optionalorderby1limitoffset |= "ORDER BY 1 _sortorder _optionallimitoffset"}
--- GROUP BY 1 does work (in the same way as ORDER BY 1)
--{_optionalorderby1limitoffset |= "GROUP BY 1"}
--{_optionalorderby1limitoffset |= "GROUP BY 1 HAVING @agg(__[#agg])                   _cmp 12"}
--{_optionalorderby1limitoffset |= "GROUP BY 1 HAVING @agg(_variable[@comparabletype]) _cmp 12"}
--{_optionalorderby1limitoffset |= "GROUP BY 1 HAVING @agg(_variable[string])          _cmp 'Z'"}

{_optionalorderbyvarlimitoffset |= ""}
{_optionalorderbyvarlimitoffset |= "LIMIT 1000"}
{_optionalorderbyvarlimitoffset |= "ORDER BY __[#ord] _sortorder _optionallimitoffset"}
{_optionalorderbyvarlimitoffset |= "GROUP BY __[#ord]"}
{_optionalorderbyvarlimitoffset |= "GROUP BY __[#ord] HAVING @agg(__[#ord])                   _cmp 12"}
--- commented out, to save execution time
--{_optionalorderbyvarlimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[@comparabletype]) _cmp 12"}
{_optionalorderbyvarlimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[string])          _cmp 'Z'"}

--- TODO: delete these, once ENG-8234 is fixed, so these are no longer needed to avoid mismatches:
{_tempoptionalorderbylimitoffset |= ""}
{_tempoptionalorderbylimitoffset |= "LIMIT 1000"}
{_tempoptionalorderbylimitoffset |= "ORDER BY __[#ord]     _optionallimitoffset"}
{_tempoptionalorderbylimitoffset |= "ORDER BY __[#ord] ASC _optionallimitoffset"}
{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord]"}
{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord] HAVING @agg(__[#ord])                   _cmp 12"}
--- commented out, to save execution time
--{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[@comparabletype]) _cmp 12"}
{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[string])          _cmp 'Z'"}

-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol

--- Test Scalar Subquery Advanced cases

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables                                                                     ) FROM @fromtables    A1 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A2.__[#agg]                   _cmp1                   __[#agg]) FROM @fromtables AS A2 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A3._variable[@comparabletype] _cmp2 _variable[@comparabletype]) FROM @fromtables    A3 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A4._variable[@comparabletype] _cmp3 _variable[@comparabletype]) FROM @fromtables AS A4 _optionalorderbyidlimitoffset

--- Queries with scalar subqueries in the FROM clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables                                                                  )    A5 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp1                   __[#agg]) AS A6 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp2 _variable[@comparabletype])    A7 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp3 _variable[@comparabletype]) AS A8 _optionalorderby1limitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT _variable[#ord]         FROM @fromtables A11 WHERE __[#ord] _cmp1 (SELECT @agg(       __[#ord]) FROM @fromtables                                      ) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A12 WHERE __[#ord] _cmp2 (SELECT @agg(_variable[#agg]) FROM @fromtables                                      ) _optionalorderbyvarlimitoffset
SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#ord] _cmp3 (SELECT @agg(       __[#ord]) FROM @fromtables WHERE     __[#ord] _cmp3 A13.__[#ord]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A14 WHERE __[#ord] _cmp2 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A14.__[#agg] _cmp2     __[#agg]) _optionalorderbyvarlimitoffset

--- TODO: uncomment this, once ENG-8234 is fixed, so the mismatches disappear:
--SELECT _variable[#ord]         FROM @fromtables A15 WHERE __[#agg] _cmp3 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] _cmp3 A15.__[#sub]) _optionalorderbyvarlimitoffset
--- TODO: delete this, once ENG-8234 is fixed, so the above mismatches disappear:
SELECT _variable[#ord]         FROM @fromtables A15 WHERE __[#agg] _cmp3 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] _cmp3 A15.__[#sub]) _tempoptionalorderbylimitoffset

--- Queries with scalar subqueries in the ORDER BY clause (these currently return errors, but probably should not - see ENG-8239)
--- TODO: uncomment out, if/when ENG-8239 is fixed (meanwhile, commented out to save execution time)
--SELECT @idcol FROM @fromtables A21 ORDER BY (SELECT @agg(_variable)       FROM @fromtables                                                                      ) _sortorder _optionallimitoffset
--SELECT @idcol FROM @fromtables A22 ORDER BY (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp1 A22.__[#agg]                  ) _sortorder _optionallimitoffset
--SELECT @idcol FROM @fromtables A23 ORDER BY (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp2 A23._variable[@comparabletype]) _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable)       FROM @fromtables                                                                      ) C0 FROM @fromtables A24 ORDER BY C0 _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp3 A25.__[#agg]                  )    FROM @fromtables A25 ORDER BY 1  _sortorder _optionallimitoffset
--SELECT (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp1 A26._variable[@comparabletype]) C0 FROM @fromtables A26 ORDER BY C0 _sortorder _optionallimitoffset

--- Queries with scalar subqueries in the LIMIT or OFFSET clause (these should, and do, return errors)
--- TODO: uncomment out, if/when scalar subquery supported in LIMIT, OFFSET clauses (meanwhile, commented out to save execution time)
--SELECT _variable AS C0 FROM @fromtables A27 ORDER BY C0 _sortorder LIMIT (SELECT @agg(_variable) FROM @fromtables) _optionaloffset
--SELECT _variable AS C0 FROM @fromtables A28 ORDER BY C0 _sortorder LIMIT 3 OFFSET (SELECT @agg(_variable) FROM @fromtables)

--- Queries with scalar subqueries in the GROUP BY or HAVING clause (these work)
SELECT (SELECT @agg(_variable[#agg])                 FROM @fromtables WHERE _variable[@comparabletype] _cmp1 A31._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A31 GROUP BY C0
SELECT (SELECT @agg(_variable[#agg @comparabletype]) FROM @fromtables WHERE _variable[@comparabletype] _cmp2 A32._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A32 GROUP BY C0 HAVING @agg(__[#agg]) _cmp 12
SELECT (SELECT @agg(_variable[#agg] string)          FROM @fromtables WHERE _variable[@comparabletype] _cmp3 A33._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A33 GROUP BY C0 HAVING @agg(__[#agg]) _cmp 'Z'
--- These do not currently work (meanwhile, commented out to save execution time)
--SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp A34._variable[@comparabletype]) C0, @agg(__[#agg]) C1 FROM @fromtables A34 GROUP BY C0 HAVING C1 _cmp 12
--SELECT _variable[#grp] C0, @agg(_variable[#agg]) C1  FROM @fromtables A35 GROUP BY C0 HAVING (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp A35._variable[@comparabletype]) _cmp 12
SELECT _variable[#grp] C0, @agg(_variable[#agg]) C1 FROM @fromtables A36 GROUP BY C0 HAVING @agg(__[#agg]) _cmp (SELECT @agg(__[#agg]) FROM @fromtables WHERE __[#grp] _cmp A36.__[#grp])

--- Queries with a simple UNION of one or more scalar subqueries (these work)
SELECT @agg(_variable)         FROM @fromtables UNION SELECT -1  FROM @fromtables A41
SELECT @agg(_variable[string]) FROM @fromtables UNION SELECT 'Z' FROM @fromtables A42
SELECT -2  FROM @fromtables UNION SELECT @agg(_variable)         FROM @fromtables A43
SELECT 'Z' FROM @fromtables UNION SELECT @agg(_variable[string]) FROM @fromtables A44
SELECT @agg(_variable)         FROM @fromtables UNION SELECT @agg(_variable)         FROM @fromtables A45
SELECT @agg(_variable[string]) FROM @fromtables UNION SELECT @agg(_variable[string]) FROM @fromtables A46

--- Queries with scalar subqueries containing a UNION (these ??)
-- TBD
