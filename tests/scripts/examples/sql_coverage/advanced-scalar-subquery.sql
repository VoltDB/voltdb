<configure-default.sql>

-- DML: purge and regenerate random data first
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

--- Define "place-holders" used in the queries below
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
--{_optionalorderbyvarlimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[@comparabletype]) _cmp 12"}
{_optionalorderbyvarlimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[string])          _cmp 'Z'"}

--- TODO: delete these, once ENG-8234 is fixed, so these are no longer needed to avoid mismatches:
{_tempoptionalorderbylimitoffset |= ""}
{_tempoptionalorderbylimitoffset |= "LIMIT 1000"}
{_tempoptionalorderbylimitoffset |= "ORDER BY __[#ord]     _optionallimitoffset"}
{_tempoptionalorderbylimitoffset |= "ORDER BY __[#ord] ASC _optionallimitoffset"}
{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord]"}
{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord] HAVING @agg(__[#ord])                   _cmp 12"}
--{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[@comparabletype]) _cmp 12"}
{_tempoptionalorderbylimitoffset |= "GROUP BY __[#ord] HAVING @agg(_variable[string])          _cmp 'Z'"}

-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol

--- Test Scalar Subquery Advanced cases

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables                                                                    ) FROM @fromtables    A1 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A2.__[#agg]                   _cmp1                   __[#agg]) FROM @fromtables AS A2 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A3a._variable[@comparabletype] _cmp2 _variable[@comparabletype]) FROM @fromtables AS A3a _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A3b._variable[@comparabletype] _cmp3 _variable[@comparabletype]) FROM @fromtables AS A3b _optionalorderbyidlimitoffset

--- Queries with scalar subqueries in the FROM clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables                                                                 )    A4 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp1                   __[#agg]) AS A5 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp2 _variable[@comparabletype]) AS A6a _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp3 _variable[@comparabletype]) AS A6b _optionalorderby1limitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT, OFFSET, GROUP BY or HAVING clauses)
SELECT _variable[#ord]         FROM @fromtables A7  WHERE __[#ord] _cmp1 (SELECT @agg(       __[#ord]) FROM @fromtables                                      ) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A8  WHERE __[#ord] _cmp2 (SELECT @agg(_variable[#agg]) FROM @fromtables                                      ) _optionalorderbyvarlimitoffset
SELECT _variable[#ord]         FROM @fromtables A9  WHERE __[#ord] _cmp3 (SELECT @agg(       __[#ord]) FROM @fromtables WHERE        __[#ord] _cmp3 A9.__[#ord]) _optionalorderbyvarlimitoffset
--SELECT _variable[#ord]         FROM @fromtables A10 WHERE __[#ord] _cmp (SELECT @agg(       __[#ord]) FROM @fromtables WHERE        __[#ord] <> A10.__[#ord]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A11 WHERE __[#ord] _cmp2 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE    A11.__[#agg] _cmp2      __[#agg]) _optionalorderbyvarlimitoffset
--SELECT _variable[#ord numeric] FROM @fromtables A12 WHERE __[#ord] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE    A12.__[#agg] >=     __[#agg]) _optionalorderbyvarlimitoffset

--- TODO: uncomment these, once ENG-8234 is fixed, so the mismatches disappear:
--SELECT _variable[#ord]         FROM @fromtables A13a WHERE __[#agg] _cmp3 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] _cmp3 A13a.__[#sub]) _optionalorderbyvarlimitoffset
--SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#agg] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] <= A13.__[#sub]) _optionalorderbyvarlimitoffset
--SELECT _variable[#ord]         FROM @fromtables A14 WHERE __[#agg] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] >  A14.__[#sub]) _optionalorderbyvarlimitoffset
--- TODO: delete these, once ENG-8234 is fixed, so the above mismatches disappear:
SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#agg] _cmp3 (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] _cmp3 A13.__[#sub]) _tempoptionalorderbylimitoffset
--SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#agg] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] <= A13.__[#sub]) _tempoptionalorderbylimitoffset
--SELECT _variable[#ord]         FROM @fromtables A14 WHERE __[#agg] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] >  A14.__[#sub]) _tempoptionalorderbylimitoffset

--- Queries with scalar subqueries in the ORDER BY clause (these currently return errors, but probably should not)
SELECT @idcol FROM @fromtables A15 ORDER BY (SELECT @agg(_variable)       FROM @fromtables                                                                     ) _sortorder _optionallimitoffset
SELECT @idcol FROM @fromtables A16 ORDER BY (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp1 A16.__[#agg]                  ) _sortorder _optionallimitoffset
SELECT @idcol FROM @fromtables A17 ORDER BY (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp2 A17._variable[@comparabletype]) _sortorder _optionallimitoffset
SELECT (SELECT @agg(_variable)       FROM @fromtables                                                                     ) C0 FROM @fromtables A18 ORDER BY C0 _sortorder _optionallimitoffset
SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp3 A19.__[#agg]                  )    FROM @fromtables A19 ORDER BY 1  _sortorder _optionallimitoffset
SELECT (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp1 A20._variable[@comparabletype]) C0 FROM @fromtables A20 ORDER BY C0 _sortorder _optionallimitoffset

--- Queries with scalar subqueries in the LIMIT or OFFSET clause (these should, and do, return errors)
SELECT _variable AS C0 FROM @fromtables A21 ORDER BY C0 _sortorder LIMIT (SELECT @agg(_variable) FROM @fromtables) _optionaloffset
SELECT _variable AS C0 FROM @fromtables A22 ORDER BY C0 _sortorder LIMIT 3 OFFSET (SELECT @agg(_variable) FROM @fromtables)


--- Queries with scalar subqueries in the GROUP BY or HAVING clause (these ???)
SELECT (SELECT @agg(_variable[#agg])                 FROM @fromtables WHERE _variable[@comparabletype] _cmp1 A23._variable[@comparabletype]) C0, @agg(__[#agg]) AS C1 FROM @fromtables A23 GROUP BY C0
SELECT (SELECT @agg(_variable[#agg @comparabletype]) FROM @fromtables WHERE _variable[@comparabletype] _cmp2 A25a._variable[@comparabletype]) C0, @agg(__[#agg]) AS C1 FROM @fromtables A25a GROUP BY C0 HAVING @agg(__[#agg]) _cmp 12
SELECT (SELECT @agg(_variable[#agg] string         ) FROM @fromtables WHERE _variable[@comparabletype] _cmp3 A25b._variable[@comparabletype]) C0, @agg(__[#agg]) AS C1 FROM @fromtables A25b GROUP BY C0 HAVING @agg(__[#agg]) _cmp 'Z'
--- These do not currently work
--SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp A24._variable[@comparabletype]) C0, @agg(__[#agg]) AS C1 FROM @fromtables A24 GROUP BY C0 HAVING C1 _cmp 12
--SELECT _variable[#grp] AS C0, @agg(_variable[#agg]) AS C1 FROM @fromtables A26 GROUP BY C0 HAVING (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[@comparabletype] _cmp A26._variable[@comparabletype]) _cmp 12
