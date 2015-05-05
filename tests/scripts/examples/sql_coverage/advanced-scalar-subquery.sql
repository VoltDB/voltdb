<configure-default.sql>

-- DML: purge and regenerate random data first
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

--- Define "place-holders" used in the queries below
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
{_optionalorderbyvarlimitoffset |= ""}
{_optionalorderbyvarlimitoffset |= "LIMIT 1000"}
{_optionalorderbyvarlimitoffset |= "ORDER BY __[#ord] _sortorder _optionallimitoffset"}


-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol


--- Test Scalar Subquery Advanced cases

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT @idcol, (SELECT @agg(_variable) FROM @fromtables                                                                          ) FROM @fromtables    A1 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE A2.__[#agg]                   _cmp                   __[#agg]) FROM @fromtables AS A2 _optionalorderbyidlimitoffset
SELECT @idcol, (SELECT @agg(_variable)       FROM @fromtables WHERE A3._variable[@comparabletype] _cmp _variable[@comparabletype]) FROM @fromtables AS A3 _optionalorderbyidlimitoffset

--- Queries with scalar subqueries in the FROM clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT * FROM (SELECT @agg(_variable)       FROM @fromtables                                                                 )    A4 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp                   __[#agg]) AS A5 _optionalorderby1limitoffset
SELECT * FROM (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp _variable[@comparabletype]) AS A6 _optionalorderby1limitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT _variable[#ord]         FROM @fromtables A7  WHERE __[#ord] _cmp (SELECT @agg(       __[#ord]) FROM @fromtables                                      ) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A8  WHERE __[#ord] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables                                      ) _optionalorderbyvarlimitoffset
SELECT _variable[#ord]         FROM @fromtables A9  WHERE __[#ord] _cmp (SELECT @agg(       __[#ord]) FROM @fromtables WHERE        __[#ord] =   A9.__[#ord]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord]         FROM @fromtables A10 WHERE __[#ord] _cmp (SELECT @agg(       __[#ord]) FROM @fromtables WHERE        __[#ord] <> A10.__[#ord]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A11 WHERE __[#ord] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE    A11.__[#agg] <      __[#agg]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord numeric] FROM @fromtables A12 WHERE __[#ord] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE    A12.__[#agg] >=     __[#agg]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord]         FROM @fromtables A13 WHERE __[#agg] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] <= A13.__[#sub]) _optionalorderbyvarlimitoffset
SELECT _variable[#ord]         FROM @fromtables A14 WHERE __[#agg] _cmp (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE _variable[#sub] >  A14.__[#sub]) _optionalorderbyvarlimitoffset

--- Queries with scalar subqueries in the ORDER BY clause (these currently return errors)
SELECT @idcol FROM @fromtables A15 ORDER BY (SELECT @agg(_variable)       FROM @fromtables                                                                     ) _sortorder _optionallimitoffset
SELECT @idcol FROM @fromtables A16 ORDER BY (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp A16.__[#agg]                  ) _sortorder _optionallimitoffset
SELECT @idcol FROM @fromtables A17 ORDER BY (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp A17._variable[@comparabletype]) _sortorder _optionallimitoffset
SELECT (SELECT @agg(_variable)       FROM @fromtables                                                                     ) C0 FROM @fromtables A18 ORDER BY C0 _sortorder _optionallimitoffset
SELECT (SELECT @agg(_variable[#agg]) FROM @fromtables WHERE __[#agg]                   _cmp A19.__[#agg]                  )    FROM @fromtables A19 ORDER BY 1  _sortorder _optionallimitoffset
SELECT (SELECT @agg(_variable)       FROM @fromtables WHERE _variable[@comparabletype] _cmp A20._variable[@comparabletype]) C0 FROM @fromtables A20 ORDER BY C0 _sortorder _optionallimitoffset

--- Queries with scalar subqueries in the LIMIT or OFFSET clause (these should, and do, return errors)
SELECT _variable AS C0 FROM @fromtables A21 ORDER BY C0 _sortorder LIMIT (SELECT @agg(_variable) FROM @fromtables) _optionaloffset
SELECT _variable AS C0 FROM @fromtables A22 ORDER BY C0 _sortorder LIMIT 3 OFFSET (SELECT @agg(_variable) FROM @fromtables)
