<configure-default.sql>

-- DML: purge and regenerate random data first
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

--- Define "place-holders" used in the queries below
{_optionalascdesc |= ""}
{_optionalascdesc |= "ASC"}
{_optionalascdesc |= "DESC"}
{_optionaloffset |= ""}
{_optionaloffset |= "OFFSET 2"}
{_optionallimitoffset |= ""}
{_optionallimitoffset |= "LIMIT 4"}
{_optionallimitoffset |= "LIMIT 4 _optionaloffset"}
{_optionaloderbylimitoffset |= ""}
{_optionaloderbylimitoffset |= "LIMIT 1000"}
{_optionaloderbylimitoffset |= "ORDER BY C0 _optionalascdesc _optionallimitoffset"}
{_optionaloderby1limitoffset |= ""}
{_optionaloderby1limitoffset |= "LIMIT 1000"}
{_optionaloderby1limitoffset |= "ORDER BY 1 _optionalascdesc _optionallimitoffset"}
{_optionaloderby2limitoffset |= ""}
{_optionaloderby2limitoffset |= "LIMIT 1000"}
{_optionaloderby2limitoffset |= "ORDER BY 1 _optionalascdesc, 2 _optionalascdesc _optionallimitoffset"}


-- TEMP, for debugging, just so I can quickly see what data was generated:
SELECT * FROM @fromtables ORDER BY @idcol


--- Test Scalar Subquery Advanced cases

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT _variable, (SELECT @agg(_variable) FROM @fromtables                                                                    ) FROM @fromtables    A1 _optionaloderby2limitoffset
SELECT _variable, (SELECT @agg(_variable) FROM @fromtables WHERE A2._variable[@comparabletype] _cmp _variable[@comparabletype]) FROM @fromtables AS A2 _optionaloderby2limitoffset

--- Queries with scalar subqueries in the FROM clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT * FROM (SELECT @agg(_variable) FROM @fromtables                                                                 )    A3 _optionaloderby1limitoffset
SELECT * FROM (SELECT @agg(_variable) FROM @fromtables WHERE _variable[@comparabletype] _cmp _variable[@comparabletype]) AS A4 _optionaloderby1limitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT _variable AS C0 FROM @fromtables A5 WHERE _variable _cmp (SELECT @agg(_variable) FROM @fromtables                                                                    ) _optionaloderbylimitoffset
-- TODO: this one takes too long (when it produces anything); may try to come up with a simpler, quicker version:
SELECT _variable AS C0 FROM @fromtables A6 WHERE _variable _cmp (SELECT @agg(_variable) FROM @fromtables WHERE _variable[@comparabletype] _cmp A6._variable[@comparabletype]) _optionaloderbylimitoffset

--- Queries with scalar subqueries in the ORDER BY, LIMIT or OFFSET clause (these return errors)
SELECT (SELECT @agg(_variable) FROM @fromtables) C0 FROM @fromtables A7 ORDER BY C0 _optionalascdesc _optionallimitoffset
SELECT _variable AS C0 FROM @fromtables A8 ORDER BY C0 _optionalascdesc LIMIT (SELECT @agg(_variable) FROM @fromtables) _optionaloffset
SELECT _variable AS C0 FROM @fromtables A9 ORDER BY C0 _optionalascdesc LIMIT 3 OFFSET (SELECT @agg(_variable) FROM @fromtables)
