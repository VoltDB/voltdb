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
{_optionallimitoffset |= "LIMIT 3"}
{_optionallimitoffset |= "LIMIT 3 _optionaloffset"}
{_optionaloderbylimitoffset |= "_optionallimitoffset"}
-- TODO: uncomment this, once ORDER BY no longer crashes the server...
{_optionaloderbylimitoffset |= " ORDER BY _optionalascdesc _optionallimitoffset"}
{_optionalwhere |= ""}
{_optionalwhere |= "_variable _cmp T._variable"}


-- TEMP, for debugging, just so I can quickly see what data was generated:
SELECT * FROM @fromtables


--- Test Scalar Subquery Advanced cases
-- TODO: uncomment all of these, once they don't crash the server...

--- Queries with scalar subqueries in the SELECT clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT *, (SELECT @agg(_variable) FROM @fromtables                                                                    ) FROM @fromtables    A1 _optionaloderbylimitoffset
SELECT *, (SELECT @agg(_variable) FROM @fromtables WHERE A2._variable[@comparabletype] _cmp _variable[@comparabletype]) FROM @fromtables AS A2 _optionaloderbylimitoffset

--- Queries with scalar subqueries in the FROM clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT * FROM (SELECT @agg(_variable) FROM @fromtables                                                                 )    A3 _optionaloderbylimitoffset
SELECT * FROM (SELECT @agg(_variable) FROM @fromtables WHERE _variable[@comparabletype] _cmp _variable[@comparabletype]) AS A4 _optionaloderbylimitoffset

--- Queries with scalar subqueries in the WHERE clause (with optional ORDER BY, LIMIT or OFFSET clauses)
SELECT * FROM @fromtables A5 WHERE _variable _cmp (SELECT @agg(_variable) FROM @fromtables                                                                    ) _optionaloderbylimitoffset
-- TODO: this one takes too long; may try to come up with a quicker version:
--SELECT * FROM @fromtables A6 WHERE _variable _cmp (SELECT @agg(_variable) FROM @fromtables WHERE _variable[@comparabletype] _cmp A6._variable[@comparabletype]) _optionaloderbylimitoffset

--- Queries with scalar subqueries in the ORDER BY clause (does this make sense??)
SELECT @idcol, (SELECT @agg(_variable) FROM @fromtables                                                                    ) C7 FROM @fromtables A7 ORDER BY C7 _optionalascdesc _optionallimitoffset
SELECT @idcol, (SELECT @agg(_variable) FROM @fromtables WHERE A8._variable[@comparabletype] _cmp _variable[@comparabletype]) C8 FROM @fromtables A8 ORDER BY C8 _optionalascdesc _optionallimitoffset

--- Queries with scalar subqueries in the LIMIT clause
SELECT @idcol FROM @fromtables A9                                 LIMIT (SELECT @agg(_variable) FROM @fromtables                                                                 ) _optionaloffset
SELECT @idcol FROM @fromtables A10                                LIMIT (SELECT @agg(_variable) FROM @fromtables WHERE _variable[@comparabletype] _cmp _variable[@comparabletype]) _optionaloffset
SELECT * FROM @fromtables A11 ORDER BY _variable _optionalascdesc LIMIT (SELECT @agg(_variable) FROM @fromtables                                                                 ) _optionaloffset
SELECT * FROM @fromtables A12 ORDER BY _variable _optionalascdesc LIMIT (SELECT @agg(_variable) FROM @fromtables WHERE _variable[@comparabletype] _cmp _variable[@comparabletype]) _optionaloffset

--- Queries with scalar subqueries in the OFFSET clause
SELECT @idcol FROM @fromtables A13                                     LIMIT _value[int:1,3] OFFSET (SELECT @agg(_variable) FROM @fromtables)
SELECT @idcol FROM @fromtables A14                                     LIMIT _value[int:1,3] OFFSET (SELECT @agg(_variable) FROM @fromtables WHERE _variable _cmp _variable)
SELECT @idcol FROM @fromtables A15 ORDER BY _variable _optionalascdesc LIMIT _value[int:1,3] OFFSET (SELECT @agg(_variable) FROM @fromtables)
SELECT @idcol FROM @fromtables A16 ORDER BY _variable _optionalascdesc LIMIT _value[int:1,3] OFFSET (SELECT @agg(_variable) FROM @fromtables WHERE _variable _cmp _variable)

