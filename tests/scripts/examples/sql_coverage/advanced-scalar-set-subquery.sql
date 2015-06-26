<configure-default.sql>

-- DML: purge and regenerate random data first
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- DML: copy some of the data, so that INTERSECT is not always null, between different tables
INSERT INTO P1 (SELECT * FROM R1 WHERE ID > 13)
INSERT INTO P1 (SELECT * FROM R2 WHERE ID > 21)
INSERT INTO R2 (SELECT * FROM R1 WHERE ID < 10)

--- Define "place-holders" used in the queries below:
--- Comparison operators in 3 groups, to save execution time
{_cmp1 |= "="}
{_cmp1 |= "<>"}
{_cmp2 |= "<"}
{_cmp2 |= ">="}
{_cmp3 |= ">"}
{_cmp3 |= "<="}

-- TEMP, for debugging, just so I can quickly see what data was generated:
--SELECT * FROM @fromtables ORDER BY @idcol
--SELECT SUM(ID), SUM(NUM), SUM(RATIO) FROM @fromtables

--- Queries with scalar subqueries containing a set (UNION, INTERSECT, EXCEPT [ALL]) query
{_optionalwherestr |= ""}
{_optionalwherestr |= "WHERE __[#str] _cmp3 Z.__[#str]"}
{_optionalwherenum |= ""}
{_optionalwherenum |= "WHERE __[#num] _cmp3 Z.__[#num]"}
{_optionalwherenum |= "WHERE _variable[@comparabletype] _cmp2 Z.__[#num]"}

{_simplequerystr |= "SELECT __[#str] FROM @fromtables"}
{_simplequerynum |= "SELECT __[#num] FROM @fromtables"}

{_wherequerystr |= "_simplequerystr WHERE __[#str] _cmp1 Z.__[#str]"}
{_wherequerynum |= "_simplequerynum WHERE __[#num] _cmp1 Z.__[#num]"}

{_setopquerystr |= "_simplequerystr _setop _simplequerystr"}
{_setopquerynum |= "_simplequerynum _setop _simplequerynum"}

{_setopwherequerystr |= "_wherequerystr _setop _wherequerystr"}
{_setopwherequerynum |= "_wherequerynum _setop _wherequerynum"}

{_scalarquerystr |= "SELECT _genericagg(_variable[#str string])   FROM (_setopquerystr) AS SQ _optionalwherestr"}
{_scalarquerycount |= "SELECT     COUNT(_variable[#str string])   FROM (_setopquerystr) AS SQ _optionalwherestr"}
{_scalarquerynum |= "SELECT @agg(_variable[#num @comparabletype]) FROM (_setopquerynum) AS NQ _optionalwherenum"}

--- Queries with scalar subqueries containing a set subquery, in the SELECT clause
SELECT @idcol ID1, (_scalarquerystr) FROM @fromtables AS Z
SELECT @idcol ID2, (_scalarquerynum) FROM @fromtables AS Z

--- Queries with scalar subqueries containing a set subquery, in the WHERE clause
SELECT @idcol ID3,                         __[#str] FROM @fromtables Z WHERE __[#str]  _cmp2 (_scalarquerystr)
SELECT @idcol ID4, _variable[#num  @comparabletype] FROM @fromtables Z WHERE __[#num]  _cmp1 (_scalarquerycount)
SELECT @idcol ID5, _variable[#num2 @comparabletype] FROM @fromtables Z WHERE __[#num2] _cmp3 (_scalarquerynum)

--- Queries with scalar subqueries containing a set operator, in the WHERE clause (directly, without an additional subquery)
SELECT @idcol ID6, _variable[#str string]          FROM @fromtables Z WHERE __[#str] _cmp2 (_setopwherequerystr)

SELECT @idcol ID7, _variable[#num @comparabletype] FROM @fromtables Z WHERE __[#num] _cmp3 (_setopwherequerynum)
