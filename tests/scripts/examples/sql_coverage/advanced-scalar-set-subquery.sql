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
{_optionalwherestr |= "WHERE __[#agg] _cmp3 Z.__[#agg]"}
{_optionalwherenum |= "_optionalwherestr"}
{_optionalwherenum |= "WHERE _variable[@comparabletype] _cmp2 Z.__[#agg]"}

{_simplequerystr |= "SELECT __[#agg] FROM @fromtables"}
{_simplequerynum |= "SELECT __[#agg] FROM @fromtables"}

{_setopquerystr  |= "_simplequerystr _setop _simplequerystr"}
{_setopquerynum  |= "_simplequerynum _setop _simplequerynum"}

{_scalarquerystr |= "SELECT _genericagg(_variable[#agg string])   FROM (_setopquerystr) AS SQ _optionalwherestr"}
{_scalarquerycount |= "SELECT     COUNT(_variable[#agg string])   FROM (_setopquerystr) AS SQ _optionalwherestr"}
{_scalarquerynum |= "SELECT @agg(_variable[#agg @comparabletype]) FROM (_setopquerynum) AS NQ _optionalwherenum"}

--- Queries with scalar subqueries containing a set subquery, in the SELECT clause
SELECT @idcol ID1, (_scalarquerystr) FROM @fromtables AS Z
SELECT @idcol ID2, (_scalarquerynum) FROM @fromtables AS Z

--- Queries with scalar subqueries containing a set subquery, in the WHERE clause
SELECT @idcol ID3,                        __[#agg] FROM @fromtables Z WHERE __[#agg] _cmp2 (_scalarquerystr)
SELECT @idcol ID4, _variable[#num @comparabletype] FROM @fromtables Z WHERE __[#num] _cmp1 (_scalarquerycount)
SELECT @idcol ID5, _variable[#num @comparabletype] FROM @fromtables Z WHERE __[#num] _cmp3 (_scalarquerynum)

