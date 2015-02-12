<configure-for-ints.sql>
<advanced-template.sql>

--- Test IN/EXISTS Advanced cases
SELECT * FROM @fromtables A WHERE _variable[@columntype] _maybe IN ( SELECT MIN(_variable[@columntype]) FROM @fromtables B WHERE B._variable[@columntype] _cmp A._variable[@columntype] )
SELECT * FROM @fromtables A WHERE EXISTS ( SELECT _variable[#GB]  FROM @fromtables B GROUP BY B.__[#GB] HAVING MAX(B._variable[@columntype]) _cmp  A._variable[@columntype] )

{@jointype = "_jointype"}
SELECT * FROM @fromtables LHS @jointype JOIN @fromtables B25RHS ON LHS.@idcol = B25RHS.@idcol where LHS._variable[@columntype] NOT IN (SELECT _variable[@columntype] FROM @fromtables IN_TABLE)
