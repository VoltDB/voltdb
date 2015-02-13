<configure-for-ints.sql>

--- Test IN/EXISTS Advanced cases
SELECT * FROM @fromtables A WHERE _variable[@columntype] _maybe IN ( SELECT MIN(_variable[@columntype]) FROM @fromtables B WHERE B._variable[@columntype] _cmp A._variable[@columntype] )
SELECT * FROM @fromtables A WHERE EXISTS ( SELECT _variable[#GB]  FROM @fromtables B GROUP BY B.__[#GB] HAVING MAX(B._variable[@columntype]) _cmp  A._variable[@columntype] )

SELECT * FROM @fromtables LHS _jointype JOIN @fromtables RHS_10 ON LHS.@idcol = RHS_10.@idcol where LHS._variable[@columntype] _maybe IN (SELECT _variable[@columntype] FROM @fromtables IN_TABLE)
SELECT 100, * FROM @fromtables A WHERE _variable[@columntype] _maybe IN ( SELECT _variable[@columntype] FROM @fromtables LHS _jointype JOIN @fromtables RHS_11 ON LHS.@idcol = RHS_11.@idcol where LHS._variable[@columntype] _cmp A._variable[@columntype] )
