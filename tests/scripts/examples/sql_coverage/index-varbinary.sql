<configure-for-varbinary.sql>

{_explicitsortorder |= " ASC "}
{_explicitsortorder |= " DESC "}

{_bval |= "'AB'"}
{_bval |= "_value[varbinary]"} 
-- {_bval |= "_value[varbinary null30]"} 

INSERT INTO _table VALUES (_value[int16], _bval, _bval, _bval, _bval, _bval, _bval)

SELECT * FROM _table WHERE A _cmp x_bval ORDER BY A _explicitsortorder
SELECT * FROM _table WHERE B _cmp x_bval AND C _cmp x_bval ORDER BY C _explicitsortorder 
