<configure-for-varbinary.sql>

{_explicitsortorder |= " ASC "}
{_explicitsortorder |= " DESC "}

{_bval |= "x'AB'"}
{_bval |= "x_value[varbinary]"} 
-- {_bval |= "x_value[varbinary null30]"} 

INSERT INTO _table VALUES (_value[int16], _bval, _bval, _bval, _bval, _bval, _bval)

SELECT * FROM _table WHERE A _cmp _bval ORDER BY A _explicitsortorder
SELECT * FROM _table WHERE B _cmp _bval AND C _cmp _bval ORDER BY C _explicitsortorder 
