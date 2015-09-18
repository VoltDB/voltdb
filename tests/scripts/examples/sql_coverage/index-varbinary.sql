<configure-for-varbinary.sql>

{_explicitsortorder |= " ASC "}
{_explicitsortorder |= " DESC "}

{_bval |= "'AB'"}
{_bval |= "_value[varbinary]"} 
-- {_bval |= "_value[varbinary null30]"} 

INSERT INTO _table VALUES (_value[int16], _bval, _bval, _bval, 'AB', 'AB')

SELECT * FROM _table WHERE A _cmp x_bval ORDER BY A _explicitsortorder

SELECT * FROM _table WHERE B _cmp x_bval AND C _cmp x_bval ORDER BY C _explicitsortorder 
-- SELECT * FROM _table WHERE D _cmp _dvalue AND E _cmp _value[int16] ORDER BY C _explicitsortorder 
-- SELECT * FROM _table WHERE D _cmp _dvalue AND E _cmp _value[int16] AND F _cmp _value[int16] ORDER BY E _explicitsortorder 
-- 
--- (1) Test reverse scan one column case:
SELECT * FROM _table WHERE A < x_bval
SELECT * FROM _table WHERE A <= x_bval
SELECT * FROM _table WHERE A > x_bval ORDER BY A DESC
SELECT * FROM _table WHERE A >= x_bval ORDER BY A DESC

-- (1.1) test between:
SELECT * FROM _table WHERE A > x_bval AND A < x_bval ORDER BY A DESC
SELECT * FROM _table WHERE A > x_bval AND A <= x_bval ORDER BY A DESC
SELECT * FROM _table WHERE A >= x_bval AND A < x_bval ORDER BY A DESC
SELECT * FROM _table WHERE A >= x_bval AND A <= x_bval ORDER BY A DESC
-- 
-- 
-- --- (2) Test reverse scan two columns case:
-- SELECT * FROM _table WHERE B = _bval ORDER BY C DESC 
-- 
-- --- (2.1) B = ? AND C
-- SELECT * FROM _table WHERE B = _bval AND C _cmp _value[int16] ORDER BY C DESC 
-- SELECT * FROM _table WHERE B = _bval AND C > _value[int16] AND C < _value[int16] ORDER BY C DESC 
-- SELECT * FROM _table WHERE B = _bval AND C > _value[int16] AND C <= _value[int16] ORDER BY C DESC 
-- SELECT * FROM _table WHERE B = _bval AND C >= _value[int16] AND C < _value[int16] ORDER BY C DESC 
-- SELECT * FROM _table WHERE B = _bval AND C >= _value[int16] AND C <= _value[int16] ORDER BY C DESC 
-- 
-- --- (2.2) B < ? AND C
-- SELECT * FROM _table WHERE B < _value[int16] AND C _cmp _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B < _value[int16] AND C > _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B < _value[int16] AND C > _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B < _value[int16] AND C >= _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B < _value[int16] AND C >= _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 
-- 
-- --- (2.3) B <= ? AND C
-- SELECT * FROM _table WHERE B <= _value[int16] AND C _cmp _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B <= _value[int16] AND C > _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B <= _value[int16] AND C > _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B <= _value[int16] AND C >= _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
-- SELECT * FROM _table WHERE B <= _value[int16] AND C >= _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 
-- 
-- 
-- --- (3) Test reverse scan three columns case:
-- SELECT * FROM _table WHERE D = _dvalue ORDER BY E DESC
-- SELECT * FROM _table WHERE D = _dvalue ORDER BY E DESC, F DESC
-- SELECT * FROM _table WHERE D = _dvalue AND E _cmp _value[int16] ORDER BY E DESC
-- 
-- --- (3.1) D = ? AND E ?
-- SELECT * FROM _table WHERE D = _dvalue AND E _cmp _value[int16] ORDER BY E DESC 
-- SELECT * FROM _table WHERE D = _dvalue AND E > _value[int16] AND E < _value[int16] ORDER BY E DESC 
-- SELECT * FROM _table WHERE D = _dvalue AND E > _value[int16] AND E <= _value[int16] ORDER BY E DESC 
-- SELECT * FROM _table WHERE D = _dvalue AND E >= _value[int16] AND E < _value[int16] ORDER BY E DESC 
-- SELECT * FROM _table WHERE D = _dvalue AND E >= _value[int16] AND E <= _value[int16] ORDER BY E DESC 
-- 
-- --- (3.2) D = ? AND E ? AND F ?
-- SELECT * FROM _table WHERE D = _dvalue AND E _cmp _value[int16] AND F _cmp _value[int16] ORDER BY E DESC, F DESC
-- SELECT * FROM _table WHERE D = _dvalue AND E > _value[int16] AND E < _value[int16] ORDER BY E DESC, F DESC
-- SELECT * FROM _table WHERE D = _dvalue AND E > _value[int16] AND E <= _value[int16] ORDER BY E DESC, F DESC
-- SELECT * FROM _table WHERE D = _dvalue AND E >= _value[int16] AND E < _value[int16] ORDER BY E DESC, F DESC
-- SELECT * FROM _table WHERE D = _dvalue AND E >= _value[int16] AND E <= _value[int16] ORDER BY E DESC, F DESC

