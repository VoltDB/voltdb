<grammar.sql>

INSERT INTO _table VALUES (_value[int16], _value[int16], _value[int16], _value[int16], _value[int16], _value[int16])

INSERT INTO _table VALUES (_value[int16], 10, _value[int16], 20, _value[int16], _value[int16])


SELECT * FROM _table WHERE A _cmp _value[int16] ORDER BY A _sortorderTwoTwo
SELECT * FROM _table WHERE B _cmp _value[int16] AND C _cmp _value[int16] ORDER BY C _sortorderTwo 
SELECT * FROM _table WHERE D _cmp _value[int16] AND E _cmp _value[int16] ORDER BY C _sortorderTwo 
SELECT * FROM _table WHERE D _cmp _value[int16] AND E _cmp _value[int16] AND F _cmp _value[int16] ORDER BY E _sortorderTwo 

--- (1) Test reverse scan one column case:
SELECT * FROM _table WHERE A < _value[int16] 
SELECT * FROM _table WHERE A <= _value[int16]
SELECT * FROM _table WHERE A > _value[int16] ORDER BY A DESC
SELECT * FROM _table WHERE A >= _value[int16] ORDER BY A DESC

-- (1.1) test between:
SELECT * FROM _table WHERE A > _value[int16] AND A < _value[int16] ORDER BY A DESC
SELECT * FROM _table WHERE A > _value[int16] AND A <= _value[int16] ORDER BY A DESC
SELECT * FROM _table WHERE A >= _value[int16] AND A < _value[int16] ORDER BY A DESC
SELECT * FROM _table WHERE A >= _value[int16] AND A <= _value[int16] ORDER BY A DESC


--- (2) Test reverse scan two columns case:
SELECT * FROM _table WHERE B = _value[int16] ORDER BY C DESC 

--- (2.1) B = ? AND C
SELECT * FROM _table WHERE B = 10 AND C _cmp _value[int16] ORDER BY C DESC 
SELECT * FROM _table WHERE B = _value[int16] AND C _cmp _value[int16] ORDER BY C DESC 
SELECT * FROM _table WHERE B = _value[int16] AND C > _value[int16] AND C < _value[int16] ORDER BY C DESC 
SELECT * FROM _table WHERE B = _value[int16] AND C > _value[int16] AND C <= _value[int16] ORDER BY C DESC 
SELECT * FROM _table WHERE B = _value[int16] AND C >= _value[int16] AND C < _value[int16] ORDER BY C DESC 
SELECT * FROM _table WHERE B = _value[int16] AND C >= _value[int16] AND C <= _value[int16] ORDER BY C DESC 

--- (2.2) B < ? AND C
SELECT * FROM _table WHERE B < _value[int16] AND C _cmp _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B < _value[int16] AND C > _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B < _value[int16] AND C > _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B < _value[int16] AND C >= _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B < _value[int16] AND C >= _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 

--- (2.3) B <= ? AND C
SELECT * FROM _table WHERE B <= _value[int16] AND C _cmp _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B <= _value[int16] AND C > _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B <= _value[int16] AND C > _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B <= _value[int16] AND C >= _value[int16] AND C < _value[int16] ORDER BY B DESC, C DESC 
SELECT * FROM _table WHERE B <= _value[int16] AND C >= _value[int16] AND C <= _value[int16] ORDER BY B DESC, C DESC 


--- (3) Test reverse scan three columns case:
SELECT * FROM _table WHERE D = _value[int16] ORDER BY E DESC, F DESC 
SELECT * FROM _table WHERE D = _value[int16] AND E _cmp _value[int16] ORDER BY E DESC 

--- (3.1) D = ? AND E ?
SELECT * FROM _table WHERE D = 20 AND E _cmp _value[int16] ORDER BY E DESC 
SELECT * FROM _table WHERE D = _value[int16] AND E _cmp _value[int16] ORDER BY E DESC 
SELECT * FROM _table WHERE D = _value[int16] AND E > _value[int16] AND E < _value[int16] ORDER BY E DESC 
SELECT * FROM _table WHERE D = _value[int16] AND E > _value[int16] AND E <= _value[int16] ORDER BY E DESC 
SELECT * FROM _table WHERE D = _value[int16] AND E >= _value[int16] AND E < _value[int16] ORDER BY E DESC 
SELECT * FROM _table WHERE D = _value[int16] AND E >= _value[int16] AND E <= _value[int16] ORDER BY E DESC 

--- (3.2) D = ? AND E ? AND F ?
SELECT * FROM _table WHERE D = 20 AND E _cmp _value[int16] AND F _cmp _value[int16] ORDER BY E DESC, F DESC
SELECT * FROM _table WHERE D = _value[int16] AND E _cmp _value[int16] AND F _cmp _value[int16] ORDER BY E DESC, F DESC
SELECT * FROM _table WHERE D = _value[int16] AND E > _value[int16] AND E < _value[int16] ORDER BY E DESC, F DESC
SELECT * FROM _table WHERE D = _value[int16] AND E > _value[int16] AND E <= _value[int16] ORDER BY E DESC, F DESC
SELECT * FROM _table WHERE D = _value[int16] AND E >= _value[int16] AND E < _value[int16] ORDER BY E DESC, F DESC
SELECT * FROM _table WHERE D = _value[int16] AND E >= _value[int16] AND E <= _value[int16] ORDER BY E DESC, F DESC

