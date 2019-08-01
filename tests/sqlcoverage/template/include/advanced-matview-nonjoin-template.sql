INSERT INTO @dmltable VALUES (_id, _value[byte], _value[byte], _value[byte null20], _value[byte])
INSERT INTO @dmltable VALUES (_id, 1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, 1020, 1020, 1020, 1020)

INSERT INTO @dmltable VALUES (_id, -1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, -1020, 1020, 1020, 1020)
INSERT INTO @dmltable VALUES (_id, _value[byte], _value[byte], _value[byte], _value[byte])

<basic-select.sql>

--- test HAVING
SELECT @agg(_variable[@comparabletype]) FROM @fromtables A HAVING @agg(_variable[#arg @comparabletype]) _cmp @comparableconstant
SELECT COUNT(*) FROM @fromtables A HAVING @agg(_variable[#arg @comparabletype]) _cmp @comparableconstant
SELECT COUNT(*) FROM @fromtables A HAVING COUNT(*) _cmp @comparableconstant

SELECT _variable[#grouped], COUNT(*) AS FOO FROM @fromtables A GROUP BY __[#grouped] HAVING @agg(_variable[#arg @comparabletype]) _cmp @comparableconstant
SELECT _variable[#grouped], COUNT(*) AS FOO FROM @fromtables A GROUP BY __[#grouped] HAVING @agg(_variable[#arg @comparabletype]) _cmp @comparableconstant ORDER BY 2, 1 _optionallimitoffset 
