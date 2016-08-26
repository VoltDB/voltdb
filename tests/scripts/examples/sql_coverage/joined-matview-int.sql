<configure-for-ints.sql>

-- Run SELECT queries against Views, rather than Tables
{@fromtables = "V_value[int:0,36;6]"}

-- DML, purge and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- INSERT some rows with some identical column values, so that equi-joins are not empty
INSERT INTO @dmltable VALUES (-1, null, null,  null)
INSERT INTO @dmltable VALUES (-2, null, null, -2222)
INSERT INTO @dmltable VALUES (-3, null, -333,  null)
INSERT INTO @dmltable VALUES (-4, null, -444, -4444)
INSERT INTO @dmltable VALUES (-5,  -55, null,  null)
INSERT INTO @dmltable VALUES (-6,  -66, null, -6666)
INSERT INTO @dmltable VALUES (-7,  -77, -777,  null)
INSERT INTO @dmltable VALUES (-8,  -88, -888, -8888)
INSERT INTO @dmltable VALUES (-9,  -99, -999, -9999)
INSERT INTO @dmltable VALUES (-10, -10,  -10,   -10)
INSERT INTO @dmltable VALUES (-11, -11,  -11,   -11)
INSERT INTO @dmltable VALUES (-12, -12,  -12,   -12)
INSERT INTO @dmltable VALUES (-13, -13,  -13,   -13)
INSERT INTO @dmltable VALUES (-14, _value[int:-10,10], _value[byte null30], _value[byte])
INSERT INTO @dmltable VALUES (-15, _value[int:-10,10], _value[byte null30], _value[byte])

-- Run both the "basic" and "advanced" SELECT query tests
<basic-select.sql>
<advanced-select.sql>

-- Run the basic UPDATE tests, and check the results afterward
<basic-update.sql>
SELECT @star FROM @dmltable   ST1
SELECT @star FROM @fromtables SV1

-- Run the basic DELETE tests, and check the results afterward
<basic-delete.sql>
SELECT @star FROM @dmltable   ST2
SELECT @star FROM @fromtables SV2
