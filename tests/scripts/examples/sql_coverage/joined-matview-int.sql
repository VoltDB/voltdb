<configure-for-ints.sql>

-- Run SELECT queries against Views, rather than Tables - the complete version,
-- running against all of the defined views (may be slow)
{@fromtables = "V_value[int:0,36,1]"}
-- The alternative, quick, but less thorough, version, running against
-- a few of the defined views, randomly selected
--{@fromtables = "V_value[int:0,36;6]"}

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

-- Run both the "basic" and "advanced" SELECT query tests, and the basic UPDATE
-- and DELETE tests (with some extra SELECT statements, to check those results)
<basic-and-advanced-template.sql>
