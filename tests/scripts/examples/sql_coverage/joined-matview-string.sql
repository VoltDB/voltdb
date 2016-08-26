<configure-for-string.sql>

-- Run SELECT queries against Views, rather than Tables
{@fromtables = "V_value[int:0,36;6]"}

-- DML, purge and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- INSERT some rows with some identical column values, so that equi-joins are not empty
INSERT INTO @dmltable VALUES (-1,   null,  null,  null,  null)
INSERT INTO @dmltable VALUES (-2,   null,  null, 'bbb', -22.22)
INSERT INTO @dmltable VALUES (-3,   null, 'ccc',  null,  null)
INSERT INTO @dmltable VALUES (-4,   null, 'ccc', 'bbb', -22.22)
INSERT INTO @dmltable VALUES (-5,  'eee',  null,  null,  null)
INSERT INTO @dmltable VALUES (-6,  'eee',  null, 'fff', -66.66)
INSERT INTO @dmltable VALUES (-7,  'eee', 'ggg',  null,  null)
INSERT INTO @dmltable VALUES (-8,  'eee', 'ggg', 'fff', -66.66)
INSERT INTO @dmltable VALUES (-9,  'jjj', 'ggg', 'fff', -66.66)
INSERT INTO @dmltable VALUES (-10, 'jjj', 'jjj', 'jjj', -10)
INSERT INTO @dmltable VALUES (-11, 'klm', 'klm', 'klm', -11)
INSERT INTO @dmltable VALUES (-12, 'lll', 'lll', 'lll', -12)
INSERT INTO @dmltable VALUES (-13, 'mmm', 'mmm', 'mmm', -13)
INSERT INTO @dmltable VALUES (-14, _value[string null20], _value[string null20], _value[string null20], _value[float])
INSERT INTO @dmltable VALUES (-15, _value[string null20], _value[string null20], _value[string null20], _value[float])

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
