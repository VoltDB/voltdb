
-- DML, purge and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- INSERT some rows with some identical column values, so that equi-joins are not empty
INSERT INTO @dmltable VALUES (-1, null, null,  null)
INSERT INTO @dmltable VALUES (-2, null, null, -22.22)
INSERT INTO @dmltable VALUES (-3, null, -333,  null)
INSERT INTO @dmltable VALUES (-4, null, -333, -22.22)
INSERT INTO @dmltable VALUES (-5,  'eee', null,  null)
INSERT INTO @dmltable VALUES (-6,  'eee', null, -66.66)
INSERT INTO @dmltable VALUES (-7,  'eee', -777,  null)
INSERT INTO @dmltable VALUES (-8,  'eee', -777, -66.66)
INSERT INTO @dmltable VALUES (-9,  'jjj', -777, -66.66)
INSERT INTO @dmltable VALUES (-10, 'jjj',  -10,   -10)
INSERT INTO @dmltable VALUES (-11, 'jjj',  -11,   -11)
INSERT INTO @dmltable VALUES (-12, 'mmm',  -12,   -12)
INSERT INTO @dmltable VALUES (-13, 'mmm',  -13,   -13)
INSERT INTO @dmltable VALUES (-14, _value[string], _value[int16 null25], _value[float])
INSERT INTO @dmltable VALUES (-15, _value[string], _value[int16 null25], _value[float])

-- Run both the "basic" and "advanced" SELECT query tests, and the basic UPDATE
-- and DELETE tests (with some extra SELECT statements, to check those results)
<basic-and-advanced-template.sql>
