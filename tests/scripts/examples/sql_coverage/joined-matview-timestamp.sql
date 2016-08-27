<configure-for-timestamp.sql>

-- Run SELECT queries against Views, rather than Tables
{@fromtables = "V_value[int:0,36,1]"}
-- The alternative, quick, but less thorough, version, running against
-- a few of the defined views, randomly selected
--{@fromtables = "V_value[int:0,36;6]"}

-- DML, purge and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- INSERT some rows with some identical column values, so that equi-joins are not empty
INSERT INTO @dmltable VALUES (-1,           null,                      null,                      null,)
INSERT INTO @dmltable VALUES (-2,           null,                      null,             '9999-12-31 23:59:59.987')
INSERT INTO @dmltable VALUES (-3,           null,             '2016-08-03 03:04:05.789',          null)
INSERT INTO @dmltable VALUES (-4,           null,             '2016-08-04 04:05:06.789', '2516-12-31 23:59:59.987')
INSERT INTO @dmltable VALUES (-5,  '1583-10-12 02:03:04.123',          null,                      null)
INSERT INTO @dmltable VALUES (-6,  '1775-04-19 07:08:09.123',          null,             '2775-04-19 07:08:09.123')
INSERT INTO @dmltable VALUES (-7,  '1967-10-01 16:08:09.123', '2016-08-07 07:08:09.123',          null)
INSERT INTO @dmltable VALUES (-8,  '2010-07-31 16:08:09.123', '2016-08-08 08:09:10.123', '2110-07-31 16:08:09.123')
INSERT INTO @dmltable VALUES (-9,  '2012-07-19 01:02:03.456', '2016-08-09 09:08:07.123', '2112-07-19 01:02:03.456')
INSERT INTO @dmltable VALUES (-10, '2016-08-10 11:02:03.456', '2016-08-10 11:02:03.456', '2016-08-10 11:02:03.456')
INSERT INTO @dmltable VALUES (-11, '2016-08-11 11:02:03.456', '2016-08-11 11:02:03.456', '2016-08-11 11:02:03.456')
INSERT INTO @dmltable VALUES (-12, '2016-08-12 11:02:03.456', '2016-08-12 11:02:03.456', '2016-08-12 11:02:03.456')
INSERT INTO @dmltable VALUES (-13, '2016-08-17 11:02:03.456', '2016-08-17 11:02:03.456', '2016-08-17 11:02:03.456')
INSERT INTO @dmltable VALUES (-14, _value[timestamp], _value[timestamp], _value[timestamp])
INSERT INTO @dmltable VALUES (-15, _value[timestamp], _value[timestamp], _value[timestamp])

-- Run both the "basic" and "advanced" SELECT query tests, and the basic UPDATE
-- and DELETE tests (with some extra SELECT statements, to check those results)
<basic-and-advanced-template.sql>
