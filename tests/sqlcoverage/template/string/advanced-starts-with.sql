-- This file repeats all the same tests in advanced-strings.sql (including LIKE
-- and STARTS WITH tests), and then adds some tests, mostly of the STARTS WITH
-- operator, that can only run against PostgreSQL (that is, they do not work in
-- HSQL), which mostly means tests using UPSERT.
<advanced-strings.sql>

-- Redo INSERT statements, since advanced-strings.sql ends with DELETE statements
INSERT INTO @dmltable VALUES (@insertvals)
-- Insert some extra rows that will have interesting, non-empty STARTS WITH results
INSERT INTO @dmltable VALUES (_id, 'abc!dez', _value[string null20], _value[string null20], _value[float])
INSERT INTO @dmltable VALUES (_id, 'abc%',    'abc',                 'a',                   _value[float])
-- Uncomment these 2 after ENG-14485 is fixed:
--INSERT INTO @dmltable VALUES (_id, 'abc%%',   'abc',                 'a',                   _value[float])
--INSERT INTO @dmltable VALUES (_id, 'abc!',    'abc_',                'abc',                 _value[float])


-- Redo tests of the SUBSTRING function, since these fail in HSQL,
-- with the small VARCHAR values above
SELECT SUBSTRING ( VCHAR FROM _value[int:1,10] FOR _value[int:1,10] ) substrQ1 FROM @fromtables ORDER BY VCHAR
SELECT VCHAR substrQ3 FROM @fromtables ORDER BY SUBSTRING ( VCHAR FROM _value[int:1,10] ), VCHAR


-- Final tests of STARTS WITH in DML: UPSERT (does not work in HSQL)
UPSERT INTO @dmltable SELECT @upsertselectcols FROM @fromtables WHERE VCHAR _maybe STARTS WITH 'abc'             ORDER BY @idcol
UPSERT INTO @dmltable SELECT @upsertselectcols FROM @fromtables WHERE VCHAR _maybe STARTS WITH _variable[string] ORDER BY @idcol
-- Confirm the values that were "upserted"
SELECT * FROM @fromtables startsW35 ORDER BY @idcol
