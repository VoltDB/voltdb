<configure-for-string.sql>
<advanced-template.sql>

-- patterns in set 1 contain '%'
{_patterns1 |= "'abc%'"}
-- Uncomment after ENG-9449 is fixed:
--{_patterns1 |= "'%'"}
{_patterns1 |= "'!%'"}
{_patterns1 |= "'abc!%'"}
{_patterns1 |= "'abc!%%'"}
{_patterns1 |= "'abc%z'"}
{_patterns1 |= "'%z'"}
{_patterns1 |= "'!%z'"}
{_patterns1 |= "'abc!%z'"}
{_patterns1 |= "'abc!%%z'"}
{_patterns1 |= "'abc%!%z'"}
{_patterns1 |= "'abc'"}

-- patterns in set 2 contain '_'
{_patterns2 |= "'abc_'"}
{_patterns2 |= "'_'"}
{_patterns2 |= "'!_'"}
{_patterns2 |= "'abc!_'"}
{_patterns2 |= "'abc!__'"}
{_patterns2 |= "'abc_z'"}
{_patterns2 |= "'_z'"}
{_patterns2 |= "'!_z'"}
{_patterns2 |= "'abc!_z'"}
{_patterns2 |= "'abc!__z'"}
{_patterns2 |= "'abc_!_z'"}

-- patterns in set 3 contain '_%'
{_patterns3 |= "'abc_%'"}
{_patterns3 |= "'_%'"}
{_patterns3 |= "'!_%'"}
{_patterns3 |= "'abc!_%'"}
{_patterns3 |= "'abc!_%%'"}
{_patterns3 |= "'abc_%z'"}
{_patterns3 |= "'_%z'"}
{_patterns3 |= "'!_%z'"}
{_patterns3 |= "'abc!_%z'"}
{_patterns3 |= "'abc!_%%z'"}
{_patterns3 |= "'abc_%!%z'"}

-- patterns in set 4 contain '%_'
{_patterns4 |= "'abc%_'"}
{_patterns4 |= "'%_'"}
{_patterns4 |= "'!%_'"}
{_patterns4 |= "'abc!%_'"}
{_patterns4 |= "'abc!%__'"}
{_patterns4 |= "'abc%_z'"}
{_patterns4 |= "'%_z'"}
{_patterns4 |= "'!%_z'"}
{_patterns4 |= "'abc!%_z'"}
{_patterns4 |= "'abc!%__z'"}
{_patterns4 |= "'abc%_!_z'"}

-- patterns in set 5 contain '%%'
{_patterns5 |= "'abc%%'"}
{_patterns5 |= "'abc%%%'"}
{_patterns5 |= "'%%'"}
{_patterns5 |= "'!%%'"}
{_patterns5 |= "'abc!%%'"}
{_patterns5 |= "'abc!%%%'"}
{_patterns5 |= "'abc%%z'"}
{_patterns5 |= "'%%z'"}
{_patterns5 |= "'!%%z'"}
{_patterns5 |= "'abc!%%z'"}
{_patterns5 |= "'abc!%%%z'"}
{_patterns5 |= "'abc%%!%z'"}

-- patterns in set 6 contain '__'
{_patterns6 |= "'abc__'"}
{_patterns6 |= "'__'"}
{_patterns6 |= "'!__'"}
{_patterns6 |= "'abc!__'"}
{_patterns6 |= "'abc!___'"}
{_patterns6 |= "'abc__z'"}
{_patterns6 |= "'__z'"}
{_patterns6 |= "'!__z'"}
{_patterns6 |= "'abc!__z'"}
{_patterns6 |= "'abc!___z'"}
{_patterns6 |= "'abc__!_z'"}


-- Tests of the SUBSTRING function
SELECT SUBSTRING ( VCHAR FROM _value[int:1,10] FOR _value[int:1,10] ) substrQ1 FROM @fromtables ORDER BY VCHAR
SELECT VCHAR substrQ3 FROM @fromtables ORDER BY SUBSTRING ( VCHAR FROM _value[int:1,10] ), VCHAR


-- Insert some extra rows that will have interesting, non-empty STARTS WITH results
INSERT INTO @dmltable VALUES (_id, 'abc!dez', _value[string null20], _value[string null20], _value[float])
INSERT INTO @dmltable VALUES (_id, 'abc%',    'abc',                 'a',                   _value[float])
-- Uncomment these 2 after ENG-14485 is fixed:
--INSERT INTO @dmltable VALUES (_id, 'abc%%',   'abc',                 'a',                   _value[float])
--INSERT INTO @dmltable VALUES (_id, 'abc!',    'abc_',                'abc',                 _value[float])


-- Tests of LIKE operator
SELECT VCHAR likeQ11 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns1
SELECT VCHAR likeQ12 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns2
SELECT VCHAR likeQ13 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns3
SELECT VCHAR likeQ14 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns4
SELECT VCHAR likeQ15 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns5
SELECT VCHAR likeQ16 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns6
SELECT VCHAR likeQ21 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns1 ESCAPE '!'
SELECT VCHAR likeQ22 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns2 ESCAPE '!'
SELECT VCHAR likeQ23 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns3 ESCAPE '!'
SELECT VCHAR likeQ24 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns4 ESCAPE '!'
SELECT VCHAR likeQ25 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns5 ESCAPE '!'
SELECT VCHAR likeQ26 FROM @fromtables WHERE VCHAR _maybe LIKE _patterns6 ESCAPE '!'

-- Tests of one column LIKE another
SELECT _variable[#col1 string], _variable[#col2 string] FROM @fromtables likeQ28   WHERE __[#col1] _maybe LIKE __[#col2]
-- Uncomment this after ENG-14485 is fixed:
--SELECT _variable[#col1 string], _variable[#col2 string] FROM @fromtables likeQ29   WHERE __[#col1] _maybe LIKE __[#col2] || '%'


-- Tests of STARTS WITH operator
SELECT VCHAR startsW11 FROM @fromtables WHERE VCHAR _maybe STARTS WITH _patterns1
SELECT VCHAR startsW12 FROM @fromtables WHERE VCHAR _maybe STARTS WITH _patterns2
SELECT VCHAR startsW13 FROM @fromtables WHERE VCHAR _maybe STARTS WITH _patterns3
SELECT VCHAR startsW14 FROM @fromtables WHERE VCHAR _maybe STARTS WITH _patterns4
SELECT VCHAR startsW15 FROM @fromtables WHERE VCHAR _maybe STARTS WITH _patterns5
SELECT VCHAR startsW16 FROM @fromtables WHERE VCHAR _maybe STARTS WITH _patterns6
-- Note: "STARTS WITH ... ESCAPE" is not valid syntax, so not tested

-- Test of one column STARTS WITH another
SELECT _variable[#col1 string], _variable[#col2 string] FROM @fromtables startsW29 WHERE __[#col1] _maybe STARTS WITH __[#col2]


-- Tests of STARTS WITH in DML: INSERT, UPDATE, DELETE (but not UPSERT, which only
-- works testing against PostgreSQL, not HSQL: see advanced-starts-with.sql for that)
INSERT INTO @dmltable SELECT @insertselectcols FROM @fromtables startsW30 WHERE VCHAR _maybe STARTS WITH 'abc'
INSERT INTO @dmltable SELECT @insertselectcols FROM @fromtables startsW31 WHERE VCHAR _maybe STARTS WITH _variable[string]
-- Confirm the values that were inserted
SELECT * FROM @fromtables startsW32 ORDER BY @idcol

-- Uncomment these 2 after ENG-14478 is fixed (and delete the next 2??):
--UPDATE @dmltable startsW36 SET VCHAR_INLINE_MAX = VCHAR_INLINE WHERE VCHAR _maybe STARTS WITH 'abc'
--UPDATE @dmltable startsW37 SET VCHAR_INLINE_MAX = VCHAR_INLINE WHERE VCHAR _maybe STARTS WITH _variable[string]
UPDATE @dmltable startsW36a SET VCHAR_INLINE_MAX = 'xyz'                  WHERE VCHAR _maybe STARTS WITH 'abc'
UPDATE @dmltable startsW37a SET VCHAR_INLINE_MAX = 'xyz'                  WHERE VCHAR _maybe STARTS WITH _variable[string]
-- Confirm the values that were updated
SELECT * FROM @fromtables startsW38 ORDER BY @idcol

DELETE FROM @dmltable WHERE VCHAR        STARTS WITH 'abc!d'
DELETE FROM @dmltable WHERE VCHAR    NOT STARTS WITH 'abc'
-- Confirm which rows were deleted (& which not)
SELECT * FROM @fromtables startsW40 ORDER BY @idcol
DELETE FROM @dmltable WHERE VCHAR _maybe STARTS WITH _variable[#col2 string]
-- Confirm which rows were deleted (& which not, if any)
SELECT * FROM @fromtables startsW41 ORDER BY @idcol
