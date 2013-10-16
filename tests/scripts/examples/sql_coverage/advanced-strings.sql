<configure-for-string.sql>
<advanced-template.sql>

SELECT SUBSTRING ( DESC FROM _value[int:1,10] FOR _value[int:1,10] ) substrQ1 FROM @fromtables ORDER BY DESC
SELECT DESC substrQ3 FROM @fromtables ORDER BY SUBSTRING ( DESC FROM _value[int:1,10] ), DESC

-- patterns in set 1 contain '%'
{_patterns1 |= "'abc%'"}
{_patterns1 |= "'%'"}
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

SELECT DESC likeQ11 FROM @fromtables WHERE DESC _maybe LIKE _patterns1
SELECT DESC likeQ12 FROM @fromtables WHERE DESC _maybe LIKE _patterns2
SELECT DESC likeQ13 FROM @fromtables WHERE DESC _maybe LIKE _patterns3
SELECT DESC likeQ14 FROM @fromtables WHERE DESC _maybe LIKE _patterns4
SELECT DESC likeQ15 FROM @fromtables WHERE DESC _maybe LIKE _patterns5
SELECT DESC likeQ16 FROM @fromtables WHERE DESC _maybe LIKE _patterns6
SELECT DESC likeQ21 FROM @fromtables WHERE DESC _maybe LIKE _patterns1 ESCAPE '!'
SELECT DESC likeQ22 FROM @fromtables WHERE DESC _maybe LIKE _patterns2 ESCAPE '!'
SELECT DESC likeQ23 FROM @fromtables WHERE DESC _maybe LIKE _patterns3 ESCAPE '!'
SELECT DESC likeQ24 FROM @fromtables WHERE DESC _maybe LIKE _patterns4 ESCAPE '!'
SELECT DESC likeQ25 FROM @fromtables WHERE DESC _maybe LIKE _patterns5 ESCAPE '!'
SELECT DESC likeQ26 FROM @fromtables WHERE DESC _maybe LIKE _patterns6 ESCAPE '!'
