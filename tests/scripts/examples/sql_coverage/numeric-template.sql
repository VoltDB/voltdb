-- Temporarily, this file holds patterns that are more complex or
-- differ from the SQL tested in the basic-template.sql file

-- Required preprocessor macros (with example values):
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@aftermath = " _math _value[int:1,3]"}
-- {@columntype = "decimal"}
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@fromtables = "_table"}

-- DML, clean out and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

{_plusorminus |= "+"}
{_plusorminus |= "-"}
-- Avoid multiplication-induced overflows and division-induced discrepencies with HSQL
-- which does floating point math for integer division!
-- SELECT _variable[numeric] _math        1.0001 AS BIG_MATH FROM @fromtables
-- SELECT _variable[numeric] _math 1      AS BIG_MATH FROM @fromtables
   SELECT _variable[numeric] _plusorminus 1.0001 AS BIG_MATH FROM @fromtables
   SELECT _variable[numeric] _plusorminus 1      AS BIG_MATH FROM @fromtables


-- alias fun
-- ticket 231
SELECT -8, _variable[#arg numeric] FROM @fromtables WHERE _numfun(__[#arg] + 5   )        > _numericvalue
SELECT -7, _variable[#arg numeric] FROM @fromtables WHERE _numfun(__[#arg]       ) + 5    > _numericvalue
SELECT -6, _numfun(_variable[numeric] + 5   )        NUMSUM FROM @fromtables ORDER BY NUMSUM
SELECT -5, _numfun(_variable[numeric]       ) + 5    NUMSUM FROM @fromtables ORDER BY NUMSUM
SELECT -4, _variable[#arg numeric] FROM @fromtables WHERE _numfun(__[#arg] + 5.25)        > _numericvalue
SELECT -3, _variable[#arg numeric] FROM @fromtables WHERE _numfun(__[#arg]       ) + 5.25 > _numericvalue
SELECT -2, _numfun(_variable[numeric] + 5.25)        NUMSUM FROM @fromtables ORDER BY NUMSUM
SELECT -1, _numfun(_variable[numeric]       ) + 5.25 NUMSUM FROM @fromtables ORDER BY NUMSUM

-- SELECT 3, * FROM @fromtables WHERE _numfun(_variable[numeric]) _cmp (_numfun(_variable[numeric]) _math _value[int:0,1000])
-- Substituting these two statements to cut down on the number of generated combinations and limit math range errors:
-- Found eng-3191 crash (fixed, since then) with statements like these:
{_somecmps |= " = "}
{_somecmps |= " > "}
{_somecmps |= " <= "}
   SELECT 3, * FROM @fromtables WHERE _numfun(_variable[numeric]) _somecmps (       (_variable[numeric]) _math        _value[int:1,10])
   SELECT 4, * FROM @fromtables WHERE        (_variable[numeric]) _somecmps (_numfun(_variable[numeric]) _plusorminus _value[int:1,10])

-- order by with generic expression
SELECT _variable[#A numeric], _variable[#B numeric], _numfun(__[#A]) _math _numfun(__[#B]) AS FOO13 FROM @fromtables ORDER BY FOO13
SELECT _numfun(_variable[numeric] _math _numfun(_variable[numeric])) AS FOO14 FROM @fromtables ORDER BY FOO14

-- ticket 232
SELECT _variable[#A numeric] as FOO15 FROM @fromtables GROUP BY __[#A] ORDER BY __[#A]

SELECT _numagg(_variable[#VA numeric]) AS Q20, _numagg(_numfun(__[#VA]) _plusorminus _numfun(_variable[#VA numeric])) FROM @fromtables
SELECT _numagg(_variable[#VA numeric]) AS Q20, _numagg(_numfun(__[#VA]) _plusorminus _numfun(_variable[#VA numeric])) FROM @fromtables
-- Avoid multiplication-induced overflows and division-induced discrepencies with HSQL -- which does floating point math for integer division!
-- SELECT SUM(DISTINCT _numfun(_variable[numeric] _math        _numfun(_variable[numeric])) AS Q22 FROM @fromtables
   SELECT SUM(DISTINCT _numfun(_variable[numeric] _plusorminus _numfun(_variable[numeric])) AS Q22 FROM @fromtables

-- additional select expression math
SELECT _variable[#A numeric], _numfun(__[#A]) _math _variable[numeric]  AS Q27 FROM @fromtables
SELECT _variable[#A numeric], _numfun(__[#A]  _math _variable[numeric]) AS Q28 FROM @fromtables

-- push on divide by zero
-- TODO: migrate likely-to-error-out cases like this to their own template/suite
SELECT _numfun(_numfun(_variable[numeric]) / 0)    AS Q29 FROM @fromtables
SELECT _numfun(_numfun(_variable[numeric]) / 0.0)  AS Q30 FROM @fromtables
SELECT _numfun(_numfun(_variable[numeric]  / 0))   AS Q31 FROM @fromtables
SELECT _numfun(_numfun(_variable[numeric]  / 0.0)) AS Q32 FROM @fromtables
-- we throw an underflow exception and HSQL returns INF.
--SELECT _numfun(_variable[numeric]) / -1e-306 from @fromtables

SELECT * FROM @fromtables FOO33 WHERE _inoneint
SELECT * FROM @fromtables FOO34 WHERE _inpairofints
--just too slow for now SELECT * FROM FOO35 @fromtables WHERE _insomeints
