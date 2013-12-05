<grammar.sql>

{_basetables |= "P2"}
{_basetables |= "R2"}

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_variable[@comparabletype] _cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] _cmp _value[int]"}
{@idcol = "V_G1"}
{@numcol = "V_SUM_AGE"}

{@dmltable = "_basetables"}
{@fromtables = "_table"}

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16 null20], _value[int16])
INSERT INTO @dmltable VALUES (_id, 1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, 1020, 1020, 1020, 1020)

INSERT INTO @dmltable VALUES (_id, -1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, -1020, 1020, 1020, 1020)
INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])

<basic-select.sql>


--- test HAVING
--- test having
SELECT @agg(_variable[@comparabletype]) FROM @fromtables HAVING @agg(_variable[@comparabletype]) _cmp @comparableconstant
SELECT COUNT(*) FROM @fromtables HAVING @agg(_variable[@comparabletype]) _cmp @comparableconstant
SELECT COUNT(*) FROM @fromtables HAVING COUNT(*) _cmp @comparableconstant

SELECT _variable[#grouped], COUNT(*) AS FOO FROM @fromtables GROUP BY __[#grouped] HAVING @agg(_variable[@comparabletype]) _cmp @comparableconstant
SELECT _variable[#grouped], COUNT(*) AS FOO FROM @fromtables GROUP BY __[#grouped] HAVING @agg(_variable[@comparabletype]) _cmp @comparableconstant ORDER BY 2, 1 _optionallimitoffset 

