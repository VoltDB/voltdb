<grammar.sql>

{_basetables |= "P2"}
{_basetables |= "R2"}
{_basetables |= "R2V"}

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_variable[@comparabletype] _cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] _cmp _value[int]"}

{@insertvals = "_id, 9, 9, 9, 9"}
{@idcol = "V_G1"}
{@numcol = "V_SUM_AGE"}

{@dmltable = "_basetables"}
{@fromtables = "_table"}

{@jointype = "_jointype"}

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])
INSERT INTO @dmltable VALUES (_id, 1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, 1020, 1020, 1020, 1020)

INSERT INTO @dmltable VALUES (_id, -1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, -1020, 1020, 1020, 1020)


--- Hsql bug: ENG-5362.
<basic-select.sql>
--- Remove the select template, the hsql problem may be reproduced.

<join-template.sql>


