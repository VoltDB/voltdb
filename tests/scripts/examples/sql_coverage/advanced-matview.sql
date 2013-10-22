<grammar.sql>

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_variable[@comparabletype] _cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] _cmp _value[int]"}
{@idcol = "V_CNT"}
{@numcol = "V_SUM_AGE"}

{@dmltable = "_table"}
{_fromviews |= "V_P2"}
{_fromviews |= "V_P2_ABS"}
{_fromviews |= "V_R2"}
{@fromtables = "_fromviews"}


{@jointype = " INNER "}
-- Replace the join type with the next line when ENG-5178 is fixed.
-- {@jointype = "_jointype"}

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])
<basic-select.sql>
<join-template.sql>

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])
INSERT INTO dmltable VALUES (_id, 1010, 1010, 1010, 1010)
INSERT INTO dmltable VALUES (_id, 1020, 1020, 1020, 1020)

-- Repeat queries with forced data value overlaps between tables.
<basic-select.sql>
<join-template.sql>


