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

{@dmltable = "_table"}
{_fromviews |= "V_P2"}
{_fromviews |= "V_P2_ABS"}
{_fromviews |= "V_R2"}
{@fromtables = "_fromviews"}

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])
INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16], _value[int16])

select * from P2
select * from V_P2
<basic-select.sql>



