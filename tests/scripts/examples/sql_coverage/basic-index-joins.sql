<grammar.sql>

{_explicitsortorder |= " ASC "}
{_explicitsortorder |= " DESC "}

{_bvalue |= "10"}
{_bvalue |= "_value[int16]"} 

{_dvalue |= "20"}
{_dvalue |= "_value[int16]"} 


{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_integercolumnpredicate"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] _cmp _value[int]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "A"}
-- scaling down the random values until overflow detection works in VoltDB AND HSQL
--{@insertvals = "_id, _value[byte], _value[int16], _value[int64]"}
{@insertvals = "_id, _value[int:-10,10], _value[byte null30], _value[int16]"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@updatecolumn = "C"}
{@updatesource = "D"}
{@updatevalue = "_value[int16]"}

{@jointype = "_jointype"}


INSERT INTO _table VALUES (_value[int16], _bvalue, _value[int16], _dvalue, _value[int16 null30], _value[int16])

INSERT INTO _table VALUES (_value[int16], 10, _value[int16], 20, _value[int16], _value[int16])

--- Aggregate with join
<join-aggregate-template.sql>

