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

