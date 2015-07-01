<grammar.sql>
-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"} 
{@agg = "_numagg"}
{@columnpredicate = "_numericcolumnpredicate"}
{@columntype = "int"}
{@comparableconstant = "44"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] _cmp _value[int16]"}
{@dmltable = "_table"}
{@fromtables = "_table B, _table"}
{@idcol = "ID"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[string], _value[int32], _value[float]"}
{@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
{@numcol = "NUM"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@updatecolumn = "NUM"}
{@updatesource = "ID"}
{@updatevalue = "_value[int:0,100]"}

--{@jointype = "INNER"}
{@jointype = "_jointype"}
