<grammar.sql>
-- Run the template against DDL with all INT types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
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
{@idcol = "ID"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[byte], _value[int32], _value[int64]"}
{@insertvals = "_id, _value[byte], _value[int16], _value[int16]"}
{@optionalfn = "_numfun"}
{@updatecolumn = "BIG"}
{@updatevalue = "_value[int16]"}
