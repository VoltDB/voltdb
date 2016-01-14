<grammar.sql>
-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"} 
{@agg = "_numagg"}
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
{@columnpredicate = "_numericcolumnpredicate"}
{@columntype = "int"}
{@comparableconstant = "42.42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_value[int:200,250]"}
{@dmlcolumnpredicate = "_variable[numeric] @cmp _value[int16]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[string], _value[int32], _value[float]"}
{@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "NUM"}
{@updatesource = "ID"}
{@updatevalue = "_value[int:0,100]"}
