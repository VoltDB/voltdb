<grammar.sql>
-- Run the template against DDL with all INT types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@distinctableagg = "_distinctableagg"}
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
{@columnpredicate = "_integercolumnpredicate"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] @cmp _value[int]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}
{@insertcols = "ID, TINY, SMALL, BIG"}
-- scaling down the random values until overflow detection works in VoltDB AND HSQL
--{@insertvals = "_id, _value[byte], _value[int16], _value[int64]"}
{@insertvals = "_id, _value[int:-10,10], _value[byte null30], _value[byte]"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@plus10 = " + 10"}
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "BIG"}
{@updatesource = "ID"}
{@updatevalue = "_value[byte]"}
