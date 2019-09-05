<grammar.sql>
-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"} 
{@agg = "_numagg"}
{@distinctableagg = "_distinctableagg"}
{@winagg = "_numwinagg"} -- [not used here?]
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
{@columnpredicate = "_numericcolumnpredicate"}
{@columntype = "int"}
{@comparableconstant = "44"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] @cmp _value[int16]"}
{@dmltable = "_table"}
{@fromtables = "_table B, _table"}
{@idcol = "ID"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[string], _value[int32], _value[float]"}
{@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
{@numcol = "NUM"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@plus10 = " + 10"}
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "NUM"}
{@updatesource = "ID"}
{@updatevalue = "_value[int:0,100]"}
{@updatecolumn2 = "RATIO"} -- rarely used; so far, only in CTE tests
{@maxdepth = "6"} -- maximum depth, in Recursive CTE tests

--{@jointype = "INNER"}
{@jointype = "_jointype"}
