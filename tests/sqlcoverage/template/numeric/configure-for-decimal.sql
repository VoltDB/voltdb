<grammar.sql>
-- Run the template against DDL with mostly decimal columns
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@distinctableagg = "_distinctableagg"}
{@winagg = "_numwinagg"}
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
{@columnpredicate = "_numericcolumnpredicate"}
{@columntype = "decimal"}
{@comparableconstant = "42.42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] @cmp _value[int16]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

{@insertcols = "ID, CASH, CREDIT, RATIO"}
{@insertselectcols = "ID+8, CASH, CREDIT, RATIO"}
{@insertvals = "_id, _value[decimal], _value[decimal null30], _value[float]"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@plus10 = " + 10"}
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "CASH"}
{@updatesource = "ID"}
{@updatevalue = "_value[decimal]"}
{@updatecolumn2 = "CREDIT"} -- rarely used; so far, only in CTE tests
{@maxdepth = "6"} -- maximum depth, in Recursive CTE tests
