<grammar.sql>
-- Run the template against DDL with all timestamp types
-- timestamps don't do math
{@aftermath = " "}
{@agg = "_genericagg"} -- timestamps don't do SUM or AVG
{@distinctableagg = "COUNT"} -- timestamps don't do SUM
{@winagg = "_stringwinagg"} -- timestamps don't do SUM (or AVG)
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
{@columnpredicate = "_timestampcolumnpredicate"}
{@columntype = "timestamp"}
{@comparableconstant = "'1967-10-01 15:25:26.123457'"}
{@comparabletype = "timestamp"}
{@comparablevalue = "_value[timestamp]"}
{@dmlcolumnpredicate = "_timestampcolumnpredicate"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

{@insertcols = "ID, PAST, PRESENT, FUTURE, RATIO"}
{@insertselectcols = "ID+16, PAST, PRESENT, FUTURE, RATIO"}
{@insertvals = "_id, _value[timestamp], _value[timestamp], _value[timestamp], _value[int64]"}
{@onefun = " "} -- There are no handy unary timestamp-to-timestamp functions.
{@optionalfn = " "} -- There are no handy unary timestamp-to-timestamp functions.
{@plus10 = ""} -- You cannot add to a timestamp
{@rankorderbytype = "timestamp"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "PRESENT"}
{@updatesource = "PAST"}
{@updatevalue = "_value[timestamp]"}
{@updatecolumn2 = "FUTURE"} -- rarely used; so far, only in CTE tests
{@maxdepth = "3"} -- maximum depth, in Recursive CTE tests
