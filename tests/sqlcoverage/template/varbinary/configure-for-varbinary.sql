<grammar.sql>
-- Run the template against DDL with mostly varbinary types

{@aftermath = " "}
{@agg = "_genericagg"} -- varbinary don't do SUM or AVG
{@distinctableagg = "COUNT"} -- varbinary don't do SUM
{@winagg = "_stringwinagg"} -- varbinary don't do SUM (or AVG) [not currently used here?]
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=) [not used here?]
{@columnpredicate = "@cmp"}
{@columntype = "varbinary"}
{@comparableconstant = "X'AB'"}  -- HEX literal string
{@comparabletype = "varbinary"}
{@comparablevalue = "_value[varbinary]"}
{@dmlcolumnpredicate = "_variable[varbinary] @cmp _value[varbinary]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}


{@insertvals = "_id, _value[varbinary null20], _value[varbinary null20], _value[varbinary null20], _value[varbinary null20]"}
-- There are no unary string-to-string functions supported yet.
{@onefun = ""}
{@optionalfn = ""}
{@plus10 = ""} -- You cannot add to a varbinary
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "B"}
{@updatesource = "A"}
{@updatevalue = "_value[varbinary]"}
{@updatecolumn2 = "C"} -- rarely used; so far, only in CTE tests
{@maxdepth = "6"} -- maximum depth, in Recursive CTE tests
