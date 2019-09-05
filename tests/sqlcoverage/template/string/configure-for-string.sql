<grammar.sql>
-- Run the template against DDL with mostly string types
-- strings don't do math
{@aftermath = " "}
{@agg = "_genericagg"} -- strings don't do SUM or AVG
{@distinctableagg = "COUNT"} -- strings don't do SUM
{@winagg = "_stringwinagg"} -- strings don't do SUM (or AVG)
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
{@columnpredicate = "_stringcolumnpredicate"}
{@columntype = "string"}
{@comparableconstant = "'42'"}
{@comparabletype = "string"}
{@comparablevalue = "_value[string]"}
{@dmlcolumnpredicate = "_variable[string] @cmp _value[string]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}
{_nonidcols |= "VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, RATIO"}
{@insertcols = "ID, _nonidcols"}
{@insertselectcols = "_id, _nonidcols"}
{@upsertselectcols = "ID+16, _nonidcols"}
{@insertvals = "_id, _value[string null20], _value[string null20], _value[string null20], _value[float]"}
-- There are no unary string-to-string functions supported yet.
{@onefun = ""}
--{@onefun = "LOWER"}
{@optionalfn = "_stringfun"}
{@plus10 = " || '10'"}  -- 'plus' here is interpreted as concatenation
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "VCHAR"}
{@updatesource = "VCHAR_INLINE"}
{@updatevalue = "_value[string]"}
{@updatecolumn2 = "VCHAR_INLINE_MAX"} -- rarely used; so far, only in CTE tests
{@maxdepth = "4"} -- maximum depth, in Recursive CTE tests
