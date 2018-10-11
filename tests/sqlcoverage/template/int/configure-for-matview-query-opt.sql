<grammar.sql>

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@distinctableagg = "_distinctableagg"}
{@winagg = "_numwinagg"} -- [not used here?]
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=) [not used here?]
{@columnpredicate = "_variable[@comparabletype] @cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] @cmp _value[int]"}
{@idcol = "ID"}
{@insertvals = "_id, _value[byte], _value[byte], _value[byte], _value[byte]"}
{@numcol = "WAGE"}
{@plus10 = " + 10"}
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "WAGE"}
{@updatesource = "DEPT"}
{@updatevalue = "_value[int]"}

{_optfun |= ""}
{_optfun |= "ABS"}
{_basetables |= "P2"}
{_basetables |= "R2"}
-- Not currently used!:
{_fromviews |= "P2_V0"}
{_fromviews |= "P2_V1"}
{_fromviews |= "P2_V2"}
{_fromviews |= "P2_V2A"}
{_fromviews |= "P2_V3"}
{_fromviews |= "P2_V4"}
{_fromviews |= "R2_V1"}
{_fromviews |= "R2_V2"}
{_fromviews |= "R2_V3"}

{@optionalfn = "_optfun"}
{@dmltable = "_basetables"}
-- Not currently used!:
{@fromtables = "_fromviews"}
