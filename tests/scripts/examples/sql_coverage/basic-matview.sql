<grammar.sql>
-- This is the same as the basic ints template, but the DDL for this has
-- materialized views, so the SELECT statements should target the views rather than '_table's.
-- Run the template against DDL with all INT types

-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"} 
{@agg = "_numagg"}
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=) [not used here?]
{@columntype = "int64"}
{@columnpredicate = "_numericcolumnpredicate"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@columnpredicate = "_numericcolumnpredicate"}
{@dmlcolumnpredicate = "_variable[numeric] @cmp _value[int16]"}
{@dmltable = "_table"}

-- Here's the materialized view fudge.
-- It relies on updates and deletes referencing _table values while selects use @fromtables.
-- A similar hack is used to query joins of _table values.
-- To avoid invalid statements, the view DDL should use consistent view column names from the base tables,
-- even for counts and sums.
{_fromviews |= "MATR1"}
{_fromviews |= "MATR2"}
{@fromtables = "_fromviews"}

{@idcol = "ID"}
{@insertvals = "_id, _value[int:-10,10], _value[byte null20], _value[int32]"}
{@optional_fn = "_numfun"}
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "BIG"}
{@updatesource = "ID"}
{@updatevalue = "_value[int64]"}

<basic-template.sql>
