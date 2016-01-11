<grammar.sql>
-- Run the template against DDL with all timestamp types
-- timestamps don't do math
{@aftermath = " "}
{@agg = "_genericagg"} -- timestamps don't do SUM or AVG
{@columnpredicate = "_timestampcolumnpredicate"}
{@columntype = "timestamp"}
{@comparableconstant = "'1967-10-01 15:25:26.123457'"}
{@comparabletype = "timestamp"}
{@comparablevalue = "_value[timestamp]"}
{@dmlcolumnpredicate = "_timestampcolumnpredicate"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}


{@insertvals = "_id, _value[timestamp], _value[timestamp], _value[timestamp], _value[int64]"}
{@onefun = " "} -- There are no handy unary timestamp-to-timestamp functions.
{@optionalfn = " "} -- There are no handy unary timestamp-to-timestamp functions.
{@star = "*"}
{@lhsstar = "*"}
{@updatecolumn = "PAST"}
{@updatesource = "_value[timestamp]"}
{@updatevalue = "_value[timestamp]"}
