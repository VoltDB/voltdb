<grammar.sql>
-- Run the template against DDL with all timestamp types
-- timestamps don't do math
{@aftermath = " "}
{@agg = "_genericagg"} -- timestamps don't do SUM or AVG
{@columnpredicate = "_timestampcolumnpredicate"}
{@columntype = "timestamp"}
-- OK. comparableconstant is not a constant, here.
-- just fudging a type-friendly placeholder until ad hoc timestamp composition is supported
{@comparableconstant = "_variable[#reusable timestamp]"}
{@comparabletype = "timestamp"}
-- OK. comparablevalue is not a constant generator, either.
-- just fudging a type-friendly placeholder until ad hoc timestamp composition is supported
--{@comparablevalue = "_value[timestamp]"}
{@comparablevalue = "_variable[timestamp]"}
{@dmlcolumnpredicate = "_timestampcolumnpredicate"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}


{@insertvals = "_id, _value[timestamp], _value[timestamp], _value[int64]"}
{@onefun = " "} -- There are no handy unary timestamp-to-timestamp functions.
{@optionalfn = " "} -- There are no handy unary timestamp-to-timestamp functions.
{@updatecolumn = "PAST"}
{@updatesource = "_value[timestamp]"}
{@updatevalue = "_value[timestamp]"}
