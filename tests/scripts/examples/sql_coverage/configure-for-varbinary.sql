<grammar.sql>
-- Run the template against DDL with mostly varbinary types

{@aftermath = " "}
{@agg = "_genericagg"} -- varbinary don't do SUM or AVG
{@columnpredicate = "_cmp"}
{@columntype = "varbinary"}
{@comparableconstant = "X'AB'"}  -- HEX literal string
{@comparabletype = "varbinary"}
{@comparablevalue = "_value[varbinary]"}
{@dmlcolumnpredicate = "_variable[varbinary] _cmp _value[varbinary]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}


{@insertvals = "_id, _value[varbinary null20], _value[varbinary null20], _value[varbinary null20], _value[varbinary null20]"}
-- There are no unary string-to-string functions supported yet.
{@onefun = ""}
{@optionalfn = ""}

{@updatecolumn = "_value[varbinary]"}
{@updatesource = "_value[varbinary]"}
{@updatevalue = "_value[varbinary]"}
