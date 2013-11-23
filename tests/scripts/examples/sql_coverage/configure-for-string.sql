<grammar.sql>
-- Run the template against DDL with mostly string types
-- strings don't do math
{@aftermath = " "}
{@agg = "_genericagg"} -- strings don't do SUM or AVG
{@columnpredicate = "_stringcolumnpredicate"}
{@columntype = "string"}
{@comparableconstant = "'42'"}
{@comparabletype = "string"}
{@comparablevalue = "_value[string]"}
{@dmlcolumnpredicate = "_variable[string] _cmp _value[string]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}


{@insertvals = "_id, _value[string], _value[string null30], _value[float]"}
-- There are no unary string-to-string functions supported yet.
{@onefun = ""}
--{@onefun = "LOWER"}
{@optionalfn = "_stringfun"}

{@updatecolumn = "DESC"}
{@updatesource = "DESC_INLINE"}
{@updatevalue = "_value[string]"}
