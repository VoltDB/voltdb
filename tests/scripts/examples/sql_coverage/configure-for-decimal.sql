<grammar.sql>
-- Run the template against DDL with mostly decimal columns
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_numericcolumnpredicate"}
{@columntype = "decimal"}
{@comparableconstant = "42.42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] _cmp _value[int16]"}
{@dmltable = "_table"}
{@from_tables = "_table"}
{@idcol = "ID"}


{@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
{@optionalfn = "_numfun"}
{@updatecolumn = "CASH"}
{@updatevalue = "_value[decimal]"}
