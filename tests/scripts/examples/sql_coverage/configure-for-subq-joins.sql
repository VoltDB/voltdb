<grammar.sql>

{_subqueryform |= "(select * from _table)"}
{_subqueryform |= "(select     _variable[#numone float],                  _variable[#arg numeric],             _variable[#string string]             from _table[#self] order by __[#string], __[#arg], __[#numone] limit 1 offset 1)"}
{_subqueryform |= "(select   X._variable[#numone float]  __[#numone],   X._variable[#arg numeric]  __[#arg], X._variable[#string string] __[#string] from _table[#self] X _jointype join __[#self] Y on X.ID = Y.ID where Y._numericcolumnpredicate)"}
{_subqueryform |= "(select max(_variable[#numone float]) __[#numone], sum(_variable[#arg numeric]) __[#arg],   _variable[#string string]             from _table[#self] group by __[#string])"}
{_subqueryform |= "(select max(_variable[#numone float]) __[#numone], sum(_variable[#arg numeric]) __[#arg],   _variable[#string string]             from _table[#self] group by __[#string] order by __[#string] limit 1 offset 1)"}


-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"} 
{@agg = "_numagg"}
{@columnpredicate = "A._variable[#arg numeric] _cmp _numericvalue"}
{@columntype = "int"}
{@comparableconstant = "44"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] _cmp _value[int16]"}
{@dmltable = "_table"}
-- {@fromtables = "_subqueryform B, _table"}
{@fromtables = "_table B, _subqueryform "}
{@idcol = "ID"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[string], _value[int32], _value[float]"}
{@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
{@numcol = "NUM"}
{@onefun = "ABS"}
{@optionalfn = "_numfun"}
{@updatecolumn = "NUM"}
{@updatesource = "ID"}
{@updatevalue = "_value[int:0,100]"}

--{@jointype = "INNER"}
{@jointype = "_jointype"}
