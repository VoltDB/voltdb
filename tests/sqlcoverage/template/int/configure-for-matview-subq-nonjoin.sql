<grammar.sql>

{_subqueryform |= "(select * from _table)"}
{_subqueryform |= "(select         _variable[#arg int16],             _variable[#numone int16]             from _table[#self] order by __[#numone] limit 1 offset 1)"}
{_subqueryform |= "(select       X._variable[#arg int16], __[#arg], X._variable[#numone int16] __[#numone] from _table[#self] X _jointype join __[#self] Y on X.__[#numone] = Y.__[#numone])"}
{_subqueryform |= "(select _numagg(_variable[#arg int16]) __[#arg],   _variable[#numone int16] __[#numone] from _table[#self] group by __[#numone])"}
{_subqueryform |= "(select max(_variable[#numone float]) __[#numone], sum(_variable[#arg numeric]) __[#arg],   _variable[#string string]             from _table[#self] group by __[#string] order by __[#string] limit 1 offset 1)"}

{_basetables |= "P2"}
{_basetables |= "R2"}

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@distinctableagg = "_distinctableagg"}
{@winagg = "_numwinagg"} -- [not used here?]
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=) [not used here?]
{@columnpredicate = "_variable[#arg int16] @cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] @cmp _value[int]"}
{@idcol = "V_G1"}
{@numcol = "V_SUM_AGE"}
{@plus10 = " + 10"}
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "*"}
{@lhsstar = "*"}

{@dmltable = "_basetables"}
{@fromtables = "_subqueryform"}
