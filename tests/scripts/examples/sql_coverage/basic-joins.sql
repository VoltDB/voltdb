-- Run the join-template against the default table in schema.py
-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?
{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_numericcolumnpredicate"}
{@columntype = "int"}
{@comparableconstant = "44"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[numeric] _cmp _value[int16]"}
{@dmltable = "_table"}
-- reducing the random values to int16 until overflow detection works
--{@insertvals = "_id, _value[string], _value[int32], _value[float]"}
{@insertvals = "_id, _value[string], _value[int16], _value[float]"}
{@optionalfn = "_numfun"}
{@updatecolumn = "NUM"}
{@updatevalue = "_value[int:0,100]"}

{@idcol = "ID"}
{@numcol = "NUM"}
{@jointype = "_pick[<options=,INNER,LEFT,RIGHT>]"}

<join-template.sql>

-- Force some non-random values to get overlaps -- yes sadly this breaks the schema-independence of the test.
INSERT INTO _table VALUES (1000, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1001, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1010, 'desc_1010', 1010, 1010.5)
INSERT INTO _table VALUES (1011, 'desc_1010', 1010, 1010.5)
-- Purposely excluding rows from some _tables to tease out different cases.
INSERT INTO P1 VALUES (1020, 'desc_1020', 1020, 1020.5)
INSERT INTO R1 VALUES (1020, 'desc_1020', 1020, 1020.5)

-- Repeat queries with forced data value overlaps between tables.
<join-template.sql>
