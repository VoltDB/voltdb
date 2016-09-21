-- This file tests both the "basic" and "advanced" SELECT query tests, the basic
-- UPDATE tests, and the basic DELETE tests, with a few simple SELECT queries
-- after the UPDATE and DELETE tests, to confirm their results. Note that no
-- initial INSERT statements are included, so those should probably be run before
-- this file, unless you want to test empty tables. This file was created to
-- test Materialized Views defined using Joins (hence those extra simple SELECT
-- queries run against both @dmltable, which are the table in that context, and
-- against @fromtables, which are the views), though I could imagine it having
-- other uses in the future.

-- Required preprocessor templates:
-- {@aftermath = " _math _value[int:1,3]"}
-- {@agg = "_numagg"}
-- {@cmp = "_cmp"} -- all comparison operators (=, <>, !=, <, >, <=, >=)
-- {@columntype = "decimal"}
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@comparablevalue = "_numericvalue"}
-- {@dmlcolumnpredicate = "_numericcolumnpredicate"}
-- {@dmltable = "_table"}
-- {@fromtables = "_table"}
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@idcol = "ID"}
-- {@optionalfn = "_numfun"}
-- {@star = "*"}
-- {@lhsstar = "*"}
-- {@updatecolumn = "CASH"}
-- {@updatesource = "ID"}
-- {@updatevalue = "_value[decimal]"}

-- Additional required templates for the "advanced" SELECT queries:
-- {@distinctableagg = "_distinctableagg"}
-- {@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
-- {@plus10 = " + 10"}

-- Run both the "basic" and "advanced" SELECT query tests
<basic-select.sql>
<advanced-select.sql>

-- Run the basic UPDATE tests, and check the results afterward
<basic-update.sql>
SELECT @star FROM @dmltable   ST1
SELECT @star FROM @fromtables SV1

-- Run the basic DELETE tests, and check the results afterward
<basic-delete.sql>
SELECT @star FROM @dmltable   ST2
SELECT @star FROM @fromtables SV2
