-- This file tests patterns that are more complex or differ from
-- the SQL tested in the basic-template.sql file

-- Required preprocessor macros (with example values):
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@aftermath = " _math _value[int:1,3]"}
-- {@agg = "_numagg"}
-- {@distinctableagg = "_distinctableagg"}
-- {@cmp = "_cmp"} -- all comparison operators (=, <>, !=, <, >, <=, >=)
-- {@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=)
-- {@columntype = "decimal"}
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@dmltable = "_table"}
-- {@fromtables = "_table"}
-- {@optionalfn = "_numfun"}
-- {@plus10 = " + 10"}
-- {@star = "*"}
-- {@updatecolumn = "CASH"}
-- {@updatevalue = "_value[decimal]"}

-- DML, clean out and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- Run the "advanced" SELECT queries (which are now in a separate file, so they
-- can be called without the DELETE and INSERT statements above, when desired)
<advanced-select.sql>
