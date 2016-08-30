-- BASIC SQL Coverage cases.  These represent more-or-less the
-- simplest possible templates for SQL statements that cover the SQL
-- keywords that we want to support.
--
-- Required preprocessor template:
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

-- DML, purge and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

<basic-select.sql>
<basic-update.sql>
<basic-delete.sql>
