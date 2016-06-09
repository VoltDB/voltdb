-- This is a set of UPSERT statements that tests their basic functionality.
-- These statements expect that the includer will have set the template macros
-- like @comparabletype to have a value that makes sense for the schema under
-- test. So, if the configuration is testing string operations, you'll want a
-- line: {@comparabletype = "string"} in the template file that includes this
-- one.

-- Required preprocessor macros (with example values):
-- {@aftermath = " _math _value[int:1,3]"}
-- {@cmp = "_cmp"} -- all comparison operators (=, <>, !=, <, >, <=, >=)
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@comparablevalue = "_numericvalue"} -- TODO: use of this is currently commented out
-- {@dmltable = "_table"}
-- {@fromtables = "_table"}
-- {@idcol = "ID"}
-- {@insertcols = "ID, VCHAR, NUM, RATIO"}
-- {@insertvals = "_id, _value[string], _value[int16 null30], _value[float]"}
-- {@star = "*"}
-- {@plus10 = "+ 10"}
-- {@updatecolumn = "NUM"} -- TODO: use of this is currently commented out
-- {@updatevalue = "_value[byte]"}


-- Tests of UPSERT INTO VALUES, using all columns (with and without a column list)
UPSERT INTO @dmltable               VALUES (@insertvals)
UPSERT INTO @dmltable (@insertcols) VALUES (@insertvals)
-- Confirm the values that were "upserted"
SELECT * FROM @dmltable

-- ... using a subset of columns
UPSERT INTO @dmltable (_variable[id], _variable[@comparabletype])                             VALUES (_value[byte], @updatevalue)
UPSERT INTO @dmltable (_variable[@comparabletype], _variable[id])                             VALUES (@updatevalue, _value[byte])
UPSERT INTO @dmltable (_variable[id], _variable[@comparabletype], _variable[@comparabletype]) VALUES (_value[byte], @updatevalue, @updatevalue)
UPSERT INTO @dmltable (_variable[@comparabletype], _variable[id], _variable[@comparabletype]) VALUES (@updatevalue, _value[byte], @updatevalue)
UPSERT INTO @dmltable (_variable[@comparabletype], _variable[@comparabletype], _variable[id]) VALUES (@updatevalue, @updatevalue, _value[byte])
-- Confirm the values that were "upserted"
SELECT * FROM @dmltable

-- Tests of UPSERT INTO SELECT, using all columns (with and without a column list)
UPSERT INTO @dmltable               SELECT @insertcols FROM @fromtables ORDER BY @idcol
UPSERT INTO @dmltable (@insertcols) SELECT @insertcols FROM @fromtables ORDER BY @idcol

-- ... using SELECT *
UPSERT INTO @dmltable               SELECT @star       FROM @fromtables ORDER BY @idcol
UPSERT INTO @dmltable (@insertcols) SELECT @star       FROM @fromtables ORDER BY @idcol
-- Confirm the values that were "upserted"
SELECT * FROM @dmltable

-- ... using a WHERE clause, with comparison ops (<, <=, =, >=, >)
UPSERT INTO @dmltable               SELECT @insertcols FROM @fromtables WHERE @columnpredicate ORDER BY @idcol
UPSERT INTO @dmltable (@insertcols) SELECT @insertcols FROM @fromtables WHERE @columnpredicate ORDER BY @idcol
--- ... with logic operators (AND, OR) and comparison ops
-- Commenting these out, because they yield too many statements, which is slow,
-- and they don't add that much value:
--UPSERT INTO @dmltable               SELECT @insertcols FROM @fromtables WHERE (@updatecolumn @cmp @comparablevalue) _logicop @columnpredicate  ORDER BY @idcol
--UPSERT INTO @dmltable (@insertcols) SELECT @insertcols FROM @fromtables WHERE (@updatecolumn @cmp @comparablevalue) _logicop @columnpredicate  ORDER BY @idcol
--- ... with arithmetic operators (+, -, *, /) and comparison ops
UPSERT INTO @dmltable               SELECT @insertcols FROM @fromtables WHERE (_variable[@comparabletype] @aftermath) @cmp @comparableconstant ORDER BY @idcol
UPSERT INTO @dmltable (@insertcols) SELECT @insertcols FROM @fromtables WHERE (_variable[@comparabletype] @aftermath) @cmp @comparableconstant ORDER BY @idcol
-- Confirm the values that were "upserted"
SELECT * FROM @dmltable

-- ... using arithmetic (+, -, *, /) ops in the SELECT clause
UPSERT INTO @dmltable (_variable[id], _variable[#c2 @comparabletype])                                 SELECT @idcol + 100, __[#c2] @plus10                   FROM @fromtables ORDER BY @idcol
UPSERT INTO @dmltable (_variable[#c2 @comparabletype], _variable[id])                                 SELECT __[#c2] @plus10, @idcol + 200                   FROM @fromtables ORDER BY @idcol
UPSERT INTO @dmltable (_variable[id], _variable[#c2 @comparabletype], _variable[#c3 @comparabletype]) SELECT @idcol + 1000, __[#c2] @plus10, __[#c3] @plus10 FROM @fromtables ORDER BY @idcol
-- TODO, see ENG-10458: Commenting these out, because they cause
-- "timeout: procedure call took longer than 5 seconds" errors:
--UPSERT INTO @dmltable (_variable[#c2 @comparabletype], _variable[id], _variable[#c3 @comparabletype]) SELECT __[#c2] @plus10, @idcol + 2000, __[#c3] @plus10 FROM @fromtables ORDER BY @idcol
--UPSERT INTO @dmltable (_variable[#c2 @comparabletype], _variable[#c3 @comparabletype], _variable[id]) SELECT __[#c2] @plus10, __[#c3] @plus10, @idcol + 3000 FROM @fromtables ORDER BY @idcol
-- Confirm the values that were "upserted"
SELECT * FROM @dmltable
