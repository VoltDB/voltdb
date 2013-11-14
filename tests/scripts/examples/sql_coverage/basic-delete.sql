-- This is a set of DELETE statements that tests the bare minimum
-- necessary to start to think that maybe we actually support the
-- subset of SQL that we claim to support.
--
-- In the brave new meta-template world (yeah, okay, kill me), these
-- statements expect that the includer will have set the template macros like
-- @comparabletype to have a value that makes sense for the schema under test.
-- So, if the configuration is pushing on string
-- operations, you'll want a line: {@comparabletype = "string"} in
-- the template file that includes this one.

-- Required preprocessor macros (with example values):
-- {@aftermath = " _math _value[int:1,3]"}
-- {@columnpredicate = "_numericcolumnpredicate"}
-- {@comparableconstant = "42.42"}
-- {@comparabletype = "numeric"}
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}

--SELECT
--DELETE
-- Delete them all, then re-insert, then do trickier deletions
-- test basic DELETE
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (@insertvals)
-- test where expressions
--- test comparison ops (<, <=, =, >=, >)
DELETE FROM @dmltable WHERE @columnpredicate
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (@insertvals)
--- test logic operators (AND) with comparison ops
DELETE FROM @dmltable WHERE (_variable[@columntype] _cmp @comparableconstant) _logicop @columnpredicate
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (@insertvals)
INSERT INTO @dmltable VALUES (@insertvals)
--- test arithmetic operators (+, -, *, /) with comparison ops
DELETE FROM @dmltable WHERE (_variable[@comparabletype] @aftermath) _cmp @comparableconstant

