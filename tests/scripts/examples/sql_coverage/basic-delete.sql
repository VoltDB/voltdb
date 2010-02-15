-- This is a set of UPDATE statements that tests the bare minimum
-- necessary to start to think that maybe we actually support the
-- subset of SQL that we claim to support.
--
-- In the brave new meta-template world (yeah, okay, kill me), these
-- statements expect that the includer will have set the template
-- @cmp_type to be the _value[???] type that makes sense for the
-- config under test.  So, if the configuration is pushing on string
-- operations, you'll want a line: {@cmp_type = "_value[string]"} in
-- the template file that includes this one.
--
-- Required preprocessor templates:
--
-- @cmp_type: the type of literal to use for comparisons
-- @insert_vals: the list of types to be used to fill in the columns
--               for INSERT

--DELETE
-- Delete them all, then re-insert, then do trickier deletions
-- test basic DELETE
DELETE FROM _table
INSERT INTO _table VALUES (@insert_vals)
INSERT INTO _table VALUES (@insert_vals)
INSERT INTO _table VALUES (@insert_vals)
-- test where expressions
--- test logic operators (AND) with comparison ops
DELETE FROM _table WHERE (_variable _cmp _variable) _logic (_variable _cmp @cmp_type)
INSERT INTO _table VALUES (@insert_vals)
INSERT INTO _table VALUES (@insert_vals)
INSERT INTO _table VALUES (@insert_vals)
--- test arithmetic operators (+, -, *, /) with comparison ops
-- XXX COMMENTING THIS OUT UNTIL TICKET 201 IS RESOLVED
DELETE FROM _table WHERE (_variable _math _value[int:0,100]) _cmp @cmp_type
INSERT INTO _table VALUES (@insert_vals)
INSERT INTO _table VALUES (@insert_vals)
INSERT INTO _table VALUES (@insert_vals)
--- test comparison ops (<, <=, =, >=, >)
DELETE FROM _table WHERE _variable _cmp @cmp_type
