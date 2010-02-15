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
-- @assign_col: the column to be updated
-- @assign_type: the type of literal to use to generate the update value

--UPDATE
-- Update tests in reverse complexity order since they have persistent effects
-- test where expressions

-- Explicitly using @assign_col instead of _variable here in the SET since we
-- don't fail gracefully if the types of the assignment can't be
-- implicitly cast.  See Ticket 200

--- test logic operators (AND) with comparison ops
UPDATE _table SET @assign_col = @assign_type WHERE (_variable _cmp _variable) _logic (_variable _cmp @cmp_type)
--- test arithmetic operators (+, -, *, /) with comparison ops
-- XXX COMMENTING THIS OUT UNTIL TICKET 201 IS RESOLVED
UPDATE _table SET @assign_col = @assign_type WHERE (_variable _math _value[int:0,100]) _cmp @cmp_type
--- test comparison ops (<, <=, =, >=, >)
UPDATE _table SET @assign_col = @assign_type WHERE _variable _cmp @cmp_type
-- test set expression
--- test arithmetic (+, -, *, /) ops
UPDATE _table SET @assign_col = @assign_col _math _value[int:0,3]
-- test simple update
UPDATE _table SET @assign_col = @assign_type
