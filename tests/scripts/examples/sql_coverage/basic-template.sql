-- BASIC SQL Coverage cases.  These represent more-or-less the
-- simplest possible templates for SQL statements that cover the SQL
-- keywords that we want to support for a version 1 release.
--
-- Required preprocessor template:
-- @insert_vals: the list of types of the table columns for INSERT
-- @cmp_type
-- @assign_col
-- @assign_type

-- DML, generate random data first.
--INSERT
-- test basic INSERT
INSERT INTO _table VALUES (@insert_vals)

<basic-select.sql>
<basic-update.sql>
<basic-delete.sql>
