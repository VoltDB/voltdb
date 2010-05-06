-- This is the same as the basic ints template, but the DDL for this has
-- materialized views

{@insert_vals = "_value[id], _value[byte], _value[int16], _value[int64]"}
{@from_tables = "_table"}
{@cmp_type = "_value[int64]"}
{@assign_col = "BIG"}
{@assign_type = "_value[int64]"}

<basic-template.sql>
