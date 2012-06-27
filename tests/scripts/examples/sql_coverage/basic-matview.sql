-- This is the same as the basic ints template, but the DDL for this has
-- materialized views

{@insert_vals = "_value[id], _value[byte], _value[int16], _value[int64]"}
{@from_tables = "_table"}
{@col_type = "int64"}
{@cmp_type = "_value[int64]"}
{@id_col = "ID"}
{@assign_col = "BIG"}
{@assign_type = "_value[int64]"}

<basic-template.sql>
