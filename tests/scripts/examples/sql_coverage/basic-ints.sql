-- Run the basic-template against DDL with all INT types

{@insert_vals = "_value[id], _value[byte], _value[int16], _value[int64]"}
{@from_tables = "_table"}
{@col_type = "int64"}
{@cmp_type = "_value[int64]"}
{@assign_col = "BIG"}
{@assign_type = "_value[int64]"}

<basic-template.sql>
