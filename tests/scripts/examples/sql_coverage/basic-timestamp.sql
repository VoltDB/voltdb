-- Run the basic-template against DDL with timestamps

{@insert_vals = "_value[id], _value[date], _value[date], _value[int64]"}
{@from_tables = "_table"}
{@cmp_type = "_value[date]"}
{@assign_col = "PAST"}
{@assign_type = "_value[date]"}

<basic-template.sql>
