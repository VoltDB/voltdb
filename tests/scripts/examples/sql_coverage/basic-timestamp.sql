-- Run the basic-template against DDL with timestamps

{@insert_vals = "_id, _value[date], _value[date], _value[int64]"}
{@from_tables = "_table"}
{@col_type = "date"}
{@cmp_type = "_value[date]"}
{@id_col = "ID"}
{@assign_col = "PAST"}
{@assign_type = "_value[date]"}

<basic-template.sql>
