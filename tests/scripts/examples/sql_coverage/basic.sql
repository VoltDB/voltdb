-- Run the basic-template against the default table in schema.py

-- reducing the random values to int16 until overflow detection works
--{@insert_vals = "_id, _value[string], _value[int32], _value[float]"}
{@insert_vals = "_id, _value[string], _value[int16], _value[float]"}
{@from_tables = "_table"}
{@col_type = "int"}
{@cmp_type = "_value[int:0,100]"}
{@id_col = "ID"}
{@assign_col = "NUM"}
{@assign_type = "_value[int:0,100]"}

<basic-template.sql>
