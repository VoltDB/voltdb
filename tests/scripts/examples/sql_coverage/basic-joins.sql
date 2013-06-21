-- Run the join-template against the default table in schema.py

{@insert_vals = "_id, _value[string], _value[int16], _value[float]"}
{@from_tables = "_table"}
{@col_type = "int"}
{@cmp_type = "_value[int:0,100]"}
{@id_col = "ID"}
{@num_col = "NUM"}
{@join_type = "_pick[<options=,INNER,LEFT,RIGHT>]"}

<join-template.sql>
