-- Run the basic-template against the default tables using two table joins

{@insert_vals = "_id, _value[string], _value[int32], _value[float]"}
{@from_tables = "_table, _table"}
{@col_type = "int"}
{@cmp_type = "_value[int:0,100]"}
{@id_col = "ID"}
{@assign_col = "NUM"}
{@assign_type = "_value[int:0,100]"}
{@set_op = "_pick[<options=UNION,INTERSECT,EXCEPT>]"}
{@optional_all = "_pick[<options=,ALL>]"}

<union-template.sql>
