-- Run the basic-template against the default tables using two table joins

{@insert_vals = "_id, _value[string], _value[string], _value[float]"}
{@from_tables = "_table"}
{@col_type = "string"}
{@cmp_type = "_value[string]"}
{@id_col = "ID"}
{@assign_col = "DESC"}
{@assign_type = "_value[string]"}

<basic-template.sql>
