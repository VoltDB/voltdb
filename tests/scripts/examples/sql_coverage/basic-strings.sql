-- Run the basic-template against the default tables using two table joins

{@insert_vals = "_value[id], _value[string], _value[string], _value[float]"}
{@from_tables = "_table"}
{@cmp_type = "_value[string]"}
{@assign_col = "DESC"}
{@assign_type = "_value[string]"}

<basic-template.sql>
