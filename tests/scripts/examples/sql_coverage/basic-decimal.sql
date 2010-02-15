-- Run the basic-template against DDL with decimal

{@insert_vals = "_value[id], _value[decimal], _value[decimal], _value[float]"}
{@from_tables = "_table"}
{@cmp_type = "_value[decimal]"}
{@assign_col = "CASH"}
{@assign_type = "_value[decimal]"}

<basic-template.sql>
