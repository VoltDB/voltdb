-- Run the basic-template against DDL with decimal

{@insert_vals = "_value[id], _value[decimal], _value[decimal], _value[float]"}
{@from_tables = "_table"}
{@col_type = "decimal"}
{@cmp_type = "_value[decimal]"}
{@id_col = "ID"}
{@assign_col = "CASH"}
{@assign_type = "_value[decimal]"}

<basic-template.sql>
