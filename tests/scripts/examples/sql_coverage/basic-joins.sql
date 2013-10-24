<configure-for-joins.sql>

-- Run the join-template against the default table in schema.py
-- Run the template against DDL with a mix of types
-- Keep the value scaled down here to prevent internal precision issues when dividing by constants > 20?

{@idcol = "ID"}
{@numcol = "NUM"}
{@fromtables = "_table"}

-- Repeat queries with forced data value overlaps between tables.
<join-template.sql>

-- Force some non-random values to get overlaps -- yes sadly this breaks the schema-independence of the test.
INSERT INTO _table VALUES (1000, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1001, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1010, 'desc_1010', 1010, 1010.5)
INSERT INTO _table VALUES (1011, 'desc_1010', 1010, 1010.5)
-- Purposely excluding rows from some _tables to tease out different cases.
INSERT INTO P1 VALUES (1020, 'desc_1020', 1020, 1020.5)
INSERT INTO R1 VALUES (1020, 'desc_1020', 1020, 1020.5)
INSERT INTO R2 VALUES (1020, 'desc_1020', 1020, 1020.5)

-- Repeat queries with forced data value overlaps between tables.
<join-template.sql>
