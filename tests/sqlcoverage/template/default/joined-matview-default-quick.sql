<configure-default.sql>

-- Run SELECT queries against Views, rather than Tables - the quick version,
-- running against a few of the defined views, randomly selected
--{@fromtables = "V_value[int:0,36;6]"}

-- First, INSERT some data; then run both the "basic" and "advanced" SELECT
-- query tests, and the basic UPDATE and DELETE tests (with some extra SELECT
-- statements, to check those results)
<joined-matview-default-template.sql>
