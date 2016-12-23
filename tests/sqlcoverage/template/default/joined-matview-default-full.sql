<configure-default.sql>

-- Run SELECT queries against Views, rather than Tables - the complete version,
-- running against all of the defined views (may be slow)
{@fromtables = "V_value[int:0,36,1]"}

-- First, INSERT some data; then run both the "basic" and "advanced" SELECT
-- query tests, and the basic UPDATE and DELETE tests (with some extra SELECT
-- statements, to check those results)
<joined-matview-default-template.sql>
