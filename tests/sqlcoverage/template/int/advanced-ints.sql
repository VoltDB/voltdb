<configure-for-ints.sql>
<advanced-template.sql>

-- Additional test of GROUP BY and aggregate functions, with CAST function (like ENG-18549, but w/o ?)
SELECT _variable[#GB], CAST(_value[byte] AS INTEGER) + @agg(_variable[@columntype]) FROM @fromtables Q44 GROUP BY __[#GB]
