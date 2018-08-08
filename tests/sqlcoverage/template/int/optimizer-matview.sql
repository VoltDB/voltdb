<grammar.sql>

{_basetables |= "P2"}
{_basetables |= "R2"}
{_basetables |= "R2V"}

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@cmp = "_cmp"} -- use all comparison operators (=, <>, !=, <, >, <=, >=)
{@somecmp = "_somecmp"} -- a smaller list of comparison operators (=, <, >=) [not used here?]
{@columnpredicate = "_variable[@comparabletype] @cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] @cmp _value[int]"}

{@insertvals = "_id, 9, 9, 9, 9"}
{@idcol = "V_G1"}
{@numcol = "V_SUM_AGE"}
{@star = "*"}
{@lhsstar = "*"}

{@dmltable = "_basetables"}
{@fromtables = "_table"}

INSERT INTO @dmltable VALUES (_id, _value[byte], _value[byte], _value[byte null20], _value[byte])
INSERT INTO @dmltable VALUES (_id, 1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, 1020, 1020, 1020, 1020)

INSERT INTO @dmltable VALUES (_id, -1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, -1020, 1020, 1020, 1020)

--- Hsql bug: ENG-5362.
<basic-select.sql>

INSERT INTO @dmltable VALUES (@insertvals)

-- For ENG-2878, no joins are involved.
SELECT dept, sum(age), sum(rent) FROM @fromtables WHERE age < _value[byte:25,60] GROUP BY wage, dept LIMIT 25
SELECT dept, sum(age), sum(rent) FROM @fromtables WHERE age > _value[byte:25,60] GROUP BY dept, wage
SELECT dept, sum(age), sum(rent) FROM @fromtables WHERE age > _value[byte:25,60] GROUP BY dept, wage LIMIT 15
SELECT dept, sum(age), sum(rent) FROM @fromtables WHERE age < 30 AND RENT > _value[byte:15,20] AND WAGE < _value[byte:50,80] GROUP BY dept, wage

