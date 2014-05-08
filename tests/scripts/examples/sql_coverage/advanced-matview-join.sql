<grammar.sql>

{_basetables |= "P2"}
{_basetables |= "R2"}
{_basetables |= "R2V"}

{@aftermath = " _math _value[int:1,3]"}
{@agg = "_numagg"}
{@columnpredicate = "_variable[@comparabletype] _cmp _value[int16]"}
{@columntype = "int"}
{@comparableconstant = "42"}
{@comparabletype = "numeric"}
{@comparablevalue = "_numericvalue"}
{@dmlcolumnpredicate = "_variable[int] _cmp _value[int]"}

{@insertvals = "_id, 9, 9, 9, 9"}
{@idcol = "V_G1"}
{@numcol = "V_SUM_AGE"}

{@dmltable = "_basetables"}
{@fromtables = "_table"}

{@jointype = "_jointype"}

INSERT INTO @dmltable VALUES (_id, _value[int16], _value[int16], _value[int16 null20], _value[int16])
INSERT INTO @dmltable VALUES (_id, 1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, 1020, 1020, 1020, 1020)

INSERT INTO @dmltable VALUES (_id, -1010, 1010, 1010, 1010)
INSERT INTO @dmltable VALUES (_id, -1020, 1020, 1020, 1020)


--- Hsql bug: ENG-5362.
<basic-select.sql>
--- Remove the select template, the hsql problem may be reproduced.

--- Start to use the template file When ENG-5385 is fixed. The template includes two more join query templates for on clause. 
-- <join-template.sql>

--- Remove all the the query templates when ENG-5385 is fixed.
INSERT INTO @dmltable VALUES (@insertvals)

SELECT * FROM @fromtables LHS11 ,              @fromtables RHS WHERE                                 LHS11._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM @fromtables LHS12 ,              @fromtables RHS WHERE LHS12.@idcol = RHS.@idcol
SELECT * FROM @fromtables LHS13 ,              @fromtables RHS WHERE LHS13.@idcol = RHS.@idcol AND     RHS._variable[numeric] = 2 
SELECT * FROM @fromtables LHS14 ,              @fromtables RHS WHERE LHS14.@idcol = RHS.@idcol AND   LHS14._variable[numeric] = 2
SELECT * FROM @fromtables LHS15 ,              @fromtables RHS WHERE LHS15.@idcol = RHS.@idcol AND   LHS15._variable[@columntype] < 45 AND LHS15._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM @fromtables LHS16 ,              @fromtables RHS WHERE LHS16.@idcol = RHS.@idcol AND   LHS16.@numcol = RHS.@numcol AND   RHS.@idcol > 10 AND LHS16.@numcol < 30 AND LHS16.@numcol >= RHS.@idcol

SELECT * FROM @fromtables LHS21 @jointype JOIN @fromtables RHS ON                                    LHS21._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM @fromtables LHS22 @jointype JOIN @fromtables RHS ON    LHS22.@idcol = RHS.@idcol

SELECT * FROM @fromtables LHS25 @jointype JOIN @fromtables RHS ON    LHS25.@idcol = RHS.@idcol WHERE LHS25._variable[@columntype] < 45 AND LHS25._variable[@columntype] = RHS._variable[@comparabletype]
-- Suffers from ENG-6174
-- SELECT * FROM @fromtables LHS31 @jointype JOIN @fromtables RHS ON    LHS31.@idcol = RHS.@idcol AND     RHS.@idcol = 2
-- SELECT * FROM @fromtables LHS32 @jointype JOIN @fromtables RHS ON    LHS32.@idcol = RHS.@idcol AND   LHS32.@idcol = 2
-- These inequality variants may fare better for now?
   SELECT * FROM @fromtables LHS31 @jointype JOIN @fromtables RHS ON    LHS31.@idcol = RHS.@idcol AND     RHS.@idcol <> 2
   SELECT * FROM @fromtables LHS32 @jointype JOIN @fromtables RHS ON    LHS32.@idcol = RHS.@idcol AND   LHS32.@idcol <> 2

SELECT * FROM @fromtables LHS36 @jointype JOIN @fromtables RHS USING(      @idcol,                         @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @numcol >=     @idcol
SELECT * FROM @fromtables LHS37 @jointype JOIN @fromtables RHS USING(      @idcol,                         @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @idcol  =      @numcol
SELECT @idcol, @numcol FROM @fromtables LHS38 @jointype JOIN @fromtables RHS USING(     @idcol,            @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @numcol >=     @idcol
SELECT @idcol, @numcol FROM @fromtables LHS39 @jointype JOIN @fromtables RHS USING(     @idcol,            @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @idcol  =      @numcol


--- Three or more table outer join test
SELECT * FROM @fromtables LHS40 @jointype JOIN @fromtables MHS ON  LHS40.@idcol = MHS.@idcol  @jointype JOIN @fromtables RHS ON LHS40.@numcol = RHS.@numcol
SELECT @idcol, @numcol FROM @fromtables LHS40 @jointype JOIN @fromtables MHS ON  LHS40.@idcol = MHS.@idcol  @jointype JOIN @fromtables RHS ON LHS40.@numcol = RHS.@numcol


