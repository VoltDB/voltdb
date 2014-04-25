-- DML, generate random data first.
--INSERT
-- test basic INSERT
INSERT INTO @dmltable VALUES (@insertvals)

SELECT * FROM @fromtables LHS11 ,              @fromtables RHS WHERE                                 LHS11._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM @fromtables LHS12 ,              @fromtables RHS WHERE LHS12.@idcol = RHS.@idcol
SELECT * FROM @fromtables LHS13 ,              @fromtables RHS WHERE LHS13.@idcol = RHS.@idcol AND     RHS._variable[numeric] = 2 
SELECT * FROM @fromtables LHS14 ,              @fromtables RHS WHERE LHS14.@idcol = RHS.@idcol AND   LHS14._variable[numeric] = 2
SELECT * FROM @fromtables LHS15 ,              @fromtables RHS WHERE LHS15.@idcol = RHS.@idcol AND   LHS15._variable[@columntype] < 45 AND LHS15._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM @fromtables LHS16 ,              @fromtables RHS WHERE LHS16.@idcol = RHS.@idcol AND   LHS16.@numcol = RHS.@numcol AND   RHS.@idcol > 10 AND LHS16.@numcol < 30 AND LHS16.@numcol >= RHS.@idcol

SELECT * FROM @fromtables LHS21 @jointype JOIN @fromtables RHS ON                                    LHS21._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM @fromtables LHS22 @jointype JOIN @fromtables RHS ON    LHS22.@idcol = RHS.@idcol
-- These STILL need softening
-- SELECT * FROM @fromtables LHS23 @jointype JOIN @fromtables RHS ON    LHS23.@idcol = RHS.@idcol AND     RHS._variable[numeric] = 2
-- SELECT * FROM @fromtables LHS24 @jointype JOIN @fromtables RHS ON    LHS24.@idcol = RHS.@idcol AND   LHS24._variable[numeric] = 2
-- Also, the equality test is too selective until ENG-6174 is fixed, so even this will not work:
-- SELECT * FROM @fromtables LHS23 @jointype JOIN @fromtables RHS ON    LHS23.@idcol = RHS.@idcol AND     RHS.@numcol = 2
-- SELECT * FROM @fromtables LHS24 @jointype JOIN @fromtables RHS ON    LHS24.@idcol = RHS.@idcol AND   LHS24.@numcol = 2
-- Even these new inequality variants suffer from ENG-6204
-- SELECT * FROM @fromtables LHS23 @jointype JOIN @fromtables RHS ON    LHS23.@idcol = RHS.@idcol AND     RHS.@numcol <> 2
-- SELECT * FROM @fromtables LHS24 @jointype JOIN @fromtables RHS ON    LHS24.@idcol = RHS.@idcol AND   LHS24.@numcol <> 2

SELECT * FROM @fromtables LHS25 @jointype JOIN @fromtables RHS ON    LHS25.@idcol = RHS.@idcol WHERE LHS25._variable[@columntype] < 45 AND LHS25._variable[@columntype] = RHS._variable[@comparabletype]

-- Still triggers wrong answer from mis-partitioning?
-- And suffers from ENG-6174
-- SELECT * FROM @fromtables LHS31 @jointype JOIN @fromtables RHS ON    LHS31.@idcol = RHS.@idcol AND     RHS.@idcol = 2
-- SELECT * FROM @fromtables LHS32 @jointype JOIN @fromtables RHS ON    LHS32.@idcol = RHS.@idcol AND   LHS32.@idcol = 2
-- These inequality variants may fare better for now?
   SELECT * FROM @fromtables LHS31 @jointype JOIN @fromtables RHS ON    LHS31.@idcol = RHS.@idcol AND     RHS.@idcol <> 2
   SELECT * FROM @fromtables LHS32 @jointype JOIN @fromtables RHS ON    LHS32.@idcol = RHS.@idcol AND   LHS32.@idcol <> 2

SELECT * FROM @fromtables LHS36 @jointype JOIN @fromtables RHS USING(      @idcol,                         @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @numcol >=     @idcol
SELECT * FROM @fromtables LHS37 @jointype JOIN @fromtables RHS USING(      @idcol,                         @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @idcol  =      @numcol
SELECT @idcol, @numcol FROM @fromtables LHS38 @jointype JOIN @fromtables RHS USING(     @idcol,            @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @numcol >=     @idcol
SELECT @idcol, @numcol FROM @fromtables LHS39 @jointype JOIN @fromtables RHS USING(     @idcol,            @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @idcol  =      @numcol

--- Outer join with NULL padding.
SELECT * FROM @fromtables LHS39 @jointype JOIN @fromtables RHS ON  LHS39.@idcol = RHS.@idcol AND LHS39.@numcol = RHS.@numcol  WHERE  RHS.@numcol IS NULL
-- ENG-6113: Hsql has wrong answers on this templates.
-- SELECT * FROM @fromtables LHS39 @jointype JOIN @fromtables RHS ON  LHS39.@idcol = RHS.@idcol AND LHS39.@numcol = RHS.@numcol  WHERE  LHS39.@numcol IS NULL

--- Three or more table outer join test
SELECT *               FROM @fromtables LHS40 @jointype JOIN @fromtables MHS ON  LHS40.@idcol = MHS.@idcol  @jointype JOIN @fromtables RHS ON LHS40.@numcol = RHS.@numcol
SELECT @idcol, @numcol FROM @fromtables LHS41 @jointype JOIN @fromtables MHS ON  LHS41.@idcol = MHS.@idcol  @jointype JOIN @fromtables RHS ON LHS41.@numcol = RHS.@numcol
