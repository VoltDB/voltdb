-- DML, generate random data first.
--INSERT
-- test basic INSERT
INSERT INTO _table VALUES (@insertvals)

SELECT * FROM _table LHS11 ,              _table RHS WHERE                                 LHS11._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM _table LHS12 ,              _table RHS WHERE LHS12.@idcol = RHS.@idcol
SELECT * FROM _table LHS13 ,              _table RHS WHERE LHS13.@idcol = RHS.@idcol AND     RHS._variable[numeric] = 2 
SELECT * FROM _table LHS14 ,              _table RHS WHERE LHS14.@idcol = RHS.@idcol AND   LHS14._variable[numeric] = 2
SELECT * FROM _table LHS15 ,              _table RHS WHERE LHS15.@idcol = RHS.@idcol AND   LHS15._variable[@columntype] < 45 AND LHS15._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM _table LHS16 ,              _table RHS WHERE LHS16.@idcol = RHS.@idcol AND   LHS16.@numcol = RHS.@numcol AND   RHS.@idcol > 10 AND LHS16.@numcol < 30 AND LHS16.@numcol >= RHS.@idcol

SELECT * FROM _table LHS21 @jointype JOIN _table RHS ON                                    LHS21._variable[@columntype] = RHS._variable[@comparabletype]
SELECT * FROM _table LHS22 @jointype JOIN _table RHS ON    LHS22.@idcol = RHS.@idcol
-- These STILL need softening
-- SELECT * FROM _table LHS23 @jointype JOIN _table RHS ON    LHS23.@idcol = RHS.@idcol AND     RHS._variable[numeric] = 2
-- SELECT * FROM _table LHS24 @jointype JOIN _table RHS ON    LHS24.@idcol = RHS.@idcol AND   LHS24._variable[numeric] = 2
   SELECT * FROM _table LHS23 @jointype JOIN _table RHS ON    LHS23.@idcol = RHS.@idcol AND     RHS.@numcol = 2
   SELECT * FROM _table LHS24 @jointype JOIN _table RHS ON    LHS24.@idcol = RHS.@idcol AND   LHS24.@numcol = 2
SELECT * FROM _table LHS25 @jointype JOIN _table RHS ON    LHS25.@idcol = RHS.@idcol WHERE LHS25._variable[@columntype] < 45 AND LHS25._variable[@columntype] = RHS._variable[@comparabletype]

-- Still triggers wrong answer from mis-partitioning?
SELECT * FROM _table LHS31 @jointype JOIN _table RHS ON    LHS31.@idcol = RHS.@idcol AND     RHS.@idcol = 2
SELECT * FROM _table LHS32 @jointype JOIN _table RHS ON    LHS32.@idcol = RHS.@idcol AND   LHS32.@idcol = 2

SELECT * FROM _table LHS36 @jointype JOIN _table RHS USING(      @idcol,                         @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @numcol >=     @idcol
SELECT * FROM _table LHS37 @jointype JOIN _table RHS USING(      @idcol,                         @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @idcol  =      @numcol
SELECT @idcol, @numcol FROM _table LHS38 @jointype JOIN _table RHS USING(     @idcol,            @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @numcol >=     @idcol
SELECT @idcol, @numcol FROM _table LHS39 @jointype JOIN _table RHS USING(     @idcol,            @numcol)              WHERE     @idcol > 10 AND       @numcol < 30 AND       @idcol  =      @numcol


--- Three or more table outer join test
SELECT * FROM _table LHS40 @jointype JOIN _table MHS ON  LHS40.@idcol = MHS.@idcol  @jointype JOIN _table RHS ON LHS40.@numcol = RHS.@numcol
SELECT @idcol, @numcol FROM _table LHS40 @jointype JOIN _table MHS ON  LHS40.@idcol = MHS.@idcol  @jointype JOIN _table RHS ON LHS40.@numcol = RHS.@numcol
