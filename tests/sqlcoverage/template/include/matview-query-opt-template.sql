-- This file tests GROUP BY queries that match a query found in a materialized
-- view on the same table. Such queries were recently optimized for performance
-- purposes (see ENG-2878, July 2018); these tests ensure that the answers are
-- still correct. Note that the GROUP BY query being tested need not exactly
-- match the one defined in a materialized view: it may use a subset of the
-- columns from the view, or the columns may be in a different order.

-- Required preprocessor macros (with example values):
-- {@agg = "_numagg"}
-- {@comparabletype = "numeric"} TODO: don't need this ???
-- {@dmltable = "_table"}
-- {@insertvals = "_id, _value[byte], _value[byte], _value[byte], _value[byte]"}

-- First, purge and regenerate random data
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- Next, add additional data that guarantees some non-trivial results in
-- GROUP BY queries, including those in materialized view definitions
INSERT INTO @dmltable VALUES (_id,  100,  10,  30,  20)
INSERT INTO @dmltable VALUES (_id,  100,  10,  30,  20)
INSERT INTO @dmltable VALUES (_id,  100,  10,  30,  21)
INSERT INTO @dmltable VALUES (_id,  100,  10,  30,  21)
INSERT INTO @dmltable VALUES (_id,  100,  10,  40,  22)
INSERT INTO @dmltable VALUES (_id,  100,  10,  40,  22)
INSERT INTO @dmltable VALUES (_id,  100,  10,  40,  23)
INSERT INTO @dmltable VALUES (_id,  100,  10,  40,  23)
INSERT INTO @dmltable VALUES (_id,  100,  11,  30,  20)
INSERT INTO @dmltable VALUES (_id,  100,  11,  30,  20)
INSERT INTO @dmltable VALUES (_id,  100,  11,  30,  21)
INSERT INTO @dmltable VALUES (_id,  100,  11,  30,  21)
INSERT INTO @dmltable VALUES (_id,  100,  11,  40,  22)
INSERT INTO @dmltable VALUES (_id,  100,  11,  40,  22)
INSERT INTO @dmltable VALUES (_id,  100,  11,  40,  23)
INSERT INTO @dmltable VALUES (_id,  100,  11,  40,  23)

INSERT INTO @dmltable VALUES (_id, -100,  10,  30,  20)
INSERT INTO @dmltable VALUES (_id, -100,  10,  30,  20)
INSERT INTO @dmltable VALUES (_id, -100,  10,  30,  21)
INSERT INTO @dmltable VALUES (_id, -100,  10,  30,  21)
INSERT INTO @dmltable VALUES (_id, -100,  10,  40,  22)
INSERT INTO @dmltable VALUES (_id, -100,  10,  40,  22)
INSERT INTO @dmltable VALUES (_id, -100,  10,  40,  23)
INSERT INTO @dmltable VALUES (_id, -100,  10,  40,  23)
INSERT INTO @dmltable VALUES (_id, -100,  11,  30,  20)
INSERT INTO @dmltable VALUES (_id, -100,  11,  30,  20)
INSERT INTO @dmltable VALUES (_id, -100,  11,  30,  21)
INSERT INTO @dmltable VALUES (_id, -100,  11,  30,  21)
INSERT INTO @dmltable VALUES (_id, -100,  11,  40,  22)
INSERT INTO @dmltable VALUES (_id, -100,  11,  40,  22)
INSERT INTO @dmltable VALUES (_id, -100,  11,  40,  23)
INSERT INTO @dmltable VALUES (_id, -100,  11,  40,  23)

-- Define "place-holders" used in the queries below
{_groupby1 |= "ABS(WAGE)"}
{_groupby1 |=     "WAGE"}
{_groupby1 |=     "DEPT"}
{_groupby1 |=     "AGE"}
{_groupby2 |= "ABS(WAGE), DEPT"}
{_groupby2 |=     "WAGE,  DEPT"}
{_groupby2 |=     "DEPT,  WAGE"}
{_groupby2 |=     "DEPT, ABS(WAGE)"}
-- Deliberate typo (no comma), which reproduces ENG-TBD
-- TODO: uncomment once ENG-TBD is fixed
--{_groupby2 |=     "WAGE   DEPT"}
{_groupby3 |= "WAGE, DEPT, AGE"}
{_groupby3 |= "AGE,  WAGE, DEPT"}
{_groupby3 |= "DEPT, AGE,  WAGE"}
{_groupby4 |= "WAGE, DEPT, AGE,  RENT"}
{_groupby4 |= "RENT, AGE,  DEPT, WAGE"}

{_opthaving |= ""}
{_opthaving |= "HAVING @agg(AGE) <= @comparableconstant"}
--{_opthaving |= "HAVING @agg(RENT) >= @comparableconstant ORDER BY 4, 2"}
--{_opthaving |= "HAVING @agg(_variable[@comparabletype]) @somecmp @comparableconstant"}
--{_opthaving |= "HAVING @agg(_variable[@comparabletype]) @somecmp @comparableconstant ORDER BY 4, 2 _optionallimitoffset"}

{_aggr5 |= "MIN(WAGE), COUNT(DEPT), MAX(AGE),  SUM(RENT),   COUNT(ID)"}
{_aggr5 |= "COUNT(ID),   SUM(RENT), MAX(AGE),  COUNT(DEPT), MIN(WAGE)"}
{_aggr4 |= "MIN(WAGE), COUNT(DEPT), MAX(AGE),  SUM(RENT)"}

{_aggr4 |= "COUNT(DEPT), MIN(AGE),  SUM(RENT), MAX(ID)"}
{_aggr4 |= "MAX(ID),     SUM(RENT), MIN(AGE),  COUNT(DEPT)"}
{_aggr3 |= "COUNT(DEPT), MIN(AGE),  SUM(RENT)"}
{_aggr3 |= "MAX(AGE),  COUNT(DEPT), MIN(WAGE)"}

{_aggr3 |= "MIN(AGE),    SUM(RENT), MAX(ID)"}
{_aggr3 |= "MAX(ID),     MIN(AGE),  SUM(RENT)"}
{_aggr3 |= "COUNT(RENT), MIN(ID),   MAX(AGE)"}
{_aggr3 |= "MIN(ID),   COUNT(RENT), MAX(AGE)"}
{_aggr2 |= "MIN(AGE),    SUM(RENT)"}
{_aggr2 |= "MAX(AGE),    SUM(RENT)"}

{_aggr2 |= "SUM(RENT),   SUM(ID)"}
{_aggr2 |= "SUM(ID),     SUM(RENT)"}
{_aggr1 |= "SUM(RENT)"}
{_aggr1 |= "SUM(ID)"}

{_aggr1 |= "MIN(ID)"}
{_aggr1 |= "MAX(ID)"}

{_whereopts |= ""}
{_whereopts |= "WHERE ABS(WAGE) < 60   AND  ABS(AGE)  >= 30  AND   ABS(AGE) < 65"}
{_whereopts |= "WHERE ABS(WAGE) < 60   AND  ABS(AGE)  BETWEEN      30 AND 64"}
{_whereopts |= "WHERE DEPT  IN    (1, 2, 3, 5, 11, 16, 27, 43, 70, 113)"}

-- Then, run some queries specifically designed to exercise the optimization of
-- queries that are similar to a materialized view definition; note that we
-- actually query the @dmltable's here (i.e., the tables), not the @fromtables
-- (i.e., the views), which is unusual, but necessary in this case

-- Queries that mostly match matview definitions (possibly out-of-order),
-- with 0, 1, 2, 3, or 4 GROUP BY columns
SELECT            COUNT(*), _aggr5 FROM @dmltable M01 _whereopts
SELECT _groupby1, COUNT(*), _aggr4 FROM @dmltable M02 _whereopts GROUP BY _groupby1 _opthaving
SELECT _groupby2, COUNT(*), _aggr3 FROM @dmltable M03 _whereopts GROUP BY _groupby2 _opthaving
SELECT _groupby3, COUNT(*), _aggr2 FROM @dmltable M04 _whereopts GROUP BY _groupby3 _opthaving
SELECT _groupby4, COUNT(*), _aggr1 FROM @dmltable M05 _whereopts GROUP BY _groupby4 _opthaving

-- Queries with a subset of group-by columns in SELECT list
SELECT _groupby1, COUNT(*), _aggr3 FROM @dmltable M10 _whereopts GROUP BY _groupby2
SELECT _groupby2, COUNT(*), _aggr2 FROM @dmltable M11 _whereopts GROUP BY _groupby3
SELECT _groupby1, COUNT(*), _aggr2 FROM @dmltable M12 _whereopts GROUP BY _groupby3
SELECT _groupby3, COUNT(*), _aggr1 FROM @dmltable M13 _whereopts GROUP BY _groupby4
SELECT _groupby2, COUNT(*), _aggr1 FROM @dmltable M14 _whereopts GROUP BY _groupby4
SELECT _groupby1, COUNT(*), _aggr1 FROM @dmltable M15 _whereopts GROUP BY _groupby4

-- Queries with a subset of aggregate columns in SELECT list
SELECT            COUNT(*), _aggr4 FROM @dmltable M20 _whereopts
SELECT            COUNT(*), _aggr3 FROM @dmltable M21 _whereopts
SELECT            COUNT(*), _aggr2 FROM @dmltable M22 _whereopts
SELECT            COUNT(*), _aggr1 FROM @dmltable M23 _whereopts
SELECT _groupby1, COUNT(*), _aggr3 FROM @dmltable M24 _whereopts GROUP BY _groupby1
SELECT _groupby1, COUNT(*), _aggr2 FROM @dmltable M25 _whereopts GROUP BY _groupby1
SELECT _groupby1, COUNT(*), _aggr1 FROM @dmltable M26 _whereopts GROUP BY _groupby1
SELECT _groupby2, COUNT(*), _aggr2 FROM @dmltable M27 _whereopts GROUP BY _groupby2
SELECT _groupby2, COUNT(*), _aggr1 FROM @dmltable M28 _whereopts GROUP BY _groupby2
SELECT _groupby3, COUNT(*), _aggr1 FROM @dmltable M29 _whereopts GROUP BY _groupby3

-- Queries with a minimum of both group-by columns and aggregate columns in SELECT list
SELECT _groupby1, COUNT(*), _aggr1 FROM @dmltable M30 _whereopts GROUP BY _groupby2
SELECT _groupby1, COUNT(*), _aggr1 FROM @dmltable M31 _whereopts GROUP BY _groupby3
