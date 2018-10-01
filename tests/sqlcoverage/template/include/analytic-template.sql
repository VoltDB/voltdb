-- This file holds patterns to test SQL Analytic (aka Window) functions, such
-- as RANK, which can only be tested against PostgreSQL, since HSQL does not
-- support them.
-- [Note: currently RANK and DENSE_RANK are the only such functions supported,
-- and tested here, but that may change in time.]

-- Required preprocessor macros (with example values):
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@comparabletype = "numeric"}
-- {@dmltable = "_table"}
-- {@fromtables = "_table"}
-- {@onefun = "ABS"}
-- {@plus10 = " + 10"}
-- {@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
-- {@star = "*"}
-- {@winagg = "_numwinagg"}

-- DML, clean out and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- Define expressions and clauses used to test windowed analytic functions (such as RANK)
{_columnorexpr |= "_variable"}
{_columnorexpr |= "@onefun(_variable[@comparabletype])@plus10"}
{_orderbycolumnorexpr |= "_variable[#ord @rankorderbytype]"}
{_orderbycolumnorexpr |= "@onefun(_variable[#ord @rankorderbytype])@plus10"}
{@orderbyclause = "ORDER BY _orderbycolumnorexpr _sortorder"}
{_maybeorderby |= ""}
{_maybeorderby |= "@orderbyclause"}
{@orderbyid = "ORDER BY _symbol[#ord @idcol] _sortorder"}

-- Define the actual windowed analytic functions being tested (such as RANK),
-- with arguments, if any
{_analyticfunc1 |= "RANK()"}
{_analyticfunc1 |= "DENSE_RANK()"}
{_analyticfunc1 |= "COUNT(*)"}
{_analyticfunc1 |= "@winagg(_columnorexpr)"}
-- Because ROW_NUMBER sometimes allows omitting ORDER BY where others require it,
-- yet omitting it makes the result non-deterministic, it is tested separately in
-- some of the query templates below, with strictly deterministic query templates
{_analyticfunc2 |= "ROW_NUMBER()"}
-- This is not currently used, but could be if we come up with query templates
-- that work with all of the above windowed analytic functions, including
-- ROW_NUMBER
{_analyticfunc3 |= "_analyticfunc1"}
{_analyticfunc3 |= "_analyticfunc2"}

-- Test SQL Analytic (Window) functions, such as RANK
-- ... without PARTITION BY clause:
SELECT               @star,  _analyticfunc1 OVER (                                                         @orderbyclause) FUNC            FROM @fromtables W01A
SELECT                       _analyticfunc1 OVER (                                                         @orderbyclause) FUNC, @star     FROM @fromtables W02A
-- ... with PARTITION BY clause:
SELECT               @star,  _analyticfunc1 OVER (PARTITION BY __[#ord]                                    @orderbyclause) FUNC            FROM @fromtables W03A
SELECT            __[#ord],  _analyticfunc1 OVER (PARTITION BY _variable[#part]                            @orderbyclause) FUNC, __[#part] FROM @fromtables W04A
SELECT __[#ord], __[#part],  _analyticfunc1 OVER (PARTITION BY _variable[#part]                            @orderbyclause) FUNC            FROM @fromtables W05A

SELECT       _columnorexpr,  _analyticfunc1 OVER (PARTITION BY _variable                                    _maybeorderby) FUNC            FROM @fromtables W06A
SELECT                       _analyticfunc1 OVER (PARTITION BY _columnorexpr                                _maybeorderby) FUNC, @star     FROM @fromtables W07A
SELECT               @star,  _analyticfunc1 OVER (PARTITION BY _columnorexpr, _columnorexpr                 _maybeorderby) FUNC            FROM @fromtables W08A
SELECT                       _analyticfunc1 OVER (PARTITION BY _columnorexpr, _columnorexpr, _columnorexpr  _maybeorderby) FUNC, @star     FROM @fromtables W09A

-- Test a SQL Analytic (Window) function used in a sub-query, with PARTITION BY clause
SELECT * FROM (SELECT @star, _analyticfunc1 OVER (PARTITION BY _columnorexpr                                _maybeorderby) SUBFUNC         FROM @fromtables W10A) SUB
SELECT                 *,    _analyticfunc1 OVER (PARTITION BY _variable[#part]                             _maybeorderby) FUNC            FROM \
             (SELECT @star,  _analyticfunc1 OVER (PARTITION BY _variable[#part]                             _maybeorderby) SUBFUNC         FROM @fromtables W11A) SUB

-- Test a SQL Analytic (Window) function used in a sub-query, without PARTITION BY clause
SELECT                 *,    _analyticfunc1 OVER (                                                         @orderbyclause) FUNC            FROM \
             (SELECT @star,  _analyticfunc1 OVER (                                                         @orderbyclause) SUBFUNC         FROM @fromtables W12A) SUB

-- Identical copies of most of the previous query templates, except that
-- ROW_NUMBER always requires an ORDER BY (on the ID column), or the result
-- is non-deterministic and will therefore result in test failures
SELECT               @star,  _analyticfunc2 OVER (                                                             @orderbyid) FUNC            FROM @fromtables W01B
SELECT                       _analyticfunc2 OVER (                                                             @orderbyid) FUNC, @star     FROM @fromtables W02B

SELECT               @star,  _analyticfunc2 OVER (PARTITION BY __[#ord]                                        @orderbyid) FUNC            FROM @fromtables W03B
SELECT            __[#ord],  _analyticfunc2 OVER (PARTITION BY _variable[#part]                                @orderbyid) FUNC, __[#part] FROM @fromtables W04B
SELECT __[#ord], __[#part],  _analyticfunc2 OVER (PARTITION BY _variable[#part]                                @orderbyid) FUNC            FROM @fromtables W05B

SELECT       _columnorexpr,  _analyticfunc2 OVER (PARTITION BY _variable                                       @orderbyid) FUNC            FROM @fromtables W06B
SELECT                       _analyticfunc2 OVER (PARTITION BY _columnorexpr                                   @orderbyid) FUNC, @star     FROM @fromtables W07B
SELECT               @star,  _analyticfunc2 OVER (PARTITION BY _columnorexpr, _columnorexpr                    @orderbyid) FUNC            FROM @fromtables W08B
SELECT                       _analyticfunc2 OVER (PARTITION BY _columnorexpr, _columnorexpr, _columnorexpr     @orderbyid) FUNC, @star     FROM @fromtables W09B

SELECT * FROM (SELECT @star, _analyticfunc2 OVER (PARTITION BY _columnorexpr                                   @orderbyid) SUBFUNC         FROM @fromtables W10B) SUB
SELECT                 *,    _analyticfunc2 OVER (PARTITION BY _variable[#part]                                @orderbyid) FUNC            FROM \
             (SELECT @star,  _analyticfunc2 OVER (PARTITION BY _variable[#part]                                @orderbyid) SUBFUNC         FROM @fromtables W11B) SUB
SELECT                 *,    _analyticfunc2 OVER (                                                             @orderbyid) FUNC            FROM \
             (SELECT @star,  _analyticfunc2 OVER (                                                             @orderbyid) SUBFUNC         FROM @fromtables W12B) SUB
