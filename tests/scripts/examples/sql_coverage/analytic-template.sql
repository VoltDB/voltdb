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
-- {@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function
-- {@star = "*"}

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

-- Define the actual windowed analytic functions being tested (such as RANK), with arguments, if any
{_analyticfunc |= "RANK()"}
{_analyticfunc |= "DENSE_RANK()"}
{_analyticfunc |= "COUNT(*)"}
{_analyticfunc |= "COUNT(_columnorexpr)"}

-- Test SQL Analytic (Window) functions, such as RANK
-- ... without PARTITION BY clause:
SELECT               @star,  _analyticfunc OVER (                                                        @orderbyclause) FUNC            FROM @fromtables W01
SELECT                       _analyticfunc OVER (                                                        @orderbyclause) FUNC, @star     FROM @fromtables W02
-- ... with PARTITION BY clause:
SELECT               @star,  _analyticfunc OVER (PARTITION BY __[#ord]                                   @orderbyclause) FUNC            FROM @fromtables W03
SELECT            __[#ord],  _analyticfunc OVER (PARTITION BY _variable[#part]                           @orderbyclause) FUNC, __[#part] FROM @fromtables W04
SELECT __[#ord], __[#part],  _analyticfunc OVER (PARTITION BY _variable[#part]                           @orderbyclause) FUNC            FROM @fromtables W05
SELECT       _columnorexpr,  _analyticfunc OVER (PARTITION BY _variable                                   _maybeorderby) FUNC            FROM @fromtables W06
SELECT                       _analyticfunc OVER (PARTITION BY _columnorexpr                               _maybeorderby) FUNC, @star     FROM @fromtables W07
SELECT               @star,  _analyticfunc OVER (PARTITION BY _columnorexpr, _columnorexpr                _maybeorderby) FUNC            FROM @fromtables W08
SELECT                       _analyticfunc OVER (PARTITION BY _columnorexpr, _columnorexpr, _columnorexpr _maybeorderby) FUNC, @star     FROM @fromtables W09

-- Test a SQL Analytic (Window) function used in a sub-query
SELECT * FROM (SELECT @star, _analyticfunc OVER (PARTITION BY _columnorexpr                               _maybeorderby) SUBFUNC         FROM @fromtables W10) SUB
SELECT                 *,    _analyticfunc OVER (PARTITION BY _variable[#part]                            _maybeorderby) FUNC            FROM \
             (SELECT @star,  _analyticfunc OVER (PARTITION BY _variable[#part]                            _maybeorderby) SUBFUNC         FROM @fromtables W11) SUB
-- ... without PARTITION BY clause:
SELECT                 *,    _analyticfunc OVER (                                                        @orderbyclause) FUNC            FROM \
             (SELECT @star,  _analyticfunc OVER (                                                        @orderbyclause) SUBFUNC         FROM @fromtables W12) SUB
