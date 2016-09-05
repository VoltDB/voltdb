-- This file holds patterns to test SQL Analytic (aka Window) functions, such
-- as RANK, which can only be tested against PostgreSQL, since HSQL does not
-- support them.
-- [Note: currently RANK is the only such function supported, and tested here,
-- but that may change in time.]

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

-- Define expressions and clauses used to test RANK, below
{_columnorexpr |= "_variable"}
{_columnorexpr |= "@onefun(_variable[@comparabletype])@plus10"}
{_orderbycolumnorexpr |= "_variable[#ord @rankorderbytype]"}
{_orderbycolumnorexpr |= "@onefun(_variable[#ord @rankorderbytype])@plus10"}
{@orderbyclause = "ORDER BY _orderbycolumnorexpr _sortorder"}

-- Test the RANK (Window / SQL Analytic) function
-- ... without PARTITION BY clause:
SELECT               @star,  RANK() OVER (                                                         @orderbyclause) RANK            FROM @fromtables W01
SELECT                       RANK() OVER (                                                         @orderbyclause) RANK, @star     FROM @fromtables W02
-- ... with PARTITION BY clause:
SELECT               @star,  RANK() OVER (PARTITION BY __[#ord]                                    @orderbyclause) RANK            FROM @fromtables W03
SELECT            __[#ord],  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK, __[#part] FROM @fromtables W04
SELECT __[#ord], __[#part],  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK            FROM @fromtables W05
SELECT       _columnorexpr,  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK            FROM @fromtables W06
SELECT                       RANK() OVER (PARTITION BY _columnorexpr                               @orderbyclause) RANK, @star     FROM @fromtables W07
SELECT               @star,  RANK() OVER (PARTITION BY _columnorexpr, _columnorexpr                @orderbyclause) RANK            FROM @fromtables W08
SELECT                       RANK() OVER (PARTITION BY _columnorexpr, _columnorexpr, _columnorexpr @orderbyclause) RANK, @star     FROM @fromtables W09

-- Test the RANK function used in a sub-query
SELECT * FROM (SELECT @star, RANK() OVER (PARTITION BY _columnorexpr                               @orderbyclause) SUBRANK         FROM @fromtables W10) SUB
SELECT                 *,    RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK            FROM \
             (SELECT @star,  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) SUBRANK         FROM @fromtables W11) SUB
-- ... without PARTITION BY clause:
SELECT                 *,    RANK() OVER (                                                         @orderbyclause) RANK            FROM \
             (SELECT @star,  RANK() OVER (                                                         @orderbyclause) SUBRANK         FROM @fromtables W12) SUB
