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
SELECT               @star,  RANK() OVER (PARTITION BY __[#ord]                                    @orderbyclause) RANK            FROM @fromtables W01
-- TODO: some failures here, due to ENG-10972:
SELECT            __[#ord],  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK, __[#part] FROM @fromtables W02
SELECT __[#ord], __[#part],  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK            FROM @fromtables W03
-- TODO: some failures here, due to ENG-10973:
SELECT       _columnorexpr,  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK            FROM @fromtables W04
SELECT                       RANK() OVER (PARTITION BY _columnorexpr                               @orderbyclause) RANK, @star     FROM @fromtables W05
SELECT               @star,  RANK() OVER (PARTITION BY _columnorexpr, _columnorexpr                @orderbyclause) RANK            FROM @fromtables W06
SELECT                       RANK() OVER (PARTITION BY _columnorexpr, _columnorexpr, _columnorexpr @orderbyclause) RANK, @star     FROM @fromtables W07

-- Test the RANK function used in a sub-query
-- TODO: some failures here, due to ENG-10953:
SELECT * FROM (SELECT @star, RANK() OVER (PARTITION BY _columnorexpr                               @orderbyclause) SUBRANK         FROM @fromtables W08) SUB
SELECT                 *,    RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) RANK            FROM \
             (SELECT @star,  RANK() OVER (PARTITION BY _variable[#part]                            @orderbyclause) SUBRANK         FROM @fromtables W09) SUB
