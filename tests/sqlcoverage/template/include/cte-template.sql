-- This file tests CTEs (Common Table Expressions), Recursive and otherwise,
-- i.e., queries using WITH or WITH RECURSIVE

-- Required preprocessor macros (with example values):
-- {@insertvals = "_id, _value[decimal], _value[decimal], _value[float]"}
-- {@agg = "_numagg"}
-- {@columntype = "int"}
-- {@comparabletype = "numeric"} TODO: don't need this ???
-- {@dmltable = "_table"}
-- {@fromtables = "_table"}
-- {@updatecolumn = "NUM"}
-- {@updatesource = "ID"}
-- {@updatecolumn2 = "RATIO"} -- rarely used; so far, only in CTE tests
-- {@maxdepth = "6"} -- maximum depth, in Recursive CTE tests


--- Define "place-holders" used in some of the queries below
{_optionalorderbyidlimitoffset |= ""}
{_optionalorderbyidlimitoffset |= "LIMIT 1000"}
{_optionalorderbyidlimitoffset |= "ORDER BY @idcol _sortorder _optionallimit _optionaloffset"}

-- For some queries, there is no ID, so we order by one of the columns
{_optionalorderbyordlimitoffset |= ""}
{_optionalorderbyordlimitoffset |= "LIMIT 1000"}
{_optionalorderbyordlimitoffset |= "ORDER BY __[#ord] _sortorder _optionallimit _optionaloffset"}

{_optionalorderbycteordlimitoffset |= ""}
{_optionalorderbycteordlimitoffset |= "LIMIT 1000"}
{_optionalorderbycteordlimitoffset |= "ORDER BY CTE.__[#ord] _sortorder _optionallimit _optionaloffset"}

-- For some queries, ordering by ID is insufficient to ensure a unique order
{_optionalorderbyidtotallimitoffset |= ""}
{_optionalorderbyidtotallimitoffset |= "LIMIT 1000"}
{_optionalorderbyidtotallimitoffset |= "ORDER BY @idcol _sortorder, PATH, TOTAL _optionallimit _optionaloffset"}

-- The types of joins that can be used are different in different queries
{@jointype = "_jointype"}
{@nonfulljointype = "_nonfulljointype"}


-- DML, clean out and regenerate random data first.
DELETE FROM @dmltable
INSERT INTO @dmltable VALUES (@insertvals)

-- Prepare tables for (Basic & Recursive) CTE query tests: first, double
-- the number of rows, compared to our usual tests
INSERT INTO @dmltable[#tab] SELECT @insertselectcols FROM __[#tab]

-- Next, one column is updated to reference another column, using a "tree"
-- structure (a binary tree, in this case) typical of Recursive CTE queries
-- (similar to our Employee/Manager example, from ENG-13286); other columns
-- remain random
UPDATE @dmltable[#tab] W01 SET @updatecolumn =   \
    ( SELECT @updatesource FROM __[#tab] WHERE @idcol =  \
        FLOOR( (SELECT MIN(@idcol) FROM __[#tab]) + (W01.@idcol  \
             - (SELECT MIN(@idcol) FROM __[#tab]))/2 ) LIMIT 1 )

-- Change the "last" 4 rows, to make the "tree" deeper
UPDATE @dmltable[#tab] W02 SET @updatecolumn =  \
    ( SELECT @updatesource FROM __[#tab] WHERE @idcol = W02.@idcol - 1 )  \
    WHERE @idcol > (SELECT MAX(@idcol) FROM __[#tab]) - 4

-- Add a NULL value at the "top" & "middle" of the "tree"
UPDATE @dmltable[#tab] W03 SET @updatecolumn = NULL  \
    WHERE @idcol = (SELECT                  MIN(@idcol)      FROM __[#tab])  \
       OR @idcol = (SELECT (MAX(@idcol)+1 + MIN(@idcol)) / 2 FROM __[#tab])

-- Then, update a second column, similar to the first but in a different order,
-- so the "top" of this "tree" is no longer the "first" row (with the lowest ID),
-- but in the "middle"; and this one is not a binary tree
-- (Note: if @updatecolumn2 is set to NONEXISTENT, then this statement will
-- return an error, and have no effect)
UPDATE @dmltable[#tab] W04 SET @updatecolumn2 =  \
    ( SELECT @updatesource FROM __[#tab],  \
        ( SELECT MIN(@idcol) AS MINID, MAX(@idcol)+1 - MIN(@idcol)                AS DIFF,  \
                                CAST( (MAX(@idcol)+1 - MIN(@idcol))/2 AS INTEGER) AS HALF   \
        FROM __[#tab] ) AS B  \
        WHERE MOD(    @idcol - B.MINID + B.HALF, B.DIFF) =   \
              MOD(W04.@idcol - B.MINID + B.HALF, B.DIFF)/3 LIMIT 1 )

-- Add a NULL value at the "top" of the "tree", but in the "middle", as far as
-- ID column values go
UPDATE @dmltable[#tab] W05 SET @updatecolumn2 = NULL WHERE @idcol =  \
    ( SELECT (MAX(@idcol)+1 + MIN(@idcol)) / 2 FROM __[#tab] )


-- Basic (non-Recursive) CTE (Common Table Expression) query tests (i.e., using WITH):

-- ... using a simple join and a simplified version of our standard Employee/Manager
-- example (at least, when the @updatesource and @updatecolumn are used as the first
-- two columns, it's kind of similar to that example - see ENG-13286)
WITH CTE AS (  \
    SELECT _variable[#ctec1 @columntype], _variable[#ctec2 @columntype], _variable[#ctec3], 1 AS DEPTH,  \
        COALESCE(CAST(__[#ctec1] AS VARCHAR), '') AS PATH, COALESCE(_variable[#ctec5 numeric], 0) AS TOTAL  \
        FROM @fromtables  \
)   SELECT W10.__[#ctec1], W10.__[#ctec2], W10.__[#ctec3],  \
        CTE.DEPTH + 1 AS DEPTH,  \
        CTE.PATH || '/' || COALESCE(CAST(W10.__[#ctec1] AS VARCHAR), '') AS PATH,   \
        CTE.TOTAL        +  COALESCE(     W10.__[#ctec5], 0)  AS TOTAL  \
        FROM @fromtables W10 @jointype JOIN CTE ON W10.__[#ctec2] = CTE.__[#ctec1]  \
        _optionalorderbyidtotallimitoffset

-- ... with aggregate functions (implicit GROUP BY), and an implicit JOIN (but no explicit one)
WITH CTE AS (  \
    SELECT @agg(_variable[#ctec1 @columntype]) AG1, MIN(_variable[#ctec2 @columntype]) MN2,  \
            MAX(_variable[numeric]) MX3,          COUNT(_variable) CT4 FROM @fromtables[#tab]  \
)   SELECT * FROM __[#tab] W11, CTE _optionalorderbyidlimitoffset

-- ... with aggregate functions (implicit GROUP BY), and an explicit JOIN
-- Note: we use @nonfulljointype because PostgreSQL does not support FULL JOIN here
WITH CTE AS (  \
    SELECT @agg(_variable[#ctec1 @columntype]) AG1, MIN(_variable[#ctec2 @columntype]) MN2,  \
            MAX(_variable[numeric]) MX3,          COUNT(_variable) CT4 FROM @fromtables[#tab]  \
)   SELECT * FROM __[#tab] W12 @nonfulljointype JOIN CTE ON W12.__[#ctec2] <= CTE.AG1  \
        _optionalorderbyidlimitoffset

-- ... with an actual GROUP BY, in the CTE (and an explicit JOIN)
WITH CTE AS (  \
    SELECT _variable[#ctec1 @columntype], @agg(_variable[#ctec2 @columntype]) AG1  \
        FROM @fromtables[#tab] GROUP BY __[#ctec1]  \
)   SELECT * FROM __[#tab] W13 @jointype JOIN CTE ON W13.__[#ctec1] = CTE.__[#ctec1]  \
        _optionalorderbyidlimitoffset

-- ... with an actual GROUP BY, in the CTE and in the main (final) query (and an explicit JOIN)
WITH CTE AS (  \
    SELECT _variable[#ord @columntype], _symbol[#agg1 @agg](_variable[#ctec2 @columntype]) __[#ctec2]  \
        FROM @fromtables[#tab] GROUP BY __[#ord]  \
)   SELECT W14.__[#ord], __[#agg1](W14.__[#ctec2]) AG1, __[#agg1](CTE.__[#ctec2]) AG2  \
        FROM __[#tab] W14 @jointype JOIN CTE ON W14.__[#ord] = CTE.__[#ord]  \
        GROUP BY W14.__[#ord] _optionalorderbyordlimitoffset

-- TODO: uncomment/fix this if we ever support more than one CTE (ENG-13575):
--WITH CTE1 AS (  \
--    SELECT _variable[#ctec1 @columntype], _symbol[#agg1 @agg](_variable[#ctec2 @columntype]) AG1  \
--        FROM @fromtables[#tab] GROUP BY __[#ctec1]  \
--    ), CTE2 AS (  \
--        SELECT __[#ctec1] FROM CTE1  \
--            WHERE AG1 > (SELECT MIN(AG1) FROM CTE1)  \
--    )  \
--SELECT __[#ctec1], _variable[#ctec3], __[#agg1](__[#ctec2]) AS AG1, _numagg(_variable[numeric]) AS AG2  \
--    FROM __[#tab] WHERE __[#ctec1] IN (SELECT __[#ctec1] FROM CTE2)  \
--    GROUP BY __[#ctec1], __[#ctec3] 
--    -- _optionalorderbyidlimitoffset


-- Recursive CTE query tests (i.e., using WITH RECURSIVE):

-- ... a very simple example (almost like using VALUES, which we do not
-- support, currently - see ENG-13576)
-- TODO: if/when ENG-13613 is fixed, could replace CAST('A' AS VARCHAR) with
-- just 'A', in the 2 examples below:
WITH RECURSIVE RCTE (CT1, STR2) AS (  \
    SELECT 1, CAST('A' AS VARCHAR) FROM @fromtables W20  \
    UNION ALL  \
    SELECT CT1+1, STR2 || 'b' FROM RCTE WHERE CT1 < 10  \
) SELECT * FROM RCTE

-- ... a very similar simple example, but with aggregates
-- (in the "final" query)
WITH RECURSIVE RCTE (CT1, STR2) AS (  \
    SELECT 1, CAST('A' AS VARCHAR) FROM @fromtables W21  \
    UNION ALL  \
    SELECT CT1+1, STR2 || 'b' FROM RCTE WHERE CT1 < 10  \
) SELECT SUM(CT1), MIN(STR2), MAX(STR2), COUNT(STR2) FROM RCTE

-- ... using a version of our standard Employee/Manager example (at least, when
-- the @updatesource and @updatecolumn are used as the first two columns, it's
-- very similar to that example - see ENG-13286)
-- Note: we use only inner joins because PostgreSQL does not support outer ones here
WITH RECURSIVE RCTE (_variable[#rctec1 @columntype], _variable[#rctec2 @columntype],  \
        _variable[#rctec3], DEPTH, PATH, TOTAL) AS (  \
    SELECT __[#rctec1], __[#rctec2], __[#rctec3], 1,  \
            CAST(__[#rctec1] AS VARCHAR),  \
            CAST(_variable[#rctec6 numeric] AS FLOAT)  \
        FROM @fromtables WHERE __[#rctec2] IS NULL  \
    UNION ALL  \
    SELECT W22.__[#rctec1], W22.__[#rctec2], W22.__[#rctec3], RCTE.DEPTH + 1,  \
            COALESCE(RCTE.PATH, 'null') || '/' || COALESCE(CAST(W22.__[#rctec1] AS VARCHAR), 'null'),  \
            COALESCE(RCTE.TOTAL, 0)         +     COALESCE(     W22.__[#rctec6], 0)  \
        FROM @fromtables W22 JOIN RCTE ON W22.__[#rctec2] = RCTE.__[#rctec1]  \
        WHERE DEPTH < @maxdepth  \
)   SELECT * FROM RCTE _optionalorderbyidtotallimitoffset

-- ... a very similar example, but with aggregate functions and GROUP BY
-- (in the "final" query)
-- Note: we use only inner joins because PostgreSQL does not support outer ones here
WITH RECURSIVE RCTE (_variable[#rctec1 @columntype], _variable[#ord @columntype],  \
        _variable[#rctec3], DEPTH, PATH, TOTAL) AS (  \
    SELECT __[#rctec1], __[#ord], __[#rctec3], 1,  \
            CAST(__[#rctec1] AS VARCHAR),  \
            CAST(_variable[#rctec6 numeric] AS FLOAT)  \
        FROM @fromtables WHERE __[#ord] IS NULL  \
    UNION ALL  \
    SELECT W23.__[#rctec1], W23.__[#ord], W23.__[#rctec3], RCTE.DEPTH + 1,  \
            COALESCE(RCTE.PATH, 'null') || '/' || COALESCE(CAST(W23.__[#rctec1] AS VARCHAR), 'null'),  \
            COALESCE(RCTE.TOTAL, 0)         +     COALESCE(     W23.__[#rctec6], 0)  \
        FROM @fromtables W23 JOIN RCTE ON W23.__[#ord] = RCTE.__[#rctec1]  \
        WHERE DEPTH < @maxdepth  \
)   SELECT __[#ord], _symbol[#agg1 @agg](__[#rctec1]) AG1, __[#agg1](DEPTH) AGD,  \
            __[#agg1](TOTAL) AGT, MIN(PATH) MINP, MAX(PATH) MAXP, COUNT(*) CT FROM RCTE   \
        GROUP BY __[#ord] _optionalorderbyordlimitoffset


-- TODO: need to fix the rest of these:


-- ... with aggregate functions (implicit GROUP BY) in the "base" query;
-- and an implicit JOIN (but no explicit one)
--WITH RECURSIVE RCTE (AG1, MN2, MX3, CT4, DEPTH) AS (  \
--    SELECT _symbol[#agg1 @agg](_variable[#rctec1 @columntype]), MIN(_variable[#rctec2 @columntype]),  \
--                           MAX(_variable[#rctec3]),           COUNT(_variable[#rctec4]), 1  \
--        FROM @fromtables  \
--    UNION ALL  \
--    SELECT __[#rctec1], __[#rctec2], __[#rctec3], COUNT(__[#rctec4]), RCTE.DEPTH + 1  \
--        FROM @fromtables W24, RCTE  \
--        WHERE W24.__[#rctec1] < RCTE.AG1 AND DEPTH < @maxdepth  \
--)   SELECT * FROM RCTE ORDER BY DEPTH



-- ... with aggregate functions (implicit GROUP BY), and an explicit JOIN
-- Note: we use _nonfulljointype because PostgreSQL does not support FULL JOIN here
--WITH RECURSIVE RCTE (AG1, MN2, MX3, CT4, DEPTH, PATH, TOTAL) AS (  \
--    SELECT _symbol[#agg1 @agg](_variable[#rctec1 @columntype]),   MIN(_variable[#rctec2 @columntype]),  \
--                           MAX(_variable[#rctec3 numeric]),     COUNT(_variable[#rctec4]), 1,  \
--                       CAST(__[#agg1](__[#rctec1]) AS VARCHAR), COALESCE(MAX(__[#rctec3]), 0)  \
--        FROM @fromtables WHERE __[#rctec2] IS NULL  \
--    UNION ALL  \
--    SELECT __[#agg1](W25.__[#rctec1]),   MIN(W25.__[#rctec2]),  \
--                 MAX(W25.__[#rctec3]), COUNT(W25.__[#rctec4]), RCTE.DEPTH + 1,  \
--            COALESCE(RCTE.PATH, '') || '/' || COALESCE(CAST(W25.__[#rctec1] AS VARCHAR), ''),  \
--            COALESCE(RCTE.TOTAL, 0)        +  COALESCE(     W25.__[#rctec3], 0)  \
--        FROM @fromtables W25 @nonfulljointype JOIN RCTE ON  W25.__[#rctec2] <= RCTE.MN2  \
--        WHERE DEPTH < @maxdepth  \
--)   SELECT * FROM RCTE _optionalorderbyidlimitoffset

-- ... with an actual GROUP BY, in the RCTE
--WITH RECURSIVE RCTE (AG1, MN2, MX3, CT4, DEPTH, PATH, TOTAL) AS (  \
--    SELECT _symbol[#agg1 @agg](_variable[#rctec1 @columntype]),   MIN(_variable[#rctec2 @columntype]),  \
--                           MAX(_variable[#rctec3 numeric]),     COUNT(_variable[#rctec4]), 1,  \
--                       CAST(__[#agg1](__[#rctec1]) AS VARCHAR), COALESCE(MAX(__[#rctec3]), 0)  \
--        FROM @fromtables WHERE __[#rctec2] IS NULL GROUP BY DEPTH  \
--    UNION ALL  \
--    SELECT __[#agg1](W26.__[#rctec1]),   MIN(W26.__[#rctec2]),  \
--                 MAX(W26.__[#rctec3]), COUNT(W26.__[#rctec4]), RCTE.DEPTH + 1,  \
--            COALESCE(RCTE.PATH, '') || '/' || COALESCE(CAST(W26.__[#rctec1] AS VARCHAR), ''),  \
--            COALESCE(RCTE.TOTAL, 0)        +  COALESCE(     W26.__[#rctec3], 0)  \
--        FROM @fromtables W26 @nonfulljointype JOIN RCTE ON  W26.__[#rctec2] <= RCTE.MN2  \
--        WHERE RCTE.DEPTH < @maxdepth GROUP BY RCTE.DEPTH  \
--)   SELECT * FROM RCTE _optionalorderbyidlimitoffset
----WITH CTE AS (  \
----    SELECT _variable[#ctec1 @columntype], @agg(_variable[#ctec2 @columntype]) AG1  \
----        FROM @fromtables[#tab] GROUP BY __[#ctec1]  \
----)   SELECT * FROM __[#tab] W13 @jointype JOIN CTE ON W13.__[#ctec1] = CTE.__[#ctec1]  \
----        _optionalorderbyidlimitoffset

---- ... with an actual GROUP BY, in the RCTE and in the main (final) query
----WITH CTE AS (  \
----    SELECT _variable[#ctec1 @columntype], _symbol[#agg1 @agg](_variable[#ctec2 @columntype]) __[#ctec2]  \
----        FROM @fromtables[#tab] GROUP BY __[#ctec1]  \
----)   SELECT _variable[#grp], __[#agg1](W14.__[#ctec2]) AG1, __[#agg1](CTE.__[#ctec2]) AG2  \
----        FROM __[#tab] W14 @jointype JOIN CTE ON W14.__[#ctec1] = CTE.__[#ctec1]  \
----        GROUP BY __[#grp] _optionalorderbyidlimitoffset


-- TODO: might want to fix this, though it partly works:


WITH RECURSIVE RCTE (_variable[#ord @columntype], _variable[#rctec2 @columntype],  \
        _variable[#rctec3], DEPTH, PATH, TOTAL) AS (  \
    SELECT __[#ord], __[#rctec2], __[#rctec3], 1, CAST(__[#ord] AS VARCHAR),  \
            COALESCE(_variable[#rctec6 numeric], 0)  \
        FROM @fromtables[#tab1]  \
        WHERE __[#rctec2] = (SELECT MIN(__[#rctec2]) FROM __[#tab1])  \
    UNION ALL  \
    SELECT W27.__[#ord], W27.__[#rctec2], W27.__[#rctec3], RCTE.DEPTH + 1,  \
            COALESCE(RCTE.PATH, '') || '/' || COALESCE(CAST(W27.__[#ord] AS VARCHAR), ''),  \
            COALESCE(RCTE.TOTAL, 0)        +  COALESCE(     W27.__[#rctec6], 0)  \
        FROM RCTE, @fromtables[#tab2] W27  \
        WHERE W27.__[#rctec2] = COALESCE(RCTE.__[#ord], (SELECT __[#rctec2] FROM __[#tab2] WHERE @idcol = W27.@idcol + 1))  \
          AND DEPTH < @maxdepth  \
)   SELECT __[#ord], @agg(__[#rctec2]) AG1, MIN(DEPTH) MIND, MAX(DEPTH) MAXD,  \
             MIN(TOTAL) MINT, MAX(TOTAL) MAXT, MIN(PATH)  MINP, MAX(PATH)  MAXP  \
        FROM RCTE GROUP BY __[#ord] _optionalorderbyordlimitoffset

-- TODO: remove this - temporary, to check the data in all tables:
SELECT @star FROM @fromtables ORDER BY ID
