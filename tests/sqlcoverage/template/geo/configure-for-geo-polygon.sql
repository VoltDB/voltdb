<grammar.sql>
-- Run the template against DDL with all Geo types
-- geo types don't do math
{@aftermath = ""}
{@agg = "_genericagg"} -- polygons don't do SUM or AVG
{@distinctableagg = "COUNT"} -- polygons don't do SUM
{@winagg = "COUNT"} -- polygons don't do windowed analytic MIN, MAX, SUM (or AVG)
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@somecmp = "_eqne"} -- in this case, the "smaller" list of comparison operators is identical
{_colpred |= "_poly2numfun(_variable[polygon])            _cmp _value[int:0,270000000000]"}  -- AREA, NumPoints, NumInteriorRings
{_colpred |= "_2geo2numfun(_variable[point],_variable[polygon]) _cmp _value[int:0,20000000]"} --DISTANCE
{_colpred |= "_2geo2numfun(_variable[polygon],_variable[point]) _cmp _value[int:0,20000000]"} --DISTANCE
-- Once DISTANCE(polygon,polygon) is supported, change to (uncomment) this version
-- (& delete the 2 previous lines; plus a similar one in configure-for-geo-point.sql??):
--{_colpred |= "_2geo2numfun(_variable[geo],_variable[geo]) _cmp _value[int:0,20000000]"}      -- DISTANCE
{_colpred |= "_geo2stringfun(_variable[polygon])          _cmp _value[string]"}              -- AsText
{_colpred |= "_poly2pointfun(_variable[polygon])          @cmp _variable[point]              -- CENTROID
{_colpred |= "_value[polygon]                             @cmp _variable[polygon]"}          -- PolygonFromText (used in "_value[polygon]")
{_colpred |= "_poly2boolfun(_variable[polygon])"}                                            -- IsValid
{_colpred |= "_polypoint2boolfun(_variable[polygon],_variable[point])"}                      -- CONTAINS
{_colpred |= "_2geonum2boolfun(_variable[point],_variable[polygon],_value[int:0,20000000])"} -- DWithin
{_colpred |= "_2geonum2boolfun(_variable[polygon],_variable[point],_value[int:0,20000000])"} -- DWithin
-- Once DWithin(polygon,polygon,number) is supported, change to (uncomment) this version
-- (& delete the 2 previous lines; plus a similar one in configure-for-geo-polygon.sql??):
--{_colpred |= "_2geonum2boolfun(_variable[geo],_variable[geo],_value[int:0,20000000])"}       -- DWithin
{@columnpredicate = "_colpred"}
{@columntype = "polygon"}
{@comparabletype = "polygon"}
{@comparableconstant = "polygonFromText('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')"}
{@comparablevalue = "_value[polygon]"}
{@dmlcolumnpredicate = "_variable[polygon] _cmp _value[polygon]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

{@insertcols = "ID, PT1, PT2, PT3, POLY1, POLY2, POLY3, RATIO"}
{@insertselectcols = "ID+16, PT1, PT2, PT3, POLY1, POLY2, POLY3, RATIO"}
{@insertvals = "_id, _value[point:-109,-102,37,41], null, null, _value[polygon], _value[polygon null25], _value[polygon null50], _value[float null50]"}
{@onefun = ""}  -- There are no handy unary polygon-to-polygon functions
{@optionalfn = "_geofun"}
{@plus10 = ""} -- You cannot add to a polygon
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star = "ID, AsText(PT1), AsText(POLY1), AsText(POLY2), AsText(POLY3), RATIO"}
{@lhsstar = "LHS.ID, AsText(LHS.PT1), AsText(LHS.POLY1), AsText(LHS.POLY2), AsText(LHS.POLY3)"}
{@updatecolumn = "POLY3"}
{@updatesource = "POLY1"}
{@updatevalue = "_value[polygon null25]"}
{@updatecolumn2 = "POLY2"} -- rarely used; so far, only in CTE tests
{@maxdepth = "3"} -- maximum depth, in Recursive CTE tests
