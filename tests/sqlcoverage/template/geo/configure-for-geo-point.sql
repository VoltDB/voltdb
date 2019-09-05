<grammar.sql>
-- Run the template against DDL with all Geo types
-- geo types don't do math
{@aftermath = ""}
{@agg = "_genericagg"} -- points don't do SUM or AVG
{@distinctableagg = "COUNT"} -- points don't do SUM
{@winagg = "COUNT"} -- points don't do windowed analytic MIN, MAX, SUM (or AVG)
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@somecmp = "_eqne"} -- in this case, the "smaller" list of comparison operators is identical
{_colpred |= "_point2numfun(_variable[point])                 _cmp _value[int:-180,180]"}    -- LATITUDE, LONGITUDE
{_colpred |= "_2geo2numfun(_variable[point],_variable[point]) _cmp _value[int:0,20000000]"}  -- DISTANCE
{_colpred |= "_geo2stringfun(_variable[point])                _cmp _value[string]"}          -- AsText
{_colpred |= "_value[point]                                   @cmp _variable[point]"}        -- PointFromText (used in "_value[point]")
{_colpred |= "_2geonum2boolfun(_variable[point],_variable[point],_value[int:0,20000000])"}   -- DWithin
{@columnpredicate = "_colpred"}
{@columntype = "point"}
{@comparabletype = "point"}
{@comparableconstant = "pointFromText('POINT(-71.06 42.36)')"}
{@comparablevalue = "_value[point]"}
{@dmlcolumnpredicate = "_variable[point] _cmp _value[point]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

{@insertcols = "ID, PT1, PT2, PT3, POLY1, POLY2, POLY3, RATIO"}
{@insertselectcols = "ID+16, PT1, PT2, PT3, POLY1, POLY2, POLY3, RATIO"}
{@insertvals = "_id, _value[point], _value[point null25], _value[point null50], null, null, null, _value[float null25]"}
{@onefun = ""}  -- There are no handy unary point-to-point functions
{@optionalfn = "_geofun"}
{@plus10 = ""} -- You cannot add to a point
{@rankorderbytype = "int"} -- as used in the ORDER BY clause in a RANK function (must be int or timestamp)
{@star    = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), LONGITUDE(PT3), LATITUDE(PT3), RATIO"}
{@lhsstar = "LHS.ID, LONGITUDE(LHS.PT1), LATITUDE(LHS.PT1), LONGITUDE(LHS.PT2), LATITUDE(LHS.PT2), LONGITUDE(LHS.PT3), LATITUDE(LHS.PT3)"}
{@updatecolumn = "PT3"}
{@updatesource = "PT1"}
{@updatevalue = "_value[point null20]"}
{@updatecolumn2 = "PT2"} -- rarely used; so far, only in CTE tests
{@maxdepth = "3"} -- maximum depth, in Recursive CTE tests
