<grammar.sql>
-- Run the template against DDL with all Geo types
-- geo types don't do math
{@aftermath = ""}
{@agg = "_genericagg"} -- geo types don't do SUM or AVG
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@somecmp = "_eqne"} -- in this case, the "smaller" list of comparison operators is identical

{_colpred |= "_point2numfun(_variable[point]) _cmp _value[int:-90,90]"}
--{_colpred |= "_poly2numfun(_variable[polygon]) _cmp _value[int:-90,90]"}
{_colpred |= "_2geo2numfun(_variable[geo],_variable[geo]) _cmp _value[int:-90,90]"}
{_colpred |= "_geo2stringfun(_variable[geo]) _cmp _value[string]"}
--{_colpred |= "_poly2pointfun(_variable[polygon]) @cmp _variable[point]
--{_colpred |= "_poly2boolfun(_variable[polygon])"}
--{_colpred |= "_polypoint2boolfun(_variable[polygon],_variable[point])"}
{@columnpredicate = "_colpred"}

{@columntype = "point"}
{@comparabletype = "point"}
{@comparableconstant = "pointFromText('POINT(-71.0 42.0)')"}

--TODO: this should be randomized (e.g. "_value[point]"??):
{@comparablevalue = "pointFromText('POINT(-71.0 42.0)')"}

--TODO: this should be randomized (e.g. "_variable[point] _cmp _value[point]"??):
{@dmlcolumnpredicate = "@columnpredicate"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

--TODO: this should be randomized:
--{@insertvals = "_id, pointFromText('POINT(0 0)'), pointFromText('POINT(-71.0 42.0)'), null, polygonFromText('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')"}
{@insertvals = "_id, pointFromText('POINT(0 0)'), pointFromText('POINT(-71.0 42.0)'), null, polygonFromText('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')"}
--{@insertvals = "_id, pointFromText('POINT(_value[decimal:-1,1] _value[decimal:-1,1])'), null, pointFromText('POINT(_value[decimal:-1,1] _value[decimal:-1,1])'), polygonFromText('POLYGON((_value[decimal:-0.9,-0.3] _value[decimal:-0.9,-0.3], _value[decimal:0.3,0.9] _value[decimal:-0.9,-0.3], _value[decimal:0.3,0.9] _value[decimal:0.3,0.9], _value[decimal:-0.9,-0.3] _value[decimal:0.3,0.9], _value[decimal:-0.9,-0.3] _value[decimal:-0.9,-0.3]))')"}

{@onefun = ""}  -- There are no handy unary point-to-point or polygon-to-polygon functions.
{@optionalfn = "_geofun"}
-- TODO: perhaps change this to use "AsText" (not LONGITUDE, LATITUDE, AREA), once fully supported (?);
-- and then, perhaps back to "*", once the Python client works (?? - only if results are comparable with PostGIS, so probably not)
{@star = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), AREA(POLY1)"}
{@lhsstar = "LHS.ID, LONGITUDE(LHS.PT1), LATITUDE(LHS.PT1), LONGITUDE(LHS.PT2), LATITUDE(LHS.PT2), AREA(LHS.POLY1)"}

{@updatecolumn = "PT2"}
{@updatesource = "PT1"}
--TODO: this should be randomized (e.g. "_value[point]"??):
{@updatevalue = "pointFromText('POINT(1.01 -1.01)')"}
