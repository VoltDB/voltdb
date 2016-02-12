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

{@insertvals = "_id, _value[point], _value[point null25], _value[point null50], null, null, null, null"}

{@onefun = ""}  -- There are no handy unary point-to-point functions.
{@optionalfn = "_geofun"}
{@star    = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), LONGITUDE(PT3), LATITUDE(PT3)"}
{@lhsstar = "LHS.ID, LONGITUDE(LHS.PT1), LATITUDE(LHS.PT1), LONGITUDE(LHS.PT2), LATITUDE(LHS.PT2), LONGITUDE(LHS.PT3), LATITUDE(LHS.PT3)"}

{@updatecolumn = "PT3"}
{@updatesource = "PT1"}
{@updatevalue = "_value[point null25]"}
