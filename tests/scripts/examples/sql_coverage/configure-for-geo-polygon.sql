<grammar.sql>
-- Run the template against DDL with all Geo types
-- geo types don't do math
{@aftermath = ""}
{@agg = "_genericagg"} -- geo types don't do SUM or AVG
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@somecmp = "_eqne"} -- in this case, the "smaller" list of comparison operators is identical

--{_colpred |= "_point2numfun(_variable[point]) _cmp _value[int:-90,90]"}
{_colpred |= "_poly2numfun(_variable[polygon]) _cmp _value[int:-90,90]"}
--{_colpred |= "_2geo2numfun(_variable[geo],_variable[geo]) _cmp _value[int:-90,90]"}
--{_colpred |= "_geo2stringfun(_variable[geo]) _cmp _value[string]"}
{_colpred |= "_poly2pointfun(_variable[polygon]) @cmp _variable[point]
{_colpred |= "_poly2boolfun(_variable[polygon])"}
{_colpred |= "_polypoint2boolfun(_variable[polygon],_variable[point])"}
{@columnpredicate = "_colpred"}

{@columntype = "polygon"}
{@comparabletype = "polygon"}
{@comparableconstant = "polygonFromText('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')"}

--TODO: this should be randomized (e.g. "_value[polygon]"??):
{@comparablevalue = "polygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')"}

--TODO: this should be randomized (e.g. "_variable[polygon] _cmp _value[polygon]"??):
{@dmlcolumnpredicate = "@columnpredicate"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

{@insertvals = "_id, _value[point:10,12], null, null, _value[polygon], _value[polygon null25], _value[polygon null50], null"}

{@onefun = ""}  -- There are no handy unary polygon-to-polygon functions.
{@optionalfn = "_geofun"}
{@star = "ID, AsText(PT1), AsText(POLY1), AsText(POLY2), AsText(POLY3)"}
{@lhsstar = "LHS.ID, AsText(LHS.PT1), AsText(LHS.POLY1), AsText(LHS.POLY2), AsText(LHS.POLY3)"}

{@updatecolumn = "POLY3"}
{@updatesource = "POLY1"}
{@updatevalue = "_value[polygon null25]"}
