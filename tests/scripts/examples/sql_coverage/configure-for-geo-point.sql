<grammar.sql>
-- Run the template against DDL with all Geo types
-- geo types don't do math
{@aftermath = ""}
{@agg = "_genericagg"} -- points don't do SUM or AVG
{@distinctableagg = "COUNT"} -- points don't do SUM
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@somecmp = "_eqne"} -- in this case, the "smaller" list of comparison operators is identical
{_colpred |= "_point2numfun(_variable[point])                 _cmp _value[int:-180,180]"}    -- LATITUDE, LONGITUDE
{_colpred |= "_2geo2numfun(_variable[point],_variable[point]) _cmp _value[int:0,20000000]"}  -- DISTANCE
{_colpred |= "_geo2stringfun(_variable[point])                _cmp _value[string]"}          -- AsText
{_colpred |= "_value[point]                                   @cmp _variable[point]"}        -- PointFromText (used in "_value[point]")
{@columnpredicate = "_colpred"}
{@columntype = "point"}
{@comparabletype = "point"}
{@comparableconstant = "pointFromText('POINT(-71.06 42.36)')"}
{@comparablevalue = "_value[point]"}
{@dmlcolumnpredicate = "_variable[point] _cmp _value[point]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}
{@insertvals = "_id, _value[point], _value[point null25], _value[point null50], null, null, null, _value[float]"}
{@onefun = ""}  -- There are no handy unary point-to-point functions
{@optionalfn = "_geofun"}
{@plus10 = " "} -- You cannot add to a point
{@star    = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), LONGITUDE(PT3), LATITUDE(PT3)"}
{@lhsstar = "LHS.ID, LONGITUDE(LHS.PT1), LATITUDE(LHS.PT1), LONGITUDE(LHS.PT2), LATITUDE(LHS.PT2), LONGITUDE(LHS.PT3), LATITUDE(LHS.PT3)"}
{@updatecolumn = "PT3"}
{@updatesource = "PT1"}
{@updatevalue = "_value[point null20]"}
