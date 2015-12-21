<grammar.sql>
-- Run the template against DDL with all Geo types
-- geo types don't do math
{@aftermath = ""}
{@agg = "_genericagg"} -- geo types don't do SUM or AVG
{_colpred |= "_geofun1arg(_variable[geo]) _cmp _value[int:-90,90]"}
{_colpred |= "CENTROID(_variable[polygon]) _cmp _variable[point]"}
{@columnpredicate = "_colpred"}

{@columntype = "geo"}
{@comparabletype = "geo"}
{_compconst |= "pointFromText('POINT(-71.0 42.0)')"}
{_compconst |= "polygonFromText('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')"}
{@comparableconstant = "_compconst"}

--{@comparablevalue = "pointFromText('POINT(-71.0 42.0)')"}
--TODO: these should be randomized (e.g. "_value[geo]"??):
{_compval |= "pointFromText('POINT(-71.0 42.0)')"}
{_compval |= "polygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')"}
{@comparablevalue = "_compval"}

--TODO: this should be randomized (e.g. "_variable[geo] _cmp _value[geo]"):
{@dmlcolumnpredicate = "@columnpredicate"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@idcol = "ID"}

--TODO: this should be randomized:
{@insertvals = "_id, pointFromText('POINT(0 0)'), pointFromText('POINT(-71.0 42.0)'), polygonFromText('POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')"}
--{@insertvals = "_id, pointFromText('POINT(_value[decimal:-1,1] _value[decimal:-1,1])'), pointFromText('POINT(_value[decimal:-1,1] _value[decimal:-1,1])'), polygonFromText('POLYGON((_value[decimal:-0.9,-0.3] _value[decimal:-0.9,-0.3], _value[decimal:0.3,0.9] _value[decimal:-0.9,-0.3], _value[decimal:0.3,0.9] _value[decimal:0.3,0.9], _value[decimal:-0.9,-0.3] _value[decimal:0.3,0.9], _value[decimal:-0.9,-0.3] _value[decimal:-0.9,-0.3]))')"}

{@onefun = "AREA"} -- not used?!?
{@optionalfn = "_geofun1arg"}
-- TODO: change this to use "asText" (not LONGITUDE, LATITUDE, AREA), once supported;
-- and then, back to "*", once the Python client works
{@star = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), AREA(POLY1)"}
{@lhsstar = "LHS.ID, LONGITUDE(LHS.PT1), LATITUDE(LHS.PT1), LONGITUDE(LHS.PT2), LATITUDE(LHS.PT2), AREA(LHS.POLY1)"}
--{@star = "*"}
--{@lhsstar = "*"}

{_updatecols |= "PT2"}
{_updatecols |= "POLY1"}
{@updatecolumn = "_updatecols"}
{_updatesrcs |= "PT1"}
{_updatesrcs |= "POLY1"}
{@updatesource = "_updatesrcs"}
--TODO: this should be randomized (e.g. "_value[geo]"):
{@updatevalue = "_compconst"}
