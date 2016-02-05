<grammar.sql>
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@star = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), AREA(POLY1)"}

-- Define clauses used to ignore points or polygons that demonstrate known differences
-- between VoltDB and PostGIS, which would otherwise cause tests to fail:
{@ignorepointeqdiffs  = "(ID < 250 OR ID > 300)"} -- ignore known differences with point equality, between VoltDB and PostGIS
{@ignore2pointeqdiffs = "((A.ID < 208 OR A.ID > 300) AND (B.ID < 208 OR B.ID > 300))"} -- ignore point equality diffs, with a join
{@ignoredistroundoff  = "A.ID <> 453"} -- ignore tiny DISTANCE round-off errors, between VoltDB and PostGIS
{@ignoreisvaliddiffs  = "ID < 800"} -- ignore known differences with IsValid(polygon), between VoltDB and PostGIS
{@ignorecentroiddiffs = "ID < 400 AND ID > 100"} -- ignore known differences with CENTROID(polygon), between VoltDB and PostGIS
{@ignoreinvalidpolys  = "ID < 600"} -- ignore polygons that are invalid, according to VoltDB and/or PostGIS
{@ignore2invalidpolys = "A.ID < 600 AND B.ID < 600"} -- ignore 2 invalid polygons, with a join
{@ignore2diffnumholes = "NumInteriorRings(A.POLY1) = NumInteriorRings(B.POLY1)"} -- ignore 2 polygons with different numbers of holes, with a join


-- Insert some random points & polygons
INSERT INTO _table VALUES (_id, _value[point:-109.05,-102.05,37.0,41.0], _value[point:129,139,-26,-11 null25], _value[point null50], _value[polygon], _value[polygon:129,139,-26,-11 null50], null, null)

-- Test inserting points & polygons, via the pointFromText & polygonFromText functions....
-- Note that the ID values are carefully chosen to make the above clauses work

-- Points without polygons:
INSERT INTO _table VALUES (200, null,                                    null,                                   null, null, null, null, null)
INSERT INTO _table VALUES (201, null,                                    pointFromText('POINT(  0.01   0.01 )'), null, null, null, null, null)
INSERT INTO _table VALUES (202, pointFromText('POINT( -1.01    1.01 )'), null,                                   null, null, null, null, null)
INSERT INTO _table VALUES (203, pointFromText('POINT(  0.01    1.01 )'), pointFromText('POINT(  0.01   1.01 )'), null, null, null, null, null)
INSERT INTO _table VALUES (204, pointFromText('POINT( -1.01    0.01 )'), pointFromText('POINT( -0.01  -0.01 )'), null, null, null, null, null)
INSERT INTO _table VALUES (205, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(  1.01   1.01 )'), null, null, null, null, null)
INSERT INTO _table VALUES (206, pointFromText('POINT(  1.01    1.01 )'), pointFromText('POINT(  2.01   2.01 )'), null, null, null, null, null)
-- Points very close to each other (near Herald Square, New York, NY):
INSERT INTO _table VALUES (207, pointFromText('POINT(-73.988  40.7504)'),pointFromText('POINT(-73.988 40.75037)'),null,null, null, null, null)
INSERT INTO _table VALUES (208, pointFromText('POINT(-73.988  40.750401)'),pointFromText('POINT(-73.98801 40.7504)'),null,null,null,null,null)

-- Points with different but equivalent values, i.e., longitude 180 and -180, or differing longitudes at the poles
-- North pole (all these points are equivalent to each other):
INSERT INTO _table VALUES (251, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(  90.0  90.0  )'), null, null, null, null, null)
INSERT INTO _table VALUES (252, pointFromText('POINT( 180.0    90.0 )'), pointFromText('POINT(-180.0  90.0  )'), null, null, null, null, null)
INSERT INTO _table VALUES (253, pointFromText('POINT(  45.0    90.0 )'), pointFromText('POINT( -45.0  90.0  )'), null, null, null, null, null)
-- South pole (all these points are equivalent to each other):
INSERT INTO _table VALUES (254, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(  90.0 -90.0  )'), null, null, null, null, null)
INSERT INTO _table VALUES (255, pointFromText('POINT(-180.0   -90.0 )'), pointFromText('POINT( 180.0 -90.0  )'), null, null, null, null, null)
INSERT INTO _table VALUES (256, pointFromText('POINT( 135.0   -90.0 )'), pointFromText('POINT(-135.0 -90.0  )'), null, null, null, null, null)
-- On the Anti-meridian (International Date Line), longitude 180 or -180
-- (each pair of points in a row is equivalent, but the points in different rows are different):
INSERT INTO _table VALUES (261, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT(-180.0   0.0  )'), null, null, null, null, null)
INSERT INTO _table VALUES (262, pointFromText('POINT(-180.0    45.0 )'), pointFromText('POINT( 180.0  45.0  )'), null, null, null, null, null)
INSERT INTO _table VALUES (263, pointFromText('POINT( 180.0   -45.0 )'), pointFromText('POINT(-180.0 -45.0  )'), null, null, null, null, null)


-- Polygons (& points):
-- Simple ("square", in the lat/long sense) polygons (without holes; and slightly un-square, to reduce round-off errors):
INSERT INTO _table VALUES (301, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41, -0.01 -0.01))'), null, null, null)
INSERT INTO _table VALUES (302, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41, -0.01 -0.01))'), null, null, null)
INSERT INTO _table VALUES (303, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.3  -1.1,  1.3  1.3,  -1.1  1.3,  -1.1  -1.1 ))'), null, null, null)
INSERT INTO _table VALUES (304, pointFromText('POINT( -4.01    2.51 )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2, -2.22 3.32, -3.3  3.3,  -3.3   2.2 ))'), null, null, null)

-- Large but simple polygons: Colorado & Wyoming (very approximate, without considering that latitude lines are not great circles)
-- (The first point for each is within that state; the second is in Boston, MA, which is not)
INSERT INTO _table VALUES (405, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -109.05 37, -102.05 37, -102.05 41, -109.05 41))'), null, null, null)
INSERT INTO _table VALUES (406, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 41, -104.05 45, -111.05 45))'), null, null, null)

-- Small polygons: Times Square & Herald Square, New York, NY (approximate)
-- (The first point for each is within that square; the second is just outside it)
INSERT INTO _table VALUES (307, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98563 40.75800, -73.98695 40.75603, -73.98644 40.75582, -73.98554 40.75796, -73.98461 40.75920, -73.98512 40.75942))'), null, null, null)
INSERT INTO _table VALUES (308, pointFromText('POINT(-73.98759 40.75039)'), pointFromText('POINT(-73.98800 40.75037)'), null, polygonFromText('POLYGON((-73.98783 40.75070, -73.98798 40.74988, -73.98776 40.74978, -73.98728 40.75046, -73.98783 40.75070))'), null, null, null)

-- Super-large polygons, dividing the Earth up into 8 octants:
-- (Note: the longitude of the North and South Pole is arbitrary; however, it has been set here to the values
--  -45, 45, -135, 135, in a (partially successful) attempt to avoid a PostGIS bug in computing the CENTROID)
-- First, the "front": the North-West, North-East, South-East & South-West octants that touch where the Prime (Greenwich) Meridian meets the Equator
INSERT INTO _table VALUES (411, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -45  90, -90   0,    0 0))'), null, null, null)
-- This one has been made slightly smaller, to avoid a PostGIS bug (it works fine either way, in VoltDB):
--INSERT INTO _table VALUES (412, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 0 0,  90   0,  45  90,    0 0))'), null, null, null)
INSERT INTO _table VALUES (412, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   1 1,  89   1,  45  89,    1 1))'), null, null, null)
INSERT INTO _table VALUES (413, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  45 -90,  90   0,    0 0))'), null, null, null)
INSERT INTO _table VALUES (414, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0, -45 -90,    0 0))'), null, null, null)
-- Then, the "back": the North-West, North-East, South-East & South-West octants that touch the Antimeridian (International Date Line) meets the Equator
INSERT INTO _table VALUES (415, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,-135  90, -180 0))'), null, null, null)
INSERT INTO _table VALUES (416, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0, 135  90,  90   0,  180 0))'), null, null, null)
INSERT INTO _table VALUES (417, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0, 135 -90,  180 0))'), null, null, null)
INSERT INTO _table VALUES (418, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,-135 -90, -90   0, -180 0))'), null, null, null)

-- Note: valid polygon exterior vertices must be listed counter-clockwise; but interior hole vertices must be clockwise:
-- A simple ("square-ish") polygon, without a hole (slightly un-square, to reduce round-off errors):
INSERT INTO _table VALUES (321, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2))'), null, null, null)
-- A simple ("square-ish") polygon, with a ("square", asymmetric) hole:
INSERT INTO _table VALUES (322, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
-- A simple ("square-ish") polygon, with 2 ("square", asymmetric) holes:
INSERT INTO _table VALUES (323, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
-- A star-shaped polygon (approximately), with 8 points (16 vertices), without a hole:
INSERT INTO _table VALUES (324, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04, 0.0 1.0))'), null, null, null)
-- An 8-point (approximately) star-shaped polygon, with an (approximately) star-shaped (symmetric, half-sized) hole:
INSERT INTO _table VALUES (325, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04, 0.0 1.0), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
-- A simple ("square-ish") polygon, with 9 holes (around Paris, France):
-- (Note: hole 6 changed to use 2.42999 (not 2.43), in order to avoid 'Loop 0 crosses loop 6', due to round-off)
INSERT INTO _table VALUES (326, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((2.43 48.89, 2.25 48.89, 2.25 48.83, 2.43 48.83, 2.43 48.89), (2.37 48.87, 2.37 48.85, 2.31 48.85, 2.31 48.87, 2.37 48.87), (2.40 48.88, 2.40 48.87, 2.37 48.87, 2.37 48.88, 2.40 48.88), (2.40 48.85, 2.40 48.84, 2.37 48.84, 2.37 48.85, 2.40 48.85), (2.31 48.85, 2.31 48.84, 2.28 48.84, 2.28 48.85, 2.31 48.85), (2.31 48.88, 2.31 48.87, 2.28 48.87, 2.28 48.88, 2.31 48.88), (2.42999 48.86, 2.38 48.855, 2.38 48.865, 2.42999 48.86), (2.25 48.86, 2.30 48.865, 2.30 48.855, 2.25 48.86), (2.34 48.85, 2.37 48.835, 2.31 48.835, 2.34 48.85), (2.34 48.89, 2.36 48.875, 2.32 48.875, 2.34 48.89))'), null, null, null)

-- Just the holes (but counter-clockwise), from the above (with ID 322, 323, 325: 332, 333 are both holes from 323):
INSERT INTO _table VALUES (332, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (333, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (335, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)

-- Most of these (except the one with ID=453) are considered valid by VoltDB, but invalid by PostGIS:
-- Polygons (without holes) covering the North Pole, the South Pole, and (part of) the Antimeridian (International Date Line);
INSERT INTO _table VALUES (851, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(  0.0   88.0  )'), null, polygonFromText('POLYGON((  0  88,  90  88,  180  88,  -90  88,   0  88))'), null, null, null)
INSERT INTO _table VALUES (852, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(  0.0  -88.0  )'), null, polygonFromText('POLYGON((  0 -88, -90 -88,  180 -88,   90 -88,   0 -88))'), null, null, null)
INSERT INTO _table VALUES (453, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT(177.0    0.0  )'), null, polygonFromText('POLYGON((178   2, 178  -2, -178  -2, -178   2, 178   2))'), null, null, null)
-- Polygons with holes that cover the North Pole, the South Pole, and (part of) the Antimeridian (International Date Line)
INSERT INTO _table VALUES (861, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(  0.0   88.0  )'), null, polygonFromText('POLYGON((  0  88,  90  88,  180  88,  -90  88,   0  88), (  0  89,  -90  89,  180  89,  90  89,   0  89))'), null, null, null)
INSERT INTO _table VALUES (862, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(  0.0  -88.0  )'), null, polygonFromText('POLYGON((  0 -88, -90 -88,  180 -88,   90 -88,   0 -88), (  0 -89,   90 -89,  180 -89, -90 -89,   0 -89))'), null, null, null)
INSERT INTO _table VALUES (863, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT(177.0    0.0  )'), null, polygonFromText('POLYGON((178   2, 178  -2, -178  -2, -178   2, 178   2), (179   1, -179   1, -179  -1, 179  -1, 179   1))'), null, null, null)

-- The following polygons are considered invalid by both VoltDB and PostGIS, due to failure to close a loop (i.e., the last vertex is not the
-- same as the first); these INSERT statements, unlike the subsequent ones, will actually return an error, so the INSERT will not succeed
-- (Note: ID values correspond to a similar polygon above, plus 200)
INSERT INTO _table VALUES (501, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41))'), null, null, null)
INSERT INTO _table VALUES (502, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41))'), null, null, null)
INSERT INTO _table VALUES (503, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.3  -1.1,  1.3  1.3,  -1.1  1.3 ))'), null, null, null)
INSERT INTO _table VALUES (504, pointFromText('POINT( -4.01    2.5  )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2, -2.2  3.3,  -3.3  3.3 ))'), null, null, null)
INSERT INTO _table VALUES (505, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -109.05 37, -102.05 37, -102.05 41))'), null, null, null)
INSERT INTO _table VALUES (506, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 41, -104.05 45))'), null, null, null)
INSERT INTO _table VALUES (507, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98563 40.75800, -73.98695 40.75603, -73.98644 40.75582, -73.98554 40.75796, -73.98461 40.75920))'), null, null, null)
INSERT INTO _table VALUES (511, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0  90, -90   0))'), null, null, null)
INSERT INTO _table VALUES (512, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  90   0,   0  90))'), null, null, null)
INSERT INTO _table VALUES (513, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0 -90,  90   0))'), null, null, null)
INSERT INTO _table VALUES (514, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0,   0 -90))'), null, null, null)
INSERT INTO _table VALUES (515, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,   0  90))'), null, null, null)
INSERT INTO _table VALUES (516, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,   0  90,  90   0))'), null, null, null)
INSERT INTO _table VALUES (517, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0,   0 -90))'), null, null, null)
INSERT INTO _table VALUES (518, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,   0 -90, -90   0))'), null, null, null)
INSERT INTO _table VALUES (521, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4))'), null, null, null)
INSERT INTO _table VALUES (522, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (523, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (524, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04))'), null, null, null)
INSERT INTO _table VALUES (525, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
INSERT INTO _table VALUES (532, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01))'), null, null, null)
INSERT INTO _table VALUES (533, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4))'), null, null, null)
INSERT INTO _table VALUES (535, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02))'), null, null, null)
-- These are invalid due to failure to close an interior hole (ID values from above plus 20; or 30 for the last one, for the second hole)
INSERT INTO _table VALUES (542, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1))'), null, null, null)
INSERT INTO _table VALUES (543, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (545, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01), (-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02))'), null, null, null)
INSERT INTO _table VALUES (553, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01))'), null, null, null)

-- The following polygons are considered invalid by both VoltDB and PostGIS, due to crossing edges:
-- (Note: ID values correspond to a similar polygon above, plus 300)
INSERT INTO _table VALUES (601, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.41  0.41,  0.41 -0.01, -0.01 0.41, -0.01 -0.01))'), null, null, null)
INSERT INTO _table VALUES (602, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, -0.01 0.41,   0.41 0.41, -0.01 -0.01))'), null, null, null)
INSERT INTO _table VALUES (603, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.3   1.3,   1.3 -1.1,   -1.1  1.3,  -1.1  -1.1 ))'), null, null, null)
INSERT INTO _table VALUES (604, pointFromText('POINT( -4.01    2.5  )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2,  -3.3  3.3,   -2.2  3.3,  -3.3   2.2 ))'), null, null, null)
INSERT INTO _table VALUES (605, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -102.05 37, -109.05 37, -102.05 41, -109.05 41))'), null, null, null)
INSERT INTO _table VALUES (606, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 45, -104.05 41, -111.05 45))'), null, null, null)
INSERT INTO _table VALUES (607, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98554 40.75796, -73.98695 40.75603, -73.98644 40.75582, -73.98563 40.75800, -73.98461 40.75920, -73.98512 40.75942))'), null, null, null)
INSERT INTO _table VALUES (611, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0  90, -90   0,    1  1,    0 0))'), null, null, null)
INSERT INTO _table VALUES (612, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  90   0,   0  90,    1 -1,    0 0))'), null, null, null)
INSERT INTO _table VALUES (613, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0 -90,  90   0,   -1 -1,    0 0))'), null, null, null)
INSERT INTO _table VALUES (614, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0,   0 -90,   -1  1,    0 0))'), null, null, null)
INSERT INTO _table VALUES (615, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,   0  90, -179 -1, -180 0))'), null, null, null)
INSERT INTO _table VALUES (616, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,   0  90,  90   0, -179  1,  180 0))'), null, null, null)
INSERT INTO _table VALUES (617, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0,   0 -90,  179  1,  180 0))'), null, null, null)
INSERT INTO _table VALUES (618, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,   0 -90, -90   0,  179 -1, -180 0))'), null, null, null)
INSERT INTO _table VALUES (621, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 2.4, 2.4 -2.2, -2.2 2.4, -2.2 -2.2))'), null, null, null)
-- TODO: commented out (invalid) polygons that have negative Area (according to PostGIS), which causes PostGIS to throw an error
--INSERT INTO _table VALUES (622, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, -2.2 2.4, 2.4 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
--INSERT INTO _table VALUES (623, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 2.4, 2.4 -2.2, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (624, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 1.04, 2.0 1.0, 1.09 0.96, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04, 0.0 1.0))'), null, null, null)
INSERT INTO _table VALUES (625, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 1.04, 2.0 1.0, 1.09 0.96, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04, 0.0 1.0), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
INSERT INTO _table VALUES (632, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 0.01, 0.01 -1.1, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (633, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.4 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (635, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)
-- These are invalid due to an interior hole with crossing edges (ID values from above plus 20; or 30 for the last one, for the second hole)
INSERT INTO _table VALUES (642, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, 0.01 0.01, -1.1 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
--INSERT INTO _table VALUES (643, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 -1.1, 0.01 0.01, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (643, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, 0.01 0.01, -1.1 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (645, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01), (-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))'), null, null, null)
INSERT INTO _table VALUES (653, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.4 0.4, 0.01 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)

-- The following polygons are considered invalid by VoltDB, but valid by PostGIS, which does not care about clockwise vs. counter-clockwise:
-- (Note: ID values correspond to a similar polygon above, plus 600)
-- Similar polygons to the above (with ID 321, 324), without holes, but vertices reversed (clockwise): [invalid!]
INSERT INTO _table VALUES (921, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2))'), null, null, null)
INSERT INTO _table VALUES (924, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 1.04, 0.3 1.7, 0.96 1.09, 1.0 2.0, 1.04 1.09, 1.7 1.7, 1.09 1.04, 2.0 1.0, 1.09 0.96, 1.7 0.3, 1.04 0.91, 1.0 0.0, 0.96 0.91, 0.3 0.3, 0.91 0.96, 0.0 1.0))'), null, null, null)
-- Similar polygons to the above (with ID 322, 323, 325), with reversed (clockwise) exterior vertices: [invalid!]
INSERT INTO _table VALUES (922, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (923, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (925, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 1.04, 0.3 1.7, 0.96 1.09, 1.0 2.0, 1.04 1.09, 1.7 1.7, 1.09 1.04, 2.0 1.0, 1.09 0.96, 1.7 0.3, 1.04 0.91, 1.0 0.0, 0.96 0.91, 0.3 0.3, 0.91 0.96, 0.0 1.0), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
-- Just the holes (like those with ID 332, 333, 335), but clockwise: [invalid!]
INSERT INTO _table VALUES (932, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (933, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (935, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))'), null, null, null)
-- Similar polygons to the above (with ID 322, 323, 325), with reversed (counter-clockwise) holes: [invalid!]
INSERT INTO _table VALUES (942, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (943, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1), (0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (945, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01), (-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)
-- Similar polygons to the above (with ID 322, 323, 325), with reversed exterior vertices (clockwise) AND holes (counter-clockwise): [invalid!]
INSERT INTO _table VALUES (952, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (953, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1), (0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (955, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  0.04, -0.7  0.7 , -0.04 0.09 , 0.01 1.01, 0.04 0.09 , 0.7  0.7 , 0.09  0.04, 1.02 0.01, 0.09  -0.04, 0.7  -0.7 , 0.04 -0.09 , 0.01 -1.01, -0.04 -0.09 , -0.7  -0.7 , -0.09  -0.04, -1.01 0.01), (-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)


-- Useful for debugging the polygons defined above; but otherwise, needs to be commented out,
-- since VoltDB & PostGIS reasons, while generally similar, do not have the same text
--SELECT ID,              IsInvalidReason(POLY1) from _table D01 WHERE _maybe IsValid(POLY1)
--SELECT ID, AREA(POLY1), IsInvalidReason(POLY1) from _table D02 WHERE _maybe IsValid(POLY1)
--SELECT ID, AREA(POLY1), IsInvalidReason(POLY1) from _table D03 WHERE _maybe IsValid(POLY1) AND AREA(POLY1) < 0


-- Test points and the AsText, CAST(point AS VARCHAR), LONGITUDE & LATITUDE functions; also COUNT(*)
SELECT COUNT(*)                                                         from _table G00
SELECT ID,         PT1,                           PT2,            PT3   from _table G01
SELECT ID,  AsText(PT1)            PT1AT,  AsText(PT2)            PT1AT from _table G02
SELECT ID,    CAST(PT1 as VARCHAR) PT1VC,    CAST(PT2 as VARCHAR) PT1VC from _table G03
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2) from _table G04
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2) from _table G05 WHERE LONGITUDE(PT1) < 0 AND LATITUDE(PT1) > 0

-- Test polygons and the AsText, CAST(polygon AS VARCHAR), NumPoints & NumInteriorRings functions
-- (Note: certain troublesome invalid polygons avoided here, with ID > 930)
SELECT ID,    AsText(POLY1)            POLY1AT                      from _table G06 WHERE ID < 930
SELECT ID,      CAST(POLY1 as VARCHAR) POLY1VC                      from _table G07 WHERE ID < 930
SELECT ID, NumPoints(POLY1) NPoints, NumInteriorRings(POLY1) NHoles from _table G08
SELECT ID, NumPoints(POLY1) NPoints, NumInteriorRings(POLY1) NHoles from _table G09 WHERE NumPoints(POLY1) > 5 AND NumInteriorRings(POLY1) > 0

-- Test polygons and the IsValid function (also using AsText)
-- (Note: certain polygons avoided here, whose IsValid has known differences between VoltDB & PostGIS)
SELECT ID,                 AsText(POLY1) from _table G11 WHERE _maybe IsValid(POLY1) AND @ignoreisvaliddiffs
-- This won't work until IsValid (& boolean return values) is supported in the initial SELECT clause
SELECT ID, IsValid(POLY1), AsText(POLY1) from _table G12 WHERE                           @ignoreisvaliddiffs

-- Test the DISTANCE function, with points & polygons
SELECT ID, DISTANCE(PT1,  PT2  ) from _table G18 WHERE                                                         @ignorepointeqdiffs
SELECT ID, DISTANCE(PT1,  PT2  ) from _table G19 WHERE DISTANCE(PT1,  PT2  ) > 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.PT1  ,B.PT1  ) G20DIST FROM _table A JOIN _table B ON A.ID <= B.ID AND   @ignore2pointeqdiffs
SELECT ID, DISTANCE(PT1,  POLY1) from _table G21 WHERE                                                         @ignoreinvalidpolys
SELECT ID, DISTANCE(PT1,  POLY1) from _table G22 WHERE DISTANCE(PT1,  POLY1) > 4000                      AND   @ignoreinvalidpolys
SELECT A.ID AID, B.ID BID, DISTANCE(A.PT1  ,B.POLY1) G23DIST FROM _table A JOIN _table B ON A.ID <> B.ID WHERE @ignore2invalidpolys AND @ignore2pointeqdiffs AND @ignoredistroundoff
SELECT ID, DISTANCE(POLY1,PT2  ) from _table G24 WHERE                                                         @ignoreinvalidpolys
SELECT ID, DISTANCE(POLY1,PT2  ) from _table G25 WHERE DISTANCE(POLY1,PT2  ) > 4000                      AND   @ignoreinvalidpolys
SELECT A.ID AID, B.ID BID, DISTANCE(A.POLY1,B.PT2  ) G26DIST FROM _table A JOIN _table B ON A.ID <> B.ID WHERE @ignore2invalidpolys AND @ignore2pointeqdiffs
-- DISTANCE between two polygons is not yet supported; so this fails in the (VoltDB)
-- planner (even when running against the PostGIS backend, which does call the planner)
SELECT ID, DISTANCE(POLY1,POLY1) from _table G27
SELECT ID, DISTANCE(POLY1,POLY1) from _table G28 WHERE DISTANCE(POLY1,POLY1) < 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.POLY1,B.POLY1) G29DIST FROM _table A JOIN _table B ON A.ID >= B.ID

-- Test joins on (and comparisons of) point or polygon columns
-- (Note: PostGIS considers two geographies to be equal if their 'bounding boxes' are the same, which
--  includes polygons with the same exterior ring but different interior rings, as well as points that
--  are very close together, but not quite identical (see: http://postgis.net/docs/ST_Geometry_EQ.html);
--  so such cases have been excluded here, since VoltDB treats them differently, by design. Similarly,
--  we avoid the use of <, <=, >, >= here, since PostGIS and VoltDB also treat those very differently,
--  by design.)
SELECT   ID,     AsText(PT1)      PT1  ,           AsText(PT2)      PT2   FROM _table G30          WHERE   PT1   @cmp   PT2   AND @ignorepointeqdiffs
SELECT A.ID AID, AsText(A.PT1  ) APT1  , B.ID BID, AsText(B.PT1  ) BPT1   FROM _table A JOIN _table B ON A.PT1   @cmp B.PT1   AND @ignore2pointeqdiffs
SELECT A.ID AID, AsText(A.PT1  ) APT1  , B.ID BID, AsText(B.PT2  ) BPT2   FROM _table A JOIN _table B ON A.PT1   @cmp B.PT2   AND @ignore2pointeqdiffs
SELECT   ID,     AsText(POLY1)    POLY1,           AsText(POLY2  )  POLY2 FROM _table G33          WHERE   POLY1 @cmp   POLY2
SELECT A.ID AID, AsText(A.POLY1) APOLY1, B.ID BID, AsText(B.POLY1) BPOLY1 FROM _table A JOIN _table B ON A.POLY1 @cmp B.POLY1 AND @ignore2diffnumholes AND @ignore2invalidpolys

-- Test polygons and the AREA & CENTROID functions (also using AsText, LONGITUDE, LATITUDE, IsValid)
SELECT ID, POLY1, POLY2, POLY3         from _table G40 WHERE                           @ignoreinvalidpolys
SELECT ID, AREA(POLY1)                 from _table G41 WHERE                           @ignoreinvalidpolys
SELECT ID, AREA(POLY1)                 from _table G42 WHERE AREA(POLY1) > 2000000 AND @ignoreinvalidpolys
SELECT ID, AREA(POLY1), AsText(POLY1)  from _table G43 WHERE                           @ignoreinvalidpolys
SELECT ID, AREA(POLY1), AsText(POLY1)  from _table G44 WHERE AREA(POLY1) > 2000000 AND @ignoreinvalidpolys
-- (Note: certain polygons avoided here, whose CENTROID has known differences between VoltDB & PostGIS)
SELECT ID, AsText(CENTROID(POLY1))                               from _table G45 WHERE @ignorecentroiddiffs
SELECT ID, LONGITUDE(CENTROID(POLY1)), LATITUDE(CENTROID(POLY1)) from _table G46 WHERE @ignorecentroiddiffs
SELECT ID, AREA(POLY1)                 from _table G47 WHERE _maybe IsValid(POLY1) AND @ignoreinvalidpolys
-- This won't work until IsValid (& boolean return values) is supported in the initial SELECT clause
SELECT ID, IsValid(POLY1), AREA(POLY1) from _table G48 WHERE                           @ignoreisvaliddiffs

-- Test the CONTAINS function, with polygons & points (also using LONGITUDE, LATITUDE, AsText)
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1) from _table G51 WHERE _maybe CONTAINS(POLY1,PT1) AND @ignoreinvalidpolys
SELECT ID, AsText(PT1),    AsText(POLY1) from _table G52 WHERE _maybe CONTAINS(POLY1,PT1) AND @ignoreinvalidpolys
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G53 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignoreinvalidpolys
SELECT ID, AsText(PT2),    AsText(POLY1) from _table G54 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignoreinvalidpolys
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G55 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignoreinvalidpolys
SELECT ID, AsText(PT2),    AsText(POLY2) from _table G56 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignoreinvalidpolys
-- Test UPDATE using CENTROID, and re-test CONTAINS afterward
-- (Note: the restriction to only use valid polygons here is only an issue on Ubuntu 12.04, for some reason)
UPDATE _table G55 SET PT2 = CENTROID(POLY1)              WHERE                                @ignoreinvalidpolys
-- (Note: certain polygons avoided here, whose CENTROID has known differences between VoltDB & PostGIS)
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G63 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignorecentroiddiffs
SELECT ID, AsText(PT2),    AsText(POLY1) from _table G64 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignorecentroiddiffs
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G65 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignorecentroiddiffs
SELECT ID, AsText(PT2),    AsText(POLY2) from _table G66 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignorecentroiddiffs

-- These won't work until CONTAINS (& boolean return values) is supported in the initial SELECT clause
SELECT ID, CONTAINS(POLY1,PT1), LONGITUDE(PT1), LATITUDE(PT1) from _table G67
SELECT ID, CONTAINS(POLY1,PT1), AsText(PT1),    AsText(POLY1) from _table G68
