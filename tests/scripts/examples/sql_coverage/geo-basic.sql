<grammar.sql>
{@star = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), AREA(POLY1)"}

-- Test inserting points & polygons, via the pointFromText & polygonFromText functions...

-- Points without polygons:
INSERT INTO _table VALUES (100, null,                                    null,                                   null, null)
INSERT INTO _table VALUES (101, null,                                    pointFromText('POINT(  0.01   0.01 )'), null, null)
INSERT INTO _table VALUES (102, pointFromText('POINT( -1.01    1.01 )'), null,                                   null, null)
INSERT INTO _table VALUES (103, pointFromText('POINT(  0.01    1.01 )'), pointFromText('POINT(  0.01   1.01 )'), null, null)
INSERT INTO _table VALUES (104, pointFromText('POINT( -1.01    0.01 )'), pointFromText('POINT( -0.01  -0.01 )'), null, null)
INSERT INTO _table VALUES (105, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(  1.01   1.01 )'), null, null)
INSERT INTO _table VALUES (106, pointFromText('POINT(  1.01    1.01 )'), pointFromText('POINT(  2.01   2.01 )'), null, null)
INSERT INTO _table VALUES (107, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT(  0.01  -0.01 )'), null, null)
-- Simple ("square", in the lat/long sense) polygons (without holes; and slightly un-square, to reduce round-off errors):
INSERT INTO _table VALUES (201, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.01 -0.01, 0.011 0.011, -0.01 0.01, -0.01 -0.01))') )
INSERT INTO _table VALUES (202, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.01 -0.01, 0.011 0.011, -0.01 0.01, -0.01 -0.01))') )
INSERT INTO _table VALUES (203, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.1  -1.1,  1.11  1.11,  -1.1  1.1,  -1.1  -1.1))') )
INSERT INTO _table VALUES (204, pointFromText('POINT( -4.01    2.5  )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2, -2.2   3.3,   -3.3  3.3,  -3.3   2.2))') )

-- A small polygon: Times Square, New York, NY (approximate)
INSERT INTO _table VALUES (205, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98563 40.75800, -73.98695 40.75603, -73.98644 40.75582, -73.98554 40.75796, -73.98461 40.75920, -73.98512 40.75942))') )

-- Large but simple polygons: Colorado & Wyoming (very approximate, without considering that latitude lines are not great circles)
INSERT INTO _table VALUES (206, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -109.05 37, -102.05 37, -102.05 41, -109.05 41))') )
INSERT INTO _table VALUES (207, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 41, -104.05 45, -111.05 45))') )

-- Super-large polygons, dividing the Earth up into 8 octants:
-- (Note: the longitude of the North and South Pole is arbitrary; however, it has been set here to the values
--  -45, 45, -135, 135, in a (partially successful) attempt to avoid a PostGIS bug in computing the CENTROID)
-- First, the "front": the North-West, North-East, South-East & South-West octants that touch the Prime (Greenwich) Meridian
INSERT INTO _table VALUES (311, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -45  90, -90   0,    0 0))') )
INSERT INTO _table VALUES (312, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  90   0,  45  90,    0 0))') )
INSERT INTO _table VALUES (313, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  45 -90,  90   0,    0 0))') )
INSERT INTO _table VALUES (314, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0, -45 -90,    0 0))') )
-- Then, the "back": the North-West, North-East, South-East & South-West octants that touch the Antimeridian (International Date Line)
INSERT INTO _table VALUES (315, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,-135  90, -180 0))') )
INSERT INTO _table VALUES (316, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0, 135  90,  90   0,  180 0))') )
INSERT INTO _table VALUES (317, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0, 135 -90,  180 0))') )
INSERT INTO _table VALUES (318, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,-135 -90, -90   0, -180 0))') )

-- Note: valid polygon exterior vertices must be listed counter-clockwise; but interior hole vertices must be clockwise:
-- A simple ("square-ish") polygon, without a hole (slightly un-square, to reduce round-off errors):
INSERT INTO _table VALUES (221, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2))') )
-- A simple ("square-ish") polygon, with a ("square", asymmetric) hole:
INSERT INTO _table VALUES (222, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))') )
-- A simple ("square-ish") polygon, with 2 ("square", asymmetric) holes:
INSERT INTO _table VALUES (223, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
-- A star-shaped polygon (approximately), with 8 points (16 vertices), without a hole:
INSERT INTO _table VALUES (224, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01))') )
-- An 8-point (approximately) star-shaped polygon, with an (approximately) star-shaped (symmetric, half-sized) hole:
INSERT INTO _table VALUES (225, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01),(-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))') )
-- A simple ("square-ish") polygon, with 9 holes:
-- (Note: hole 6 changed to use 2.429 (not 2.43), in order to avoid 'Loop 0 crosses loop 6')
INSERT INTO _table VALUES (226, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((2.43 48.89, 2.25 48.89, 2.25 48.83, 2.43 48.83, 2.43 48.89),(2.37 48.87, 2.37 48.85, 2.31 48.85, 2.31 48.87, 2.37 48.87),(2.40 48.88, 2.40 48.87, 2.37 48.87, 2.37 48.88, 2.40 48.88),(2.40 48.85, 2.40 48.84, 2.37 48.84, 2.37 48.85, 2.40 48.85),(2.31 48.85, 2.31 48.84, 2.28 48.84, 2.28 48.85, 2.31 48.85),(2.31 48.88, 2.31 48.87, 2.28 48.87, 2.28 48.88, 2.31 48.88),(2.429 48.86, 2.38 48.855, 2.38 48.865, 2.429 48.86),(2.25 48.86, 2.30 48.865, 2.30 48.855, 2.25 48.86),(2.34 48.85, 2.37 48.835, 2.31 48.835, 2.34 48.85),(2.34 48.89, 2.36 48.875, 2.32 48.875, 2.34 48.89))') )

-- Just the holes (but counter-clockwise), from the above (with ID 222, 223, 225: 227, 228 are both holes from 223):
INSERT INTO _table VALUES (227, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))') )
INSERT INTO _table VALUES (228, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.3 0.01, 0.3 0.3, 0.01 0.3, 0.01 0.01))') )
INSERT INTO _table VALUES (229, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))') )


-- The following polygons are considered invalid by both VoltDB and PostGIS, due to failure to close a loop (i.e., the last vertex is not the
-- same as the first); these INSERT statements, unlike the subsequent ones, will actually return an error, so the INSERT will not succeed
-- (Note: ID values correspond to a similar polygon above, plus 500)
INSERT INTO _table VALUES (701, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.01 -0.01, 0.01 0.01, -0.01 0.01))') )
INSERT INTO _table VALUES (702, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.01 -0.01, 0.01 0.01, -0.01 0.01))') )
INSERT INTO _table VALUES (703, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.1  -1.1,  1.1  1.1,  -1.1  1.1))') )
INSERT INTO _table VALUES (704, pointFromText('POINT( -4.01    2.5  )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2, -2.2  3.3,  -3.3  3.3))') )
INSERT INTO _table VALUES (705, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98563 40.75800, -73.98695 40.75603, -73.98644 40.75582, -73.98554 40.75796, -73.98461 40.75920))') )
INSERT INTO _table VALUES (706, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -109.05 37, -102.05 37, -102.05 41))') )
INSERT INTO _table VALUES (707, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 41, -104.05 45))') )
INSERT INTO _table VALUES (711, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0  90, -90   0))') )
INSERT INTO _table VALUES (712, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  90   0,   0  90))') )
INSERT INTO _table VALUES (713, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0 -90,  90   0))') )
INSERT INTO _table VALUES (714, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0,   0 -90))') )
INSERT INTO _table VALUES (715, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,   0  90))') )
INSERT INTO _table VALUES (716, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,   0  90,  90   0))') )
INSERT INTO _table VALUES (717, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0,   0 -90))') )
INSERT INTO _table VALUES (718, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,   0 -90, -90   0))') )
INSERT INTO _table VALUES (721, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2))') )
INSERT INTO _table VALUES (722, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))') )
INSERT INTO _table VALUES (723, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
INSERT INTO _table VALUES (724, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04))') )
INSERT INTO _table VALUES (725, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04),(-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))') )
INSERT INTO _table VALUES (727, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01))') )
INSERT INTO _table VALUES (728, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.3 0.01, 0.3 0.3, 0.01 0.3))') )
INSERT INTO _table VALUES (729, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02))') )
-- These are invalid due to failure to close an interior hole (ID values from above plus 10; or 20 for the last one, for the second hole)
INSERT INTO _table VALUES (732, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1))') )
INSERT INTO _table VALUES (733, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
INSERT INTO _table VALUES (735, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01),(-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02))') )
INSERT INTO _table VALUES (743, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01))') )

-- The following polygons are considered invalid by both VoltDB and PostGIS, due to crossing edges:
-- (Note: ID values correspond to a similar polygon above, plus 600)
INSERT INTO _table VALUES (801, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.01  0.01,  0.01 -0.01, -0.01 0.01, -0.01 -0.01))') )
INSERT INTO _table VALUES (802, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.01 -0.01, -0.01 0.01,   0.01 0.01, -0.01 -0.01))') )
INSERT INTO _table VALUES (803, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.1   1.1,   1.1 -1.1,   -1.1  1.1,  -1.1  -1.1 ))') )
INSERT INTO _table VALUES (804, pointFromText('POINT( -4.01    2.5  )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2,  -3.3  3.3,   -2.2  3.3,  -3.3   2.2 ))') )
INSERT INTO _table VALUES (805, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98554 40.75796, -73.98695 40.75603, -73.98644 40.75582, -73.98563 40.75800, -73.98461 40.75920, -73.98512 40.75942))') )
INSERT INTO _table VALUES (806, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -102.05 37, -109.05 37, -102.05 41, -109.05 41))') )
INSERT INTO _table VALUES (807, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 45, -104.05 41, -111.05 45))') )
INSERT INTO _table VALUES (811, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0  90, -90   0,    1  1,    0 0))') )
INSERT INTO _table VALUES (812, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  90   0,   0  90,    1 -1,    0 0))') )
INSERT INTO _table VALUES (813, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0 -90,  90   0,   -1 -1,    0 0))') )
INSERT INTO _table VALUES (814, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0,   0 -90,   -1  1,    0 0))') )
INSERT INTO _table VALUES (815, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,   0  90, -179 -1, -180 0))') )
INSERT INTO _table VALUES (816, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,   0  90,  90   0, -179  1,  180 0))') )
INSERT INTO _table VALUES (817, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0,   0 -90,  179  1,  180 0))') )
INSERT INTO _table VALUES (818, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,   0 -90, -90   0,  179 -1, -180 0))') )
INSERT INTO _table VALUES (821, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.22 2.2, 2.2 -2.2, -2.2 2.2, -2.2 -2.2))') )
-- TODO: commented out (invalid) polygons that have negative Area (according to PostGIS), which causes PostGIS to throw an error
--INSERT INTO _table VALUES (822, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, -2.2 2.2, 2.22 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))') )
--INSERT INTO _table VALUES (823, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.22 2.2, 2.2 -2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
INSERT INTO _table VALUES (824, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09   0.04, 1.02 0.01, 0.09 -0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01))') )
INSERT INTO _table VALUES (825, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09   0.04, 1.02 0.01, 0.09 -0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01),(-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))') )
INSERT INTO _table VALUES (827, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 0.01, 0.01 -1.1, -1.1 0.01, -1.1 -1.1))') )
INSERT INTO _table VALUES (828, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.3 0.01, 0.3 0.3, 0.01 0.3, 0.3 0.3, 0.01 0.01))') )
INSERT INTO _table VALUES (829, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))') )
-- These are invalid due to an interior hole with crossing edges (ID values from above plus 10; or 20 for the last one, for the second hole)
INSERT INTO _table VALUES (832, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, 0.01 0.01, -1.1 0.01, 0.01 -1.1, -1.1 -1.1))') )
INSERT INTO _table VALUES (833, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 -1.1, 0.01 0.01, -1.1 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
INSERT INTO _table VALUES (835, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01),(-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))') )
INSERT INTO _table VALUES (843, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1),(0.01 0.01, 0.3 0.3, 0.01 0.3, 0.3 0.01, 0.01 0.01))') )

-- The following polygons are considered invalid by VoltDB, but valid by PostGIS, which does not care about clockwise vs. counter-clockwise:
-- (Note: ID values correspond to a similar polygon above, plus 700)
-- Similar polygons to the above (with ID 221, 224), without holes, but vertices reversed (clockwise): [invalid!]
INSERT INTO _table VALUES (921, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.2, 2.22 2.2, 2.2 -2.2, -2.2 -2.2))') )
INSERT INTO _table VALUES (924, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  0.04, -0.7  0.7 , -0.04 0.09 , 0.01 1.01, 0.04 0.09 , 0.7  0.7 , 0.09  0.04, 1.02 0.01, 0.09  -0.04, 0.7  -0.7 , 0.04 -0.09 , 0.01 -1.01, -0.04 -0.09 , -0.7  -0.7 , -0.09  -0.04, -1.01 0.01))') )
-- Similar polygons to the above (with ID 222, 223, 225), with reversed (clockwise) exterior vertices: [invalid!]
INSERT INTO _table VALUES (922, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.2, 2.22 2.2, 2.2 -2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))') )
INSERT INTO _table VALUES (923, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.2, 2.22 2.2, 2.2 -2.2, -2.2 -2.2),(-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1),(0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
INSERT INTO _table VALUES (925, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  0.04, -0.7  0.7 , -0.04 0.09 , 0.01 1.01, 0.04 0.09 , 0.7  0.7 , 0.09  0.04, 1.02 0.01, 0.09  -0.04, 0.7  -0.7 , 0.04 -0.09 , 0.01 -1.01, -0.04 -0.09 , -0.7  -0.7 , -0.09  -0.04, -1.01 0.01),(-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))') )
-- Just the holes (like those with ID 227, 228, 229), but clockwise: [invalid!]
INSERT INTO _table VALUES (927, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))') )
INSERT INTO _table VALUES (928, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.01 0.3, 0.3 0.3, 0.3 0.01, 0.01 0.01))') )
INSERT INTO _table VALUES (929, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))') )
-- Similar polygons to the above (with ID 222, 223, 225), with reversed (counter-clockwise) holes: [invalid!]
INSERT INTO _table VALUES (932, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))') )
INSERT INTO _table VALUES (933, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.2 -2.2, 2.22 2.2, -2.2 2.2, -2.2 -2.2),(-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1),(0.01 0.01, 0.3 0.01, 0.3 0.3, 0.01 0.3, 0.01 0.01))') )
INSERT INTO _table VALUES (935, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01),(-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))') )
-- Similar polygons to the above (with ID 222, 223, 225), with reversed exterior vertices (clockwise) AND holes (counter-clockwise): [invalid!]
INSERT INTO _table VALUES (942, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.2, 2.22 2.2, 2.2 -2.2, -2.2 -2.2),(-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))') )
INSERT INTO _table VALUES (943, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.2, 2.22 2.2, 2.2 -2.2, -2.2 -2.2),(-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1),(0.01 0.01, 0.3 0.01, 0.3 0.3, 0.01 0.3, 0.01 0.01))') )
INSERT INTO _table VALUES (945, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  0.04, -0.7  0.7 , -0.04 0.09 , 0.01 1.01, 0.04 0.09 , 0.7  0.7 , 0.09  0.04, 1.02 0.01, 0.09  -0.04, 0.7  -0.7 , 0.04 -0.09 , 0.01 -1.01, -0.04 -0.09 , -0.7  -0.7 , -0.09  -0.04, -1.01 0.01),(-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))') )


-- Useful for debugging the polygons defined above; but otherwise, needs
-- to be commented out, since VoltDB & PostGIS reasons do not agree exactly
--SELECT ID,              IsInvalidReason(POLY1) from _table D01 WHERE _maybe IsValid(POLY1)
--SELECT ID, AREA(POLY1), IsInvalidReason(POLY1) from _table D02 WHERE _maybe IsValid(POLY1)
--SELECT ID, AREA(POLY1), IsInvalidReason(POLY1) from _table D03 WHERE _maybe IsValid(POLY1) AND AREA(POLY1) < 0


-- Test points and the AsText, CAST(point AS VARCHAR), LONGITUDE & LATITUDE functions; also COUNT(*)
SELECT COUNT(*)                                                         from _table G01
SELECT ID,  AsText(PT1)            PT1AT,  AsText(PT2)            PT1AT from _table G02
SELECT ID,    CAST(PT1 as VARCHAR) PT1VC,    CAST(PT2 as VARCHAR) PT1VC from _table G03
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2) from _table G04
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2) from _table G05 WHERE LONGITUDE(PT1) < 0 AND LATITUDE(PT1) > 0

-- Test polygons and the AsText, CAST(polygon AS VARCHAR), NumPoints & NumInteriorRings functions
SELECT ID,    AsText(POLY1)            POLY1AT                      from _table G06
SELECT ID,      CAST(POLY1 as VARCHAR) POLY1VC                      from _table G07
SELECT ID, NumPoints(POLY1) NPoints, NumInteriorRings(POLY1) NHoles from _table G08
SELECT ID, NumPoints(POLY1) NPoints, NumInteriorRings(POLY1) NHoles from _table G09 WHERE NumPoints(POLY1) > 5 AND NumInteriorRings(POLY1) > 0

-- Before any of the remaining tests, must delete those polygons that VoltDB considers invalid, but PostGIS considers valid
-- (It can be interesting to comment this out to see the differences, but many tests will fail)
DELETE from _table where ID > 900

-- Test polygons and the IsValid function (also using AsText)
SELECT ID,                 AsText(POLY1) from _table G11 WHERE _maybe IsValid(POLY1)
-- This won't work until IsValid (& boolean return values) is supported in the initial SELECT clause
SELECT ID, IsValid(POLY1), AsText(POLY1) from _table G12

-- Before any of the remaining tests, must delete those invalid polygons whose edges cross, since
-- VoltDB & PostGIS sometimes give different answers for DISTANCE, AREA, etc., in such cases
-- (It can be interesting to comment this out to see the differences, but many tests will fail)
DELETE from _table where ID > 800

-- Test the DISTANCE function, with points & polygons
SELECT ID, DISTANCE(PT1,  PT2  ) from _table G18
SELECT ID, DISTANCE(PT1,  PT2  ) from _table G19 WHERE DISTANCE(PT1,  PT2  ) > 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.PT1  ,B.PT1  ) G20DIST FROM _table A JOIN _table B ON A.ID <= B.ID
SELECT ID, DISTANCE(PT1,  POLY1) from _table G21
SELECT ID, DISTANCE(PT1,  POLY1) from _table G22 WHERE DISTANCE(PT1,  POLY1) > 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.PT1  ,B.POLY1) G23DIST FROM _table A JOIN _table B ON A.ID <= B.ID
SELECT ID, DISTANCE(POLY1,PT1  ) from _table G24
SELECT ID, DISTANCE(POLY1,PT1  ) from _table G25 WHERE DISTANCE(POLY1,PT1  ) > 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.POLY1,B.PT1  ) G26DIST FROM _table A JOIN _table B ON A.ID <= B.ID
-- DISTANCE between two polygons is not yet supported
SELECT ID, DISTANCE(POLY1,POLY1) from _table G27
SELECT ID, DISTANCE(POLY1,POLY1) from _table G28 WHERE DISTANCE(POLY1,POLY1) < 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.POLY1,B.POLY1) G29DIST FROM _table A JOIN _table B ON A.ID <= B.ID

-- Test polygons and the AREA & CENTROID functions (also using AsText, LONGITUDE, LATITUDE, IsValid)
SELECT ID, AREA(POLY1)                 from _table G31
SELECT ID, AREA(POLY1)                 from _table G32 WHERE AREA(POLY1) > 2000000
SELECT ID, AREA(POLY1), AsText(POLY1)  from _table G33
SELECT ID, AREA(POLY1), AsText(POLY1)  from _table G34 WHERE AREA(POLY1) > 2000000
SELECT ID, AsText(CENTROID(POLY1))     from _table G35
SELECT ID, LONGITUDE(CENTROID(POLY1)), LATITUDE(CENTROID(POLY1)) from _table G36
SELECT ID, AREA(POLY1)                 from _table G36 WHERE _maybe IsValid(POLY1)
-- This won't work until IsValid (& boolean return values) is supported in the initial SELECT clause
SELECT ID, IsValid(POLY1), AREA(POLY1) from _table G37

-- Test the CONTAINS function, with polygons & points (also using LONGITUDE, LATITUDE, AsText)
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1) from _table G41 WHERE _maybe CONTAINS(POLY1,PT1)
SELECT ID, AsText(PT1),    AsText(POLY1) from _table G42 WHERE _maybe CONTAINS(POLY1,PT1)
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G43 WHERE _maybe CONTAINS(POLY1,PT2)
SELECT ID, AsText(PT2),    AsText(POLY1) from _table G44 WHERE _maybe CONTAINS(POLY1,PT2)
-- Test UPDATE using CENTROID, and re-test CONTAINS afterward
UPDATE _table G45 SET PT2 = CENTROID(POLY1)
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G46 WHERE _maybe CONTAINS(POLY1,PT2)
SELECT ID, AsText(PT2),    AsText(POLY1) from _table G47 WHERE _maybe CONTAINS(POLY1,PT2)

-- These won't work until CONTAINS (& boolean return values) is supported in the initial SELECT clause
SELECT ID, CONTAINS(POLY1,PT1), LONGITUDE(PT1), LATITUDE(PT1) from _table G48
SELECT ID, CONTAINS(POLY1,PT1), AsText(PT1),    AsText(POLY1) from _table G49
