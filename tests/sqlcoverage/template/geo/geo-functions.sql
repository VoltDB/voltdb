<grammar.sql>
{@cmp = "_eqne"} -- geo types don't do <, >, <=, >= (at least, not in a way that agrees with PostGIS)
{@star = "ID, LONGITUDE(PT1), LATITUDE(PT1), LONGITUDE(PT2), LATITUDE(PT2), AREA(POLY1)"}

-- Define clauses used to ignore points or polygons that demonstrate known differences between
-- VoltDB and PostGIS, which would otherwise cause tests to fail. The ID numbers correspond
-- specifically to values in the hard-coded test data defined below, that were shown to uncover
-- incompatibilities with PostGIS. These incompatibilities are currently deemed acceptable and
-- mostly uninteresting.
-- First, PostGIS, oddly, considers different, but equivalent, representations of the North or
-- South pole (e.g. 'POINT(0 90)' and 'POINT(90 90)' to be both equal and unequal (see Comments
-- in ENG-9804; and also ENG-9991, which is a minor VoltDB bug); adding these clauses ignores
-- certain points at the North and South poles:
{@ignorepointeqdiffs  = "(ID < 350 OR ID > 360)"} -- ignore known differences with point equality/inequality, between VoltDB and PostGIS
-- Same as above, for two tables with a join (and for a few more points); plus, PostGIS considers
-- the points defined below with ID 307 and 308, which are extremely close to each other, to be
-- equal, but VoltDB does not
{@ignore2pointeqdiffs = "((A.ID < 340 OR A.ID > 360) AND (B.ID < 340 OR B.ID > 360) AND (B.ID < 307 OR B.ID > 308))"}

-- This just ignores a tiny round-off error, when computing DISTANCE between a point and a polygon
{@ignoredistroundoff  = "A.ID <> 553"} -- ignore tiny DISTANCE round-off errors, between VoltDB and PostGIS

-- PostGIS considers two polygons to be equal if their exterior rings are equal, even if their
-- interior rings (holes) are different; VoltDB considers two polygons to be equal only if both
-- their exterior and interior rings are the same; adding this clause causes PostGIS to behave
-- more like VoltDB, in this regard
{@ignore2diffnumholes = "NumInteriorRings(A.POLY1) = NumInteriorRings(B.POLY1)"} -- ignore 2 polygons with different numbers of holes, with a join

-- VoltDB and PostGIS have different definitions of validity of a polygon. While they agree in
-- some cases (e.g., both consider a polygon whose edges cross to be invalid), VoltDB considers
-- a polygon to be valid only when its exterior ring is defined counter-clockwise, and all
-- interior rings (if any) are defined clockwise; PostGIS ignores this, and considers clockwise
-- exterior or counter-clockwise interior rings to be valid. On the other hand, PostGIS considers
-- any polygon that covers the North pole or the South pole, and some (with interior rings, i.e.,
-- holes) that cross the Antimeridian (International Date Line), to be invalid; VoltDB considers
-- all of those to be valid.
{@ignoreisvaliddiffs = "ID < 800"} -- ignore known differences with IsValid(polygon), between VoltDB and PostGIS

-- Since invalid polygons are not necessarily well-defined (e.g., when their edges cross), calling
-- functions like DISTANCE, AREA or CONTAINS on them may give unpredictable results, which sometimes
-- differ between VoltDB and PostGIS
{@ignoreinvalidpolys  = "ID < 600"} -- ignore polygons that are invalid, according to VoltDB and/or PostGIS
{@ignore2invalidpolys = "A.ID < 600 AND B.ID < 600"} -- ignore 2 invalid polygons, with a join

-- PostGIS's equivalent of VoltDB's CENTROID function, ST_Centroid, is a geometric, not geographic,
-- function, which essentially means that it assumes a flat Earth; this becomes more and more
-- inaccurate, the larger a polygon gets, and the further it is from the equator
{@ignorecentroiddiffs = "ID < 500 AND ID > 300"} -- ignore known differences with CENTROID(polygon), between VoltDB and PostGIS


-- Insert some random points & polygons, within certain areas:
-- PT1 and POLY1 are within Colorado (Northern and Western Hemispheres);
-- PT2 and POLY2 are within Australia's Northern Territory (Southern and Eastern Hemispheres);
-- PT3 is anywhere on Earth (with 50% chance of being NULL);
-- POLY3 is within Wyoming, with ~5 holes (and 10% chance of being NULL);
-- RATIO is a number between 0 and 1 (with 25% chance of being NULL);
-- ID starts at 0 and increments, currently to 255; we cannot add additional
-- random values here, without running into the ID values used below
INSERT INTO _table VALUES (_id, _value[point:-109.05,-102.05,37,41], _value[point:129,139,-26,-11 null25], _value[point null50], _value[polygon], _value[polygon:129,139,-26,-11 null25], _value[polygon:-111.05,-104.05,41,45;-5 null10], _value[float null25])

-- Test inserting points & polygons, via the pointFromText & polygonFromText functions....
-- Note that the ID values are carefully chosen to make the above clauses work

-- Points without polygons:
-- (Also set the RATIO to non-null values here, since one test of CAST uses RATIO)
INSERT INTO _table VALUES (300, null,                                    null,                                   null, null, null, null, 0.0)
INSERT INTO _table VALUES (301, null,                                    pointFromText('POINT(  0.01   0.01 )'), null, null, null, null, 0.1)
INSERT INTO _table VALUES (302, pointFromText('POINT( -1.01    1.01 )'), null,                                   null, null, null, null, 0.2)
INSERT INTO _table VALUES (303, pointFromText('POINT(  0.01    1.01 )'), pointFromText('POINT(  0.01   1.01 )'), null, null, null, null, 0.3)
INSERT INTO _table VALUES (304, pointFromText('POINT( -1.01    0.01 )'), pointFromText('POINT( -0.01  -0.01 )'), null, null, null, null, 0.4)
INSERT INTO _table VALUES (305, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(  1.01   1.01 )'), null, null, null, null, 0.5)
INSERT INTO _table VALUES (306, pointFromText('POINT(  1.01    1.01 )'), pointFromText('POINT(  2.01   2.01 )'), null, null, null, null, 0.6)
-- Points very close to each other (near Herald Square, New York, NY):
INSERT INTO _table VALUES (307, pointFromText('POINT(-73.988  40.7504)'),pointFromText('POINT(-73.988 40.75037)'),null,null, null, null, 0.7)
INSERT INTO _table VALUES (308, pointFromText('POINT(-73.988  40.750401)'),pointFromText('POINT(-73.98801 40.7504)'),null,null,null,null,0.8)

-- Points with different but equivalent values, i.e., longitude 180 and -180, or differing longitudes at the poles
-- North pole (all these points are equivalent to each other):
INSERT INTO _table VALUES (341, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(   0.0  90.0  )'), null, null, null, null, -1.1)
INSERT INTO _table VALUES (342, pointFromText('POINT( -90.0    90.0 )'), pointFromText('POINT( -90.0  90.0  )'), null, null, null, null, -2.2)
INSERT INTO _table VALUES (343, pointFromText('POINT( 180.0    90.0 )'), pointFromText('POINT( 180.0  90.0  )'), null, null, null, null, -3.3)
INSERT INTO _table VALUES (351, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(  90.0  90.0  )'), null, null, null, null,  1.1)
INSERT INTO _table VALUES (352, pointFromText('POINT( 180.0    90.0 )'), pointFromText('POINT(-180.0  90.0  )'), null, null, null, null,  2.2)
INSERT INTO _table VALUES (353, pointFromText('POINT(  45.0    90.0 )'), pointFromText('POINT( -45.0  90.0  )'), null, null, null, null,  3.3)
-- South pole (all these points are equivalent to each other):
INSERT INTO _table VALUES (344, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(   0.0 -90.0  )'), null, null, null, null, -4.4)
INSERT INTO _table VALUES (345, pointFromText('POINT(  90.0   -90.0 )'), pointFromText('POINT(  90.0 -90.0  )'), null, null, null, null, -5.5)
INSERT INTO _table VALUES (346, pointFromText('POINT(-180.0   -90.0 )'), pointFromText('POINT(-180.0 -90.0  )'), null, null, null, null, -6.6)
INSERT INTO _table VALUES (354, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(  90.0 -90.0  )'), null, null, null, null,  4.4)
INSERT INTO _table VALUES (355, pointFromText('POINT(-180.0   -90.0 )'), pointFromText('POINT( 180.0 -90.0  )'), null, null, null, null,  5.5)
INSERT INTO _table VALUES (356, pointFromText('POINT( 135.0   -90.0 )'), pointFromText('POINT(-135.0 -90.0  )'), null, null, null, null,  6.6)
-- On the Anti-meridian (International Date Line), longitude 180 or -180
-- (each pair of points in a row is equivalent, but the points in different rows are different):
INSERT INTO _table VALUES (347, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT( 180.0   0.0  )'), null, null, null, null, -7.7)
INSERT INTO _table VALUES (348, pointFromText('POINT(-180.0    45.0 )'), pointFromText('POINT(-180.0  45.0  )'), null, null, null, null, -8.8)
INSERT INTO _table VALUES (349, pointFromText('POINT( 180.0   -45.0 )'), pointFromText('POINT( 180.0 -45.0  )'), null, null, null, null, -9.9)
INSERT INTO _table VALUES (357, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT(-180.0   0.0  )'), null, null, null, null,  7.7)
INSERT INTO _table VALUES (358, pointFromText('POINT(-180.0    45.0 )'), pointFromText('POINT( 180.0  45.0  )'), null, null, null, null,  8.8)
INSERT INTO _table VALUES (359, pointFromText('POINT( 180.0   -45.0 )'), pointFromText('POINT(-180.0 -45.0  )'), null, null, null, null,  9.9)


-- Polygons (& points):
-- Simple ("square", in the lat/long sense) polygons (without holes; and slightly un-square, to reduce round-off errors):
INSERT INTO _table VALUES (401, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41, -0.01 -0.01))'), null, null, null)
INSERT INTO _table VALUES (402, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41, -0.01 -0.01))'), null, null, null)
INSERT INTO _table VALUES (403, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.3  -1.1,  1.3  1.3,  -1.1  1.3,  -1.1  -1.1 ))'), null, null, null)
INSERT INTO _table VALUES (404, pointFromText('POINT( -4.01    2.51 )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2, -2.22 3.32, -3.3  3.3,  -3.3   2.2 ))'), null, null, null)

-- Large but simple polygons: Colorado & Wyoming (very approximate, without considering that latitude lines are not great circles)
-- (The first point for each is within that state; the second is in Boston, MA, which is not)
INSERT INTO _table VALUES (505, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -109.05 37, -102.05 37, -102.05 41, -109.05 41))'), null, null, null)
INSERT INTO _table VALUES (506, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 41, -104.05 45, -111.05 45))'), null, null, null)

-- Small polygons: Times Square & Herald Square, New York, NY (approximate)
-- (The first point for each is within that square; the second is just outside it)
INSERT INTO _table VALUES (407, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98563 40.75800, -73.98695 40.75603, -73.98644 40.75582, -73.98554 40.75796, -73.98461 40.75920, -73.98512 40.75942))'), null, null, null)
INSERT INTO _table VALUES (408, pointFromText('POINT(-73.98759 40.75039)'), pointFromText('POINT(-73.98800 40.75037)'), null, polygonFromText('POLYGON((-73.98783 40.75070, -73.98798 40.74988, -73.98776 40.74978, -73.98728 40.75046, -73.98783 40.75070))'), null, null, null)

-- Super-large polygons, dividing the Earth up into 8 octants:
-- (Note: the longitude of the North and South Pole is arbitrary; however, it has been set here to the values
--  -45, 45, -135, 135, in a (partially successful) attempt to avoid a PostGIS bug in computing the CENTROID)
-- First, the "front": the North-West, North-East, South-East & South-West octants that touch where the Prime (Greenwich) Meridian meets the Equator
INSERT INTO _table VALUES (511, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -45  90, -90   0,    0 0))'), null, null, null)
-- This one has been made slightly smaller, to avoid a PostGIS bug (it works fine either way, in VoltDB):
--INSERT INTO _table VALUES (512, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 0 0,  90   0,  45  90,    0 0))'), null, null, null)
INSERT INTO _table VALUES (512, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   1 1,  89   1,  45  89,    1 1))'), null, null, null)
INSERT INTO _table VALUES (513, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  45 -90,  90   0,    0 0))'), null, null, null)
INSERT INTO _table VALUES (514, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0, -45 -90,    0 0))'), null, null, null)
-- Then, the "back": the North-West, North-East, South-East & South-West octants that touch the Antimeridian (International Date Line) meets the Equator
INSERT INTO _table VALUES (515, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,-135  90, -180 0))'), null, null, null)
INSERT INTO _table VALUES (516, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0, 135  90,  90   0,  180 0))'), null, null, null)
INSERT INTO _table VALUES (517, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0, 135 -90,  180 0))'), null, null, null)
INSERT INTO _table VALUES (518, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,-135 -90, -90   0, -180 0))'), null, null, null)

-- Note: valid polygon exterior vertices must be listed counter-clockwise; but interior hole vertices must be clockwise:
-- A simple ("square-ish") polygon, without a hole (slightly un-square, to reduce round-off errors):
INSERT INTO _table VALUES (421, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2))'), null, null, null)
-- A simple ("square-ish") polygon, with a ("square", asymmetric) hole:
INSERT INTO _table VALUES (422, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
-- A simple ("square-ish") polygon, with 2 ("square", asymmetric) holes:
INSERT INTO _table VALUES (423, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
-- A star-shaped polygon (approximately), with 8 points (16 vertices), without a hole:
INSERT INTO _table VALUES (424, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04, 0.0 1.0))'), null, null, null)
-- An 8-point (approximately) star-shaped polygon, with an (approximately) star-shaped (symmetric, half-sized) hole:
INSERT INTO _table VALUES (425, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04, 0.0 1.0), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
-- A simple ("square-ish") polygon, with 9 holes (around Paris, France):
-- (Note: hole 6 changed to use 2.42999 (not 2.43), in order to avoid 'Loop 0 crosses loop 6', due to round-off)
INSERT INTO _table VALUES (426, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((2.43 48.89, 2.25 48.89, 2.25 48.83, 2.43 48.83, 2.43 48.89), (2.37 48.87, 2.37 48.85, 2.31 48.85, 2.31 48.87, 2.37 48.87), (2.40 48.88, 2.40 48.87, 2.37 48.87, 2.37 48.88, 2.40 48.88), (2.40 48.85, 2.40 48.84, 2.37 48.84, 2.37 48.85, 2.40 48.85), (2.31 48.85, 2.31 48.84, 2.28 48.84, 2.28 48.85, 2.31 48.85), (2.31 48.88, 2.31 48.87, 2.28 48.87, 2.28 48.88, 2.31 48.88), (2.42999 48.86, 2.38 48.855, 2.38 48.865, 2.42999 48.86), (2.25 48.86, 2.30 48.865, 2.30 48.855, 2.25 48.86), (2.34 48.85, 2.37 48.835, 2.31 48.835, 2.34 48.85), (2.34 48.89, 2.36 48.875, 2.32 48.875, 2.34 48.89))'), null, null, null)

-- Just the holes (but counter-clockwise), from the above (with ID 422, 423, 425: 432, 433 are both holes from 423):
INSERT INTO _table VALUES (432, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (433, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (435, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)

-- Polygons (without holes) covering the North Pole, the South Pole, and (part of) the Antimeridian (International Date Line);
-- Note: most of these (except the one with ID=553) are considered valid by VoltDB, but invalid by PostGIS:
INSERT INTO _table VALUES (851, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(  0.0   88.0  )'), null, polygonFromText('POLYGON((  0  88,  90  88,  180  88,  -90  88,   0  88))'), null, null, null)
INSERT INTO _table VALUES (852, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(  0.0  -88.0  )'), null, polygonFromText('POLYGON((  0 -88, -90 -88,  180 -88,   90 -88,   0 -88))'), null, null, null)
INSERT INTO _table VALUES (553, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT(177.0    0.0  )'), null, polygonFromText('POLYGON((178   2, 178  -2, -178  -2, -178   2, 178   2))'), null, null, null)
-- Polygons with holes that cover the North Pole, the South Pole, and (part of) the Antimeridian (International Date Line)
INSERT INTO _table VALUES (861, pointFromText('POINT(   0.0    90.0 )'), pointFromText('POINT(  0.0   88.0  )'), null, polygonFromText('POLYGON((  0  88,  90  88,  180  88,  -90  88,   0  88), (  0  89,  -90  89,  180  89,  90  89,   0  89))'), null, null, null)
INSERT INTO _table VALUES (862, pointFromText('POINT(   0.0   -90.0 )'), pointFromText('POINT(  0.0  -88.0  )'), null, polygonFromText('POLYGON((  0 -88, -90 -88,  180 -88,   90 -88,   0 -88), (  0 -89,   90 -89,  180 -89, -90 -89,   0 -89))'), null, null, null)
INSERT INTO _table VALUES (863, pointFromText('POINT( 180.0     0.0 )'), pointFromText('POINT(177.0    0.0  )'), null, polygonFromText('POLYGON((178   2, 178  -2, -178  -2, -178   2, 178   2), (179   1, -179   1, -179  -1, 179  -1, 179   1))'), null, null, null)

-- The following polygons are considered invalid by both VoltDB and PostGIS, due to failure to close a loop (i.e., the last vertex is not the
-- same as the first); these INSERT statements, unlike the subsequent ones, will actually return an error, so the INSERT will not succeed
-- (Note: ID values correspond to a similar polygon above, plus 300)
INSERT INTO _table VALUES (701, null,                                    null,                                   null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41))'), null, null, null)
INSERT INTO _table VALUES (702, pointFromText('POINT(  0.005   0.005)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.01 -0.01, 0.41 -0.01, 0.41 0.41, -0.01 0.41))'), null, null, null)
INSERT INTO _table VALUES (703, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.1  -1.1,  1.3  -1.1,  1.3  1.3,  -1.1  1.3 ))'), null, null, null)
INSERT INTO _table VALUES (704, pointFromText('POINT( -4.01    2.5  )'), pointFromText('POINT( -2.5    2.5  )'), null, polygonFromText('POLYGON((-3.3   2.2, -2.2   2.2, -2.2  3.3,  -3.3  3.3 ))'), null, null, null)
INSERT INTO _table VALUES (705, pointFromText('POINT(-105.5   39.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-109.05 41, -109.05 37, -102.05 37, -102.05 41))'), null, null, null)
INSERT INTO _table VALUES (706, pointFromText('POINT(-107.5   43.1  )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-111.05 45, -111.05 41, -104.05 41, -104.05 45))'), null, null, null)
INSERT INTO _table VALUES (707, pointFromText('POINT(-73.98558 40.75798)'), pointFromText('POINT(-73.98552 40.75796)'), null, polygonFromText('POLYGON((-73.98512 40.75942, -73.98563 40.75800, -73.98695 40.75603, -73.98644 40.75582, -73.98554 40.75796, -73.98461 40.75920))'), null, null, null)
INSERT INTO _table VALUES (711, pointFromText('POINT(-71.065  42.355)'), pointFromText('POINT( 71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0  90, -90   0))'), null, null, null)
INSERT INTO _table VALUES (712, pointFromText('POINT( 71.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,  90   0,   0  90))'), null, null, null)
INSERT INTO _table VALUES (713, pointFromText('POINT( 71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0,   0 -90,  90   0))'), null, null, null)
INSERT INTO _table VALUES (714, pointFromText('POINT(-71.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((   0 0, -90   0,   0 -90))'), null, null, null)
INSERT INTO _table VALUES (715, pointFromText('POINT(-99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0, -90   0,   0  90))'), null, null, null)
INSERT INTO _table VALUES (716, pointFromText('POINT( 99.065  42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,   0  90,  90   0))'), null, null, null)
INSERT INTO _table VALUES (717, pointFromText('POINT( 99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON(( 180 0,  90   0,   0 -90))'), null, null, null)
INSERT INTO _table VALUES (718, pointFromText('POINT(-99.065 -42.355)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-180 0,   0 -90, -90   0))'), null, null, null)
INSERT INTO _table VALUES (721, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4))'), null, null, null)
INSERT INTO _table VALUES (722, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (723, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (724, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04))'), null, null, null)
INSERT INTO _table VALUES (725, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 0.96, 0.3 0.3, 0.96 0.91, 1.0 0.0, 1.04 0.91, 1.7 0.3, 1.09 0.96, 2.0 1.0, 1.09 1.04, 1.7 1.7, 1.04 1.09, 1.0 2.0, 0.96 1.09, 0.3 1.7, 0.91 1.04), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
INSERT INTO _table VALUES (732, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01))'), null, null, null)
INSERT INTO _table VALUES (733, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4))'), null, null, null)
INSERT INTO _table VALUES (735, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02))'), null, null, null)
-- These are invalid due to failure to close an interior hole (ID values from above plus 20; or 30 for the last one, for the second hole)
INSERT INTO _table VALUES (742, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT(  -0.05 -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1))'), null, null, null)
INSERT INTO _table VALUES (743, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (745, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01), (-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02))'), null, null, null)
INSERT INTO _table VALUES (753, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT(  -0.5  -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01))'), null, null, null)

-- The following polygons are considered invalid by both VoltDB and PostGIS, due to crossing edges:
-- (Note: ID values correspond to a similar polygon above, plus 200)
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
-- (Note: ID values correspond to a similar polygon above, plus 500)
-- Similar polygons to the above (with ID 421, 424), without holes, but vertices reversed (clockwise): [invalid!]
INSERT INTO _table VALUES (921, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2))'), null, null, null)
INSERT INTO _table VALUES (924, pointFromText('POINT( -0.099  -0.044)'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 1.04, 0.3 1.7, 0.96 1.09, 1.0 2.0, 1.04 1.09, 1.7 1.7, 1.09 1.04, 2.0 1.0, 1.09 0.96, 1.7 0.3, 1.04 0.91, 1.0 0.0, 0.96 0.91, 0.3 0.3, 0.91 0.96, 0.0 1.0))'), null, null, null)
-- Similar polygons to the above (with ID 422, 423, 425), with reversed (clockwise) exterior vertices: [invalid!]
INSERT INTO _table VALUES (922, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (923, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1), (0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (925, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((0.0 1.0, 0.91 1.04, 0.3 1.7, 0.96 1.09, 1.0 2.0, 1.04 1.09, 1.7 1.7, 1.09 1.04, 2.0 1.0, 1.09 0.96, 1.7 0.3, 1.04 0.91, 1.0 0.0, 0.96 0.91, 0.3 0.3, 0.91 0.96, 0.0 1.0), (0.5 1.0, 0.955 1.02, 0.65 1.35, 0.98 1.045, 1.0 1.5, 1.02 1.045, 1.35 1.35, 1.045 1.02, 1.44 1.0, 1.045 0.98, 1.35 0.65, 1.02 0.955, 1.0 0.5, 0.98 0.955, 0.65 0.65, 0.955 0.98, 0.5 1.0))'), null, null, null)
-- Just the holes (like those with ID 432, 433, 435), but clockwise: [invalid!]
INSERT INTO _table VALUES (932, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-1.1 -1.1, -1.1 0.01, 0.01 0.01, 0.01 -1.1, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (933, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((0.01 0.01, 0.01 0.4, 0.4 0.4, 0.4 0.01, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (935, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-0.5 0.01, -0.045 0.02, -0.35 0.35, -0.02 0.045, 0.01 0.5, 0.02 0.045, 0.35 0.35, 0.045 0.02, 0.5 0.01, 0.045 -0.02, 0.35 -0.35, 0.02 -0.045, 0.01 -0.5, -0.02 -0.045, -0.35 -0.35, -0.045 -0.02, -0.5 0.01))'), null, null, null)
-- Similar polygons to the above (with ID 422, 423, 425), with reversed (counter-clockwise) holes: [invalid!]
INSERT INTO _table VALUES (942, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (943, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, 2.4 -2.2, 2.4 2.4, -2.2 2.4, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1), (0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (945, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  -0.04, -0.7  -0.7 , -0.04 -0.09 , 0.01 -1.01, 0.04 -0.09 , 0.7  -0.7 , 0.09  -0.04, 1.02 0.01, 0.09  0.04, 0.7  0.7 , 0.04 0.09 , 0.01 1.01, -0.04  0.09, -0.7  0.7 , -0.09 0.04 , -1.01 0.01), (-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)
-- Similar polygons to the above (with ID 422, 423, 425), with reversed exterior vertices (clockwise) AND holes (counter-clockwise): [invalid!]
INSERT INTO _table VALUES (952, pointFromText('POINT( -0.5    -0.5  )'), pointFromText('POINT( -0.05  -0.1  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1))'), null, null, null)
INSERT INTO _table VALUES (953, pointFromText('POINT( -0.05   -0.1  )'), pointFromText('POINT( -0.5   -0.5  )'), null, polygonFromText('POLYGON((-2.2 -2.2, -2.2 2.4, 2.4 2.4, 2.4 -2.2, -2.2 -2.2), (-1.1 -1.1, 0.01 -1.1, 0.01 0.01, -1.1 0.01, -1.1 -1.1), (0.01 0.01, 0.4 0.01, 0.4 0.4, 0.01 0.4, 0.01 0.01))'), null, null, null)
INSERT INTO _table VALUES (955, pointFromText('POINT(  0.01    0.01 )'), pointFromText('POINT(-71.065 42.355)'), null, polygonFromText('POLYGON((-1.01 0.01, -0.09  0.04, -0.7  0.7 , -0.04 0.09 , 0.01 1.01, 0.04 0.09 , 0.7  0.7 , 0.09  0.04, 1.02 0.01, 0.09  -0.04, 0.7  -0.7 , 0.04 -0.09 , 0.01 -1.01, -0.04 -0.09 , -0.7  -0.7 , -0.09  -0.04, -1.01 0.01), (-0.5 0.01, -0.045 -0.02, -0.35 -0.35, -0.02 -0.045, 0.01 -0.5, 0.02 -0.045, 0.35 -0.35, 0.045 -0.02, 0.5 0.01, 0.045 0.02, 0.35 0.35, 0.02 0.045, 0.01 0.5, -0.02 0.045, -0.35 0.35, -0.045 0.02, -0.5 0.01))'), null, null, null)


-- These are useful for debugging the polygons defined above; but otherwise, they need to be commented
-- out, since VoltDB & PostGIS reasons, while generally similar, do not have the same text
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

-- Test polygons and the AsText & CAST(polygon AS VARCHAR) functions
-- (Note: certain troublesome invalid polygons avoided here, with ID > 930)
SELECT ID,    AsText(POLY1)            POLY1AT                      from _table G06 WHERE ID < 930
SELECT ID,      CAST(POLY1 as VARCHAR) POLY1VC                      from _table G07 WHERE ID < 930
-- Test using CAST to a non-Geo column type (this is to make sure that the SQL transformation
-- to PostGIS, in PostGISBackend.java, does not break CAST to other column types)
SELECT ID, CAST(ID as VARCHAR) IDVC, RATIO, CAST(CAST(FLOOR(RATIO*10000+0.5) as INTEGER) as VARCHAR) RVC from _table G08

-- Test polygons and the NumPoints & NumInteriorRings functions (also using IsValid)
SELECT ID, NumPoints(POLY1) NPoints, NumInteriorRings(POLY1) NHoles from _table G10
SELECT ID, NumPoints(POLY1) NPoints, NumInteriorRings(POLY1) NHoles from _table G11 WHERE NumPoints(POLY1) > 5 AND NumInteriorRings(POLY1) > 0
SELECT ID, NumPoints(POLY2) NPoints, NumInteriorRings(POLY2) NHoles from _table G12 WHERE _maybe IsValid(POLY2) ORDER BY NHoles, NPoints, ID
SELECT ID, NumPoints(POLY3) NPoints, NumInteriorRings(POLY3) NHoles from _table G13 WHERE _maybe IsValid(POLY3) ORDER BY NHoles, NPoints, ID

-- Test polygons and the IsValid function (also using AsText)
-- (Note: certain polygons avoided here, whose IsValid has known differences between VoltDB & PostGIS)
SELECT ID, AsText(POLY1)                            from _table G14 WHERE _maybe IsValid(POLY1) AND   @ignoreisvaliddiffs
-- This won't work until IsValid (& boolean return values) is supported in the initial SELECT clause
SELECT ID, AsText(POLY1),           IsValid(POLY1)  from _table G15                             WHERE @ignoreisvaliddiffs
-- Equivalent query, using a CASE statement
SELECT ID, AsText(POLY1), CASE WHEN IsValid(POLY1) THEN 'True' ELSE 'False' END from _table G16 WHERE @ignoreisvaliddiffs

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
SELECT ID, DISTANCE(POLY1,POLY2) from _table G27
SELECT ID, DISTANCE(POLY1,POLY2) from _table G28 WHERE DISTANCE(POLY1,POLY2) < 200000
SELECT A.ID AID, B.ID BID, DISTANCE(A.POLY1,B.POLY1) G29DIST FROM _table A JOIN _table B ON A.ID >= B.ID

-- Test the DWithin function, with points & polygons (also using DISTANCE)
SELECT ID, DISTANCE(PT1,  PT2  ) from _table G31 WHERE DWithin(PT1,  PT2  , 200000) AND @ignorepointeqdiffs
SELECT ID, DISTANCE(PT1,  POLY1) from _table G32 WHERE DWithin(PT1,  POLY1, 200000) AND @ignoreinvalidpolys
SELECT ID, DISTANCE(POLY1,PT2  ) from _table G33 WHERE DWithin(POLY1,PT2  , 200000) AND @ignoreinvalidpolys
-- DISTANCE & DWithin between two polygons is not yet supported; so this fails in the (VoltDB)
-- planner (even when running against the PostGIS backend, which does call the planner)
SELECT ID, DISTANCE(POLY1,POLY2) from _table G34 WHERE DWithin(POLY1,POLY2, 200000)
-- These won't work until DWithin (& boolean return values) is supported in the initial SELECT clause
SELECT ID, DISTANCE(PT1,  PT2  ), DWithin(PT1,  PT2  , 200000) from _table G35    WHERE @ignorepointeqdiffs
SELECT ID, DISTANCE(PT1,  POLY1), DWithin(PT1,  POLY1, 200000) from _table G36    WHERE @ignoreinvalidpolys
SELECT ID, DISTANCE(POLY1,PT2  ), DWithin(POLY1,PT2  , 200000) from _table G37    WHERE @ignoreinvalidpolys
-- This fails for both of the reasons above (DISTANCE & DWithin between two polygons and boolean return values)
SELECT ID, DISTANCE(POLY1,POLY2), DWithin(POLY1,POLY2, 200000) from _table G38
-- Equivalent queries, using a CASE statement
SELECT ID, DISTANCE(PT1,  PT2  ), CASE WHEN DWithin(PT1,  PT2  , 200000) THEN 'True' ELSE 'False' END from _table G45 WHERE @ignorepointeqdiffs
SELECT ID, DISTANCE(PT1,  POLY1), CASE WHEN DWithin(PT1,  POLY1, 200000) THEN 'True' ELSE 'False' END from _table G46 WHERE @ignoreinvalidpolys
SELECT ID, DISTANCE(POLY1,PT2  ), CASE WHEN DWithin(POLY1,PT2  , 200000) THEN 'True' ELSE 'False' END from _table G47 WHERE @ignoreinvalidpolys
-- Again, this equivalent query fails because DISTANCE & DWithin between two polygons is not yet supported
SELECT ID, DISTANCE(POLY1,POLY2), CASE WHEN DWithin(POLY1,POLY2, 200000) THEN 'True' ELSE 'False' END from _table G48

-- Test joins on (and comparisons of) point or polygon columns
-- (Note: PostGIS considers two geographies to be equal if their 'bounding boxes' are the same, which
--  includes polygons with the same exterior ring but different interior rings, as well as points that
--  are very close together, but not quite identical (see: http://postgis.net/docs/ST_Geometry_EQ.html);
--  so such cases have been excluded here, since VoltDB treats them differently, by design. Similarly,
--  we avoid the use of <, <=, >, >= here, since PostGIS and VoltDB also treat those very differently,
--  by design.)
SELECT   ID,     AsText(PT1)      PT1  ,           AsText(PT2)      PT2   FROM _table G50          WHERE   PT1   @cmp   PT2   AND @ignorepointeqdiffs
SELECT A.ID AID, AsText(A.PT1  ) APT1  , B.ID BID, AsText(B.PT1  ) BPT1   FROM _table A JOIN _table B ON A.PT1   @cmp B.PT1   AND @ignore2pointeqdiffs
SELECT A.ID AID, AsText(A.PT1  ) APT1  , B.ID BID, AsText(B.PT2  ) BPT2   FROM _table A JOIN _table B ON A.PT1   @cmp B.PT2   AND @ignore2pointeqdiffs
SELECT   ID,     AsText(POLY1)    POLY1,           AsText(POLY2  )  POLY2 FROM _table G53          WHERE   POLY1 @cmp   POLY2
SELECT A.ID AID, AsText(A.POLY1) APOLY1, B.ID BID, AsText(B.POLY1) BPOLY1 FROM _table A JOIN _table B ON A.POLY1 @cmp B.POLY1 AND @ignore2diffnumholes AND @ignore2invalidpolys

-- Test polygons and the AREA function (also using AsText, IsValid)
SELECT ID, POLY1, POLY2, POLY3         from _table G60 WHERE IsValid(POLY3)        AND @ignoreinvalidpolys
SELECT ID, AREA(POLY1)                 from _table G61 WHERE                           @ignoreinvalidpolys
SELECT ID, AREA(POLY1)                 from _table G62 WHERE AREA(POLY1) > 2000000 AND @ignoreinvalidpolys
SELECT ID, AREA(POLY1), AsText(POLY1)  from _table G63 WHERE                           @ignoreinvalidpolys
SELECT ID, AREA(POLY1), AsText(POLY1)  from _table G64 WHERE AREA(POLY1) > 2000000 AND @ignoreinvalidpolys
SELECT ID, AREA(POLY1)                 from _table G65 WHERE _maybe IsValid(POLY1) AND @ignoreinvalidpolys
-- This won't work until IsValid (& boolean return values) is supported in the initial SELECT clause
SELECT ID, AREA(POLY1), IsValid(POLY1) from _table G66 WHERE                           @ignoreinvalidpolys
-- Equivalent query, using a CASE statement
SELECT ID, AREA(POLY1), CASE WHEN IsValid(POLY1) THEN 'True' ELSE 'False' END from _table G67 WHERE @ignoreinvalidpolys

-- Test polygons and the CENTROID function (also using AsText, LONGITUDE, LATITUDE)
-- (Note: certain polygons avoided here, whose CENTROID has known differences between VoltDB & PostGIS)
SELECT ID, AsText(CENTROID(POLY1))                               from _table G71 WHERE @ignorecentroiddiffs
SELECT ID, LONGITUDE(CENTROID(POLY1)), LATITUDE(CENTROID(POLY1)) from _table G72 WHERE @ignorecentroiddiffs

-- Test the CONTAINS function, with polygons & points (also using LONGITUDE, LATITUDE, AsText)
SELECT ID, LONGITUDE(PT1), LATITUDE(PT1) from _table G81 WHERE _maybe CONTAINS(POLY1,PT1) AND @ignoreinvalidpolys
SELECT ID, AsText(PT1),    AsText(POLY1) from _table G82 WHERE _maybe CONTAINS(POLY1,PT1) AND @ignoreinvalidpolys
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G83 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignoreinvalidpolys
SELECT ID, AsText(PT2),    AsText(POLY1) from _table G84 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignoreinvalidpolys
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G85 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignoreinvalidpolys
SELECT ID, AsText(PT2),    AsText(POLY2) from _table G86 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignoreinvalidpolys

-- Test UPDATE using CENTROID, and re-test CONTAINS afterward
-- (Note: the restriction to only use valid polygons here is only an issue on Ubuntu 12.04, for some reason)
UPDATE _table G90 SET PT2 = CENTROID(POLY1)              WHERE                                @ignoreinvalidpolys
-- (Note: certain polygons avoided here, whose CENTROID has known differences between VoltDB & PostGIS)
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G93 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignorecentroiddiffs
SELECT ID, AsText(PT2),    AsText(POLY1) from _table G94 WHERE _maybe CONTAINS(POLY1,PT2) AND @ignorecentroiddiffs
SELECT ID, LONGITUDE(PT2), LATITUDE(PT2) from _table G95 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignorecentroiddiffs
SELECT ID, AsText(PT2),    AsText(POLY2) from _table G96 WHERE _maybe CONTAINS(POLY2,PT2) AND @ignorecentroiddiffs

-- This won't work until CONTAINS (& boolean return values) is supported in the initial SELECT clause
SELECT ID, AsText(PT1),    AsText(POLY1), CONTAINS(POLY1,PT1) from _table G98           WHERE @ignoreinvalidpolys
-- Equivalent query, using a CASE statement
SELECT ID, AsText(PT1),    AsText(POLY1), CASE WHEN CONTAINS(POLY1,PT1) THEN 'True' ELSE 'False' END from _table G99 WHERE @ignoreinvalidpolys
