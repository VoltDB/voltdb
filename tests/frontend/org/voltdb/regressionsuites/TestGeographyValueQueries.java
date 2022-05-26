/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

public class TestGeographyValueQueries extends RegressionSuite {
    public TestGeographyValueQueries(String name) {
        super(name);
    }

    private final String BERMUDA_TRIANGLE_WKT = "POLYGON ("
            + "(-64.751 32.305, "
            + "-80.437 25.244, "
            + "-66.371 18.476, "
            + "-64.751 32.305))";

    // The Bermuda Triangle with a square hole inside
    private final String BERMUDA_TRIANGLE_HOLE_WKT = "POLYGON ("
            + "(-64.751 32.305, "
            + "-80.437 25.244, "
            + "-66.371 18.476, "
            + "-64.751 32.305), "
            + "(-67.448 27.026, "
            + "-67.448 25.968, "
            + "-68.992 25.968, "
            + "-68.992 27.026, "
            + "-67.448 27.026))";

    // (Useful for testing comparisons since it has the same number of vertices as
    // the Bermuda Triangle)
    private final String BILLERICA_TRIANGLE_WKT = "POLYGON ("
            + "(-71.276 42.571, "
            + "-71.308 42.547, "
            + "-71.231 42.533, "
            + "-71.276 42.571))";

    // The dreaded "Lowell Square".  One loop,
    // five vertices (last is the same as the first)
    private final String LOWELL_SQUARE_WKT = "POLYGON ("
            + "(-71.338 42.641, "
            + "-71.340 42.619, "
            + "-71.313 42.617, "
            + "-71.316 42.639, "
            + "-71.338 42.641))";

    private final GeographyValue BERMUDA_TRIANGLE_POLY = new GeographyValue(BERMUDA_TRIANGLE_WKT);
    private final GeographyValue BERMUDA_TRIANGLE_HOLE_POLY = new GeographyValue(BERMUDA_TRIANGLE_HOLE_WKT);
    private final GeographyValue BILLERICA_TRIANGLE_POLY = new GeographyValue(BILLERICA_TRIANGLE_WKT);
    private final GeographyValue LOWELL_SQUARE_POLY = new GeographyValue(LOWELL_SQUARE_WKT);

    private final String[] TABLES = {"t", "pt"};
    private final String[] NOT_NULL_TABLES = {"t_not_null", "pt_not_null"};

    private int fillTable(Client client, String tbl, int startPk) throws Exception {
        VoltTable vt = client.callProcedure(tbl + ".Insert", startPk, "Bermuda Triangle",
                BERMUDA_TRIANGLE_POLY).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        ++startPk;

        vt = client.callProcedure(tbl + ".Insert", startPk, "Bermuda Triangle with a hole",
                BERMUDA_TRIANGLE_HOLE_POLY).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        ++startPk;

        vt = client.callProcedure(tbl + ".Insert", startPk, "Billerica Triangle",
                BILLERICA_TRIANGLE_POLY).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        ++startPk;

        vt = client.callProcedure(tbl + ".Insert", startPk, "Lowell Square",
                LOWELL_SQUARE_POLY).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        ++startPk;

        client.callProcedure(tbl + ".Insert", startPk, "null poly", null);
        ++startPk;

        return startPk;
    }

    public void testNullValues() throws Exception {
        Client client = getClient();

        for (String tbl : TABLES) {
            validateTableOfScalarLongs(client, "select * from " + tbl, new long[] {});

            // Insert a null via default value
            validateTableOfScalarLongs(client, "insert into " + tbl + " (pk) values (0);", new long[] {1});

            VoltTable vt = client.callProcedure("@AdHoc", "select poly from " + tbl).getResults()[0];
            assertTrue(vt.toString().contains("NULL"));

            assertTrue(vt.advanceRow());
            GeographyValue gv = vt.getGeographyValue(0);
            assertTrue(vt.wasNull());
            assertEquals(null, gv);
            assertFalse(vt.advanceRow());

            // This produces a null geography since the function argument is null
            vt = client.callProcedure("@AdHoc", "select polygonfromtext(null) from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            gv = vt.getGeographyValue(0);
            assertTrue(vt.wasNull());
            assertEquals(null, gv);
            assertFalse(vt.advanceRow());

            // This tests the is null predicate for this type
            vt = client.callProcedure("@AdHoc", "select poly from " + tbl + " where poly is null").getResults()[0];
            assertTrue(vt.advanceRow());
            gv = vt.getGeographyValue(0);
            assertTrue(vt.wasNull());
            assertEquals(null, gv);
            assertFalse(vt.advanceRow());

            // Try inserting a SQL literal null, which takes a different code path.
            validateTableOfScalarLongs(client, "delete from " + tbl, new long[] {1});
            validateTableOfScalarLongs(client, "insert into " + tbl + " values (0, 'boo', null);", new long[] {1});

            vt = client.callProcedure("@AdHoc", "select poly from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            gv = vt.getGeographyValue(0);
            assertTrue(vt.wasNull());
            assertEquals(null, gv);
            assertFalse(vt.advanceRow());

            // Insert a null by passing a null reference.
            validateTableOfScalarLongs(client, "delete from " + tbl, new long[] {1});
            vt = client.callProcedure(tbl + ".Insert", 0, "null geog", null).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});

            vt = client.callProcedure("@AdHoc", "select poly from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            gv = vt.getGeographyValue(0);
            assertTrue(vt.wasNull());
            assertEquals(null, gv);
            assertFalse(vt.advanceRow());

            // Insert a null by passing an instance of the null sigil
            validateTableOfScalarLongs(client, "delete from " + tbl, new long[] {1});
            vt = client.callProcedure(tbl + ".Insert", 0, "null geog", VoltType.NULL_GEOGRAPHY).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});

            vt = client.callProcedure("@AdHoc", "select poly from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            gv = vt.getGeographyValue(0);
            assertTrue(vt.wasNull());
            assertEquals(null, gv);
            assertFalse(vt.advanceRow());
        }
    }

    public void testInsertAndSimpleSelect() throws IOException, ProcCallException {
        Client client = getClient();

        for (String tbl : TABLES) {
            // There's no rows in here yet.
            validateTableOfScalarLongs(client, "select * from " + tbl, new long[] {});

            // insert using the polygonfromtext function
            validateTableOfScalarLongs(client, "insert into " + tbl + " values(0, 'Bermuda Triangle', "
                    + "polygonfromtext('" + BERMUDA_TRIANGLE_WKT + "'));",
                    new long[] {1});

            VoltTable vt = client.callProcedure("@AdHoc", "select * from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            assertEquals(0, vt.getLong(0));
            assertEquals("Bermuda Triangle", vt.getString(1));
            assertEquals(BERMUDA_TRIANGLE_WKT, vt.getGeographyValue(2).toString());
            assertFalse(vt.advanceRow());

            vt = client.callProcedure("@AdHoc",
                    "select polygonfromtext('" + BERMUDA_TRIANGLE_WKT + "') from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            assertEquals(BERMUDA_TRIANGLE_WKT, vt.getGeographyValue(0).toString());
            assertFalse(vt.advanceRow());
        }
    }

    private void checkOnePolygonParams(long id, String wkt, String label, String tbl, Client client) throws IOException, ProcCallException {
        GeographyValue gv = new GeographyValue(wkt);
        assertEquals(wkt, gv.toString());

        VoltTable vt = client.callProcedure(tbl + ".Insert", id, label, gv).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc", "select * from " + tbl + " where pk = " + id).getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(id, vt.getLong(0));
        assertEquals(label, vt.getString(1));
        assertEquals(wkt, vt.getGeographyValue(2).toString());
        assertFalse(vt.advanceRow());

    }

    public void testParams() throws IOException, ProcCallException {
        Client client = getClient();

        for (String tbl : TABLES) {
            checkOnePolygonParams(101, BERMUDA_TRIANGLE_WKT, "Bermuda Triangle", tbl, client);
            checkOnePolygonParams(102, BERMUDA_TRIANGLE_HOLE_WKT, "Bermuda Triangle With A Hole", tbl, client);
        }
    }

    public void testComparison() throws Exception {
        Client client = getClient();

        for (String tbl : TABLES) {
            fillTable(client, tbl, 0);
        }

        for (String tbl : TABLES) {

            // equals
            VoltTable vt = client.callProcedure("@AdHoc",
                    "select t1.pk, t1.name, t1.poly from " + tbl +
                            " as t1, t as t2 where t1.poly = t2.poly order by t1.pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle", BERMUDA_TRIANGLE_POLY},
                    {1, "Bermuda Triangle with a hole", BERMUDA_TRIANGLE_HOLE_POLY},
                    {2, "Billerica Triangle", BILLERICA_TRIANGLE_POLY},
                    {3, "Lowell Square", LOWELL_SQUARE_POLY}},
                    vt);

            // not equals
            vt = client.callProcedure("@AdHoc",
                    "select t1.pk, t1.name, t2.pk, t2.name from " + tbl +
                            " as t1, t as t2 where t1.poly != t2.poly order by t1.pk, t2.pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle", 1, "Bermuda Triangle with a hole"},
                    {0, "Bermuda Triangle", 2, "Billerica Triangle"},
                    {0, "Bermuda Triangle", 3, "Lowell Square"},
                    {1, "Bermuda Triangle with a hole", 0, "Bermuda Triangle"},
                    {1, "Bermuda Triangle with a hole", 2, "Billerica Triangle"},
                    {1, "Bermuda Triangle with a hole", 3, "Lowell Square"},
                    {2, "Billerica Triangle", 0, "Bermuda Triangle"},
                    {2, "Billerica Triangle", 1, "Bermuda Triangle with a hole"},
                    {2, "Billerica Triangle", 3, "Lowell Square"},
                    {3, "Lowell Square", 0, "Bermuda Triangle"},
                    {3, "Lowell Square", 1, "Bermuda Triangle with a hole"},
                    {3, "Lowell Square", 2, "Billerica Triangle"}},
                    vt);

            // less than
            vt = client.callProcedure("@AdHoc",
                    "select t1.pk, t1.name, t2.pk, t2.name from " + tbl +
                            " as t1, t as t2 where t1.poly < t2.poly order by t1.pk, t2.pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle", 1, "Bermuda Triangle with a hole"},
                    {0, "Bermuda Triangle", 3, "Lowell Square"},
                    {2, "Billerica Triangle", 0, "Bermuda Triangle"},
                    {2, "Billerica Triangle", 1, "Bermuda Triangle with a hole"},
                    {2, "Billerica Triangle", 3, "Lowell Square"},
                    {3, "Lowell Square", 1, "Bermuda Triangle with a hole"}},
                    vt);

            // less than or equal to
            vt = client.callProcedure("@AdHoc",
                    "select t1.pk, t1.name, t2.pk, t2.name from " + tbl +
                            " as t1, t as t2 where t1.poly <= t2.poly order by t1.pk, t2.pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle", 0, "Bermuda Triangle"},
                    {0, "Bermuda Triangle", 1, "Bermuda Triangle with a hole"},
                    {0, "Bermuda Triangle", 3, "Lowell Square"},
                    {1, "Bermuda Triangle with a hole", 1, "Bermuda Triangle with a hole"},
                    {2, "Billerica Triangle", 0, "Bermuda Triangle"},
                    {2, "Billerica Triangle", 1, "Bermuda Triangle with a hole"},
                    {2, "Billerica Triangle", 2, "Billerica Triangle"},
                    {2, "Billerica Triangle", 3, "Lowell Square"},
                    {3, "Lowell Square", 1, "Bermuda Triangle with a hole"},
                    {3, "Lowell Square", 3, "Lowell Square"}},
                    vt);

            // greater than
            vt = client.callProcedure("@AdHoc",
                    "select t1.pk, t1.name, t2.pk, t2.name from " + tbl +
                            " as t1, t as t2 where t1.poly > t2.poly order by t1.pk, t2.pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle", 2, "Billerica Triangle"},
                    {1, "Bermuda Triangle with a hole", 0, "Bermuda Triangle"},
                    {1, "Bermuda Triangle with a hole", 2, "Billerica Triangle"},
                    {1, "Bermuda Triangle with a hole", 3, "Lowell Square"},
                    {3, "Lowell Square", 0, "Bermuda Triangle"},
                    {3, "Lowell Square", 2, "Billerica Triangle"}},
                    vt);

            // greater than or equal to
            vt = client.callProcedure("@AdHoc",
                    "select t1.pk, t1.name, t2.pk, t2.name from " + tbl +
                            " as t1, t as t2 where t1.poly >= t2.poly order by t1.pk, t2.pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle", 0, "Bermuda Triangle"},
                    {0, "Bermuda Triangle", 2, "Billerica Triangle"},
                    {1, "Bermuda Triangle with a hole", 0, "Bermuda Triangle"},
                    {1, "Bermuda Triangle with a hole", 1, "Bermuda Triangle with a hole"},
                    {1, "Bermuda Triangle with a hole", 2, "Billerica Triangle"},
                    {1, "Bermuda Triangle with a hole", 3, "Lowell Square"},
                    {2, "Billerica Triangle", 2, "Billerica Triangle"},
                    {3, "Lowell Square", 0, "Bermuda Triangle"},
                    {3, "Lowell Square", 2, "Billerica Triangle"},
                    {3, "Lowell Square", 3, "Lowell Square"}},
                    vt);

            // is null
            vt = client.callProcedure("@AdHoc",
                    "select pk, name from " + tbl + " where poly is null order by pk").getResults()[0];
            assertContentOfTable(new Object[][] {{4, "null poly"}}, vt);

            // is not null
            vt = client.callProcedure("@AdHoc",
                    "select pk, name from " + tbl + " where poly is not null order by pk").getResults()[0];
            assertContentOfTable(new Object[][] {
                    {0, "Bermuda Triangle"},
                    {1, "Bermuda Triangle with a hole"},
                    {2, "Billerica Triangle"},
                    {3, "Lowell Square"}},
                    vt);
        }
    }

    public void testArithmetic() throws Exception {
        final Client client = getClient();
        fillTable(client, "t", 0);
        final String expectedErrorMessage =
                USING_CALCITE ? "Cannot apply '\\+' to arguments of type" : "incompatible data types in combination";
        verifyStmtFails(client, "select pk, poly + poly from t order by pk", expectedErrorMessage);
        verifyStmtFails(client, "select pk, poly + 1 from t order by pk", expectedErrorMessage);
    }

    // The shell is 5 fixed but arbitrarily selected points.
    // However, the holes are carefully selected to be symmetric
    // around the origin.  This means that the area of each
    // hole is equal, and the area of the cheese is
    // almost exactly equal to the area of the area of the
    // shell minus the sum of the areas of the holes.
    //
    // If the areas were not symmetric in this way, more generally
    // northern holes would have less area than more more
    // generally southern holes in the same equatorial hemisphere.
    private final String cheesyWKT = "POLYGON ((-5.0 46.0, -50.0 10.0, -40.0 -35.0, 25.0 -45.0, 30.0 20.0, -5.0 46.0), "
            + "(  1.0   1.0,  1.0   21.0,  21.0  21.0, 21.0   1.0,   1.0   1.0), "
            + "(  1.0 -21.0,  1.0   -1.0,  21.0  -1.0, 21.0 -21.0,   1.0 -21.0), "
            + "(-21.0 -21.0,-21.0   -1.0,  -1.0  -1.0, -1.0 -21.0, -21.0 -21.0), "
            + "(-21.0   1.0,-21.0   21.0,  -1.0  21.0, -1.0   1.0, -21.0   1.0))";
    private final String cheesyShellWKT = "POLYGON ((-5.0 46.0, -50.0 10.0, -40.0 -35.0, 25.0 -45.0, 30.0 20.0, -5.0 46.0))";

    private void fillCheesyTable(Client client) throws Exception {
        GeographyValue cheesyPolygon = GeographyValue.fromWKT(cheesyWKT);
        GeographyValue cheesyShellPolygon = GeographyValue.fromWKT(cheesyShellWKT);
        //
        // Get the holes from the cheesy polygon, and make them
        // into polygons in their own right.  This means we need
        // to reverse them.
        //
        List<GeographyValue> cheesyHoles = new ArrayList<>();
        List<List<GeographyPointValue>> loops = cheesyPolygon.getRings();
        for (int idx = 1; idx < loops.size(); idx += 1) {
            final List<GeographyPointValue> oneHole = loops.get(idx);
            cheesyHoles.add(new GeographyValue(Collections.singletonList(new ArrayList<GeographyPointValue>() {{
                addAll(oneHole);
                Collections.reverse(this);
            }})));
        }
        String cheesyOrigin = "POINT(0.0 0.0)";
        String cheesyInHole = "POINT(15  15)";
        List<String> exteriorPoints = Arrays.asList("POINT( 60  60)", "POINT( 60 -60)", "POINT(-60 -60)", "POINT(-60  60)");
        List<String> centers = Arrays.asList("POINT( 11  11)", "POINT( 11 -11)", "POINT(-11 -11)", "POINT(-11  11)");
        client.callProcedure("T.INSERT", 0, "SHELL", cheesyShellPolygon);
        client.callProcedure("T.INSERT", 1, "Formaggio", cheesyPolygon);
        for (int idx = 0; idx < cheesyHoles.size(); idx += 1) {
            GeographyValue hole = cheesyHoles.get(idx);
            client.callProcedure("T.INSERT", idx + 100, "hole"+ idx + 100, hole);
        }
        client.callProcedure("LOCATION.INSERT", 0, "ORIGIN", GeographyPointValue.fromWKT(cheesyOrigin));
        client.callProcedure("LOCATION.INSERT", 1, "INHOLE", GeographyPointValue.fromWKT(cheesyInHole));
        for (int idx = 0; idx < exteriorPoints.size(); idx += 1) {
            String exPt = exteriorPoints.get(idx);
            client.callProcedure("LOCATION.INSERT", idx + 200, exPt, GeographyPointValue.fromWKT(exPt));
            idx += 1;
        }
        for (int idx = 0; idx < centers.size(); idx += 1) {
            String ctrPt = centers.get(idx);
            client.callProcedure("LOCATION.INSERT", idx + 300, ctrPt, GeographyPointValue.fromWKT(ctrPt));
        }
        // Make sure that all the polygons
        // are valid.
        VoltTable vt = client.callProcedure("@AdHoc", "select t.pk from t where not isValid(t.poly) order by t.pk").getResults()[0];
        assertEquals("fillCheesyTable: " + vt.getRowCount() + " invalid polygons.", 0, vt.getRowCount());
    }

    // This is mostly a planner test, as the planner had problems recognizing that geo types
    // were compatible with themselves in CASE expressions and that geography was a valid
    // variable-length type.
    public void testCaseWhenElseENG9983ENG9984() throws Exception {
        final double EPSILON = 1.0e-13;
        Client client = getClient();
        fillCheesyTable(client);
        // ENG-9983 CASE WHEN THEN ELSE on geography type.
        VoltTable vt = client.callProcedure("@AdHoc",
                "select CASE WHEN area(t.poly) < area(alt_t.poly) THEN t.poly ELSE alt_t.poly END" +
                " from t, t alt_t where t.pk + 1 = alt_t.pk and t.pk >= 100 order by t.pk;"
                ).getResults()[0];
        assertEquals("Expected (N-1) rows.", 3, vt.getRowCount());
        assertTrue(vt.advanceRow());
        GeographyValue cheesyRoundTripper1 = vt.getGeographyValue(0);

        vt = client.callProcedure("@AdHoc",
                "select CASE WHEN area(t.poly) < area(alt_t.poly) THEN t.poly ELSE alt_t.poly END" +
                " from t, t alt_t where t.pk >= 100 and alt_t.pk >= 100 order by t.pk;"
                ).getResults()[0];
        assertEquals("Expected (N^2) rows.", 16, vt.getRowCount());
        assertTrue(vt.advanceRow());
        GeographyValue cheesyRoundTripper2 = vt.getGeographyValue(0);
        assertApproximatelyEquals("Expected Equivalent Round Trip Polygons", cheesyRoundTripper1, cheesyRoundTripper2, EPSILON);

        // ENG-9983 CASE WHEN THEN ELSE on geography point type.
        vt = client.callProcedure("@AdHoc",
                "select CASE WHEN longitude(l.loc_point) <= longitude(alt_l.loc_point) THEN l.loc_point ELSE alt_l.loc_point END" +
                " from location l, location alt_l where l.pk + 1 = alt_l.pk and l.pk >= 300 order by l.pk;"
                ).getResults()[0];
        assertEquals("Expected (N-1) rows.", 3, vt.getRowCount());
        assertTrue(vt.advanceRow());
        GeographyPointValue cheesyRoundTripper3 = vt.getGeographyPointValue(0);
        vt = client.callProcedure("@AdHoc",
                "select CASE WHEN longitude(l.loc_point) <= longitude(alt_l.loc_point) THEN l.loc_point ELSE alt_l.loc_point END" +
                " from location l, location alt_l where l.pk >= 300 and alt_l.pk >= 300 order by l.pk;"
                ).getResults()[0];
        assertEquals("Expected (N^2) rows.", 16, vt.getRowCount());
        assertTrue(vt.advanceRow());
        GeographyPointValue cheesyRoundTripper4 = vt.getGeographyPointValue(0);
        assertApproximatelyEquals("Expected Equivalent Round Trip Points", cheesyRoundTripper3, cheesyRoundTripper4, EPSILON);

        // ENG-9984 CASE WHEN THEN no ELSE on geography type.
        vt = client.callProcedure("@AdHoc",
                "select CASE WHEN area(t.poly) <= area(alt_t.poly) THEN t.poly END" +
                " from t, t alt_t where t.pk >= 100 and alt_t.pk >= 100 order by t.pk;"
                ).getResults()[0];
        assertEquals("Expected (N^2) rows.", 16, vt.getRowCount());
        assertTrue(vt.advanceRow());
        GeographyValue cheesyRoundTripper5 = vt.getGeographyValue(0);
        assertFalse(vt.wasNull());
        assertApproximatelyEquals("Expected Equivalent Round Trip Polygons", cheesyRoundTripper1, cheesyRoundTripper5, EPSILON);
    }

    public void testLoopOrderInCheesyPolygon() throws Exception {
        final double EPSILON = 1.0e-13;
        Client client = getClient();
        fillCheesyTable(client);
        GeographyValue cheesyPolygon = GeographyValue.fromWKT(cheesyWKT);
        VoltTable vt = client.callProcedure("@AdHoc", "select t.poly from t where t.pk = 1 order by t.pk;").getResults()[0];
        assertEquals("Expected only one row.", 1, vt.getRowCount());
        assertTrue(vt.advanceRow());
        GeographyValue cheesyRoundTripper = vt.getGeographyValue(0);
        assertApproximatelyEquals("Expected Equivalent Round Trip Polygons", cheesyPolygon, cheesyRoundTripper, EPSILON);
    }

    public void testContainsInCheesyPolygon() throws Exception {
        Client client = getClient();
        fillCheesyTable(client);
        // Everything is in the shell (t.pk == 0).
        // Only the origin is in the cheesy polygon (t.pk == 1)
        // Nothing in in t.pk == 2, which is a hole sized shell.
        //    This latter fact is just because the hole-shaped shell
        //    is carefully chosen to not contain the test point.
        //
        // Also, none of the exterior points are contained in
        // the shell or cheesy polygon.
        VoltTable vt = client.callProcedure("@AdHoc",
                "select t.pk, location.pk from t, location   where location.pk < 300 and t.pk < 100 "
                + "        and contains(t.poly, location.loc_point) order by t.pk, location.pk;").getResults()[0];
        Object [][] expectedQ1 = new Object[][] {{0, 0}, {0, 1}, {1, 0}};
        assertContentOfTable(expectedQ1, vt);
    }

    public void testAreasInCheesyPolygon() throws Exception {
        Client client = getClient();
        fillCheesyTable(client);
        VoltTable vt = client.callProcedure("@AdHoc", "select t.pk, area(t.poly), t.name from t order by t.pk;").getResults()[0];
        double[] resmap = new double[2];
        double holeArea = 0;
        while (vt.advanceRow()) {
            int key = (int)vt.getLong(0);
            double value = vt.getDouble(1);
            if (100 <= key && key < 200) {
                holeArea += value;
            } else if (0 <= key && key <= 1) {
                resmap[key] = value;
            }
        }
        double shellArea = resmap[0];
        double cheeseArea = resmap[1];
        // Require that the relative error be
        // less than 1% in absolute value.
        final double AREA_EPSILON = 1.0e-14;
        double relerror = Math.abs((shellArea  - (cheeseArea + holeArea))/shellArea);
        assertTrue("AreaCalculation is incorrect.  ", relerror < AREA_EPSILON);
    }

    public void testDistancesInCheesyPolygons() throws Exception {
        Client client = getClient();
        fillCheesyTable(client);
        // Check that distances from exterior points are not affected
        // by holes.
        VoltTable vt = client.callProcedure("@AdHoc",
                "select t.pk, l.pk, distance(t.poly, l.loc_point)   from t, location as l "
                        + "where 200 <= l.pk and t.pk < 2 order by t.pk, l.pk;")
                .getResults()[0];
        // shellMap.get(n) is the distance between the shell and exterior point pk == n.
        // cheeseMap.get(n) is the distance between the cheese and exterior point with pk == n.
        // indices has all the indices, so that we can iterate over them.
        Map<Long, Double> shellMap = new HashMap<>();
        Map<Long, Double> cheeseMap = new HashMap<>();
        Set<Long> indices = new HashSet<>();
        while (vt.advanceRow()) {
            Long polyKey = vt.getLong(0);
            Long ptKey = vt.getLong(1);
            Double distance = vt.getDouble(2);
            if (polyKey == 0) {
                shellMap.put(ptKey, distance);
            } else if (polyKey == 1) {
                cheeseMap.put(ptKey, distance);
            } else {
                fail("Unexpected polygon : " + polyKey);
            }
            indices.add(ptKey);
        }
        for (Long index: indices) {
            Double shellDist = shellMap.get(index);
            Double cheeseDist = shellMap.get(index);
            assertNotNull("Index " + index + " not found in shell.", shellDist);
            assertNotNull("Index " + index + " not found in cheese.", cheeseDist);
            assertEquals("Expected shell and cheese distance to be equal", shellDist, cheeseDist);
        }
        // Check that the distances inside holes are
        // what we would expect.  We have four square holes,
        // each 20degrees on a side, and all symmetric around
        // the origin (longitude = latitude = 0.0).
        vt = client.callProcedure("@AdHoc",
                "select l.pk, distance(l.loc_point, t.poly) from t, location as l where t.pk = 1 and 300 <= l.pk order by l.pk;")
                .getResults()[0];
        double dist = -1;
        while (vt.advanceRow()) {
            if (dist < 0) {
                dist = vt.getDouble(1);
            } else {
                assertEquals("Distances are not equal", dist, vt.getDouble(1));
            }
        }
    }

    public void testGroupBy() throws Exception {
        Client client = getClient();

        for (String tbl : TABLES) {
            int pk = 0;
            pk = fillTable(client, tbl, pk);
            pk = fillTable(client, tbl, pk);
            fillTable(client, tbl, pk);

            VoltTable vt = client.callProcedure("@AdHoc",
                    "select poly, count(*) from " + tbl + " group by poly order by poly asc")
                            .getResults()[0];
            assertContentOfTable(new Object[][] {
                    {null, 3},
                    {BILLERICA_TRIANGLE_POLY, 3},
                    {BERMUDA_TRIANGLE_POLY, 3},
                    {LOWELL_SQUARE_POLY, 3},
                    {BERMUDA_TRIANGLE_HOLE_POLY, 3}},
                    vt);
        }
    }

    public void testUpdate() throws Exception {
        Client client = getClient();

        String santaCruzWkt = "POLYGON("
                + "(-122.061 36.999, "
                + "-122.058 36.950, "
                + "-121.974 36.955, "
                + "-122.061 36.999))";

        String southValleyWkt = "POLYGON("
                + "(-122.038 37.367, "
                + "-121.980 37.232, "
                + "-121.887 37.339, "
                + "-122.038 37.367))";


        for (String tbl : TABLES) {
            fillTable(client, tbl, 0);

            VoltTable vt = client.callProcedure("@AdHoc",
                    "update " + tbl + " set poly = ?, name = ? where pk = ?",
                    new GeographyValue(santaCruzWkt), "Santa Cruz Triangle", 0)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});

            vt = client.callProcedure("@AdHoc",
                    "update " + tbl + " set poly = polygonfromtext(?), name = ? where pk = ?",
                    southValleyWkt, "South Valley Triangle", 2)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {1});

            vt = client.callProcedure("@AdHoc",
                    "select * from " + tbl + " order by pk asc")
                    .getResults()[0];
            assertApproximateContentOfTable(new Object[][] {
                    {0, "Santa Cruz Triangle", new GeographyValue(santaCruzWkt)},
                    {1, "Bermuda Triangle with a hole", BERMUDA_TRIANGLE_HOLE_POLY},
                    {2, "South Valley Triangle", new GeographyValue(southValleyWkt)},
                    {3, "Lowell Square", LOWELL_SQUARE_POLY},
                    {4, "null poly", null}},
                    vt,
                    GEOGRAPHY_EPSILON);
        }
    }

    public void testNotNullConstraint() throws Exception {
        Client client = getClient();

        for (String tbl : NOT_NULL_TABLES) {
            verifyStmtFails(client, "insert into " + tbl + " (pk) values (0)",
                    "Column POLY has no default and is not nullable");

            verifyStmtFails(client, "insert into " + tbl + " values (0, 'foo', null)",
                    "Attempted violation of constraint");

            verifyStmtFails(client, "insert into " + tbl + " values (0, 'foo', null)",
                    "Attempted violation of constraint");

            validateTableOfScalarLongs(client,
                    "insert into " + tbl + " values (0, 'foo', polygonfromtext('" + BERMUDA_TRIANGLE_WKT + "'))",
                    new long[] {1});

            verifyStmtFails(client, "update " + tbl + " set poly = null where pk = 0",
                    "Attempted violation of constraint");
        }
    }

    public void testIn() throws Exception {
        Client client = getClient();

        for (String tbl : TABLES) {
            fillTable(client, tbl, 0);

            VoltTable vt = client.callProcedure("select_in_" + tbl,
                    (Object)(new GeographyValue[] {BERMUDA_TRIANGLE_POLY, null, LOWELL_SQUARE_POLY}))
                    .getResults()[0];
            assertContentOfTable(new Object[][] {{0}, {3}}, vt);

            try {
                client.callProcedure("select_in_" + tbl,
                    (Object)(new Object[] {BERMUDA_TRIANGLE_POLY, VoltType.NULL_GEOGRAPHY, LOWELL_SQUARE_POLY}));
                fail("Expected an exception to be thrown");
            }
            catch (RuntimeException rte) {
                // When ENG-9311 is fixed, then we shouldn't get this error and
                // the procedure call should succeed.
                assertTrue(rte.getMessage().contains("GeographyPointValue or GeographyValue instances "
                        + "are not yet supported in Object arrays passed as parameters"));
            }
        }
    }

    private String wktRoundTrip(Client client, String wkt) throws Exception {
        VoltTable vt = client.callProcedure("@AdHoc", "select polygonfromtext(?) from t", wkt)
                .getResults()[0];
        vt.advanceRow();
        return vt.getGeographyValue(0).toString();
    }

    public void testPolygonFromAdHocTextPositive() throws Exception {
        Client client = getClient();
        validateTableOfScalarLongs(client, "insert into t (pk) values (0)", new long[] {1});

        String expected = "POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305))";

        // Just a simple round trip with reasonable WKT.
        assertEquals(expected, wktRoundTrip(client, expected));

        // polygonfromtext should be case-insensitve.
        assertEquals(expected, wktRoundTrip(client,
                "Polygon((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305))"));
        assertEquals(expected, wktRoundTrip(client,
                "polygon((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305))"));
        assertEquals(expected, wktRoundTrip(client,
                "PoLyGoN((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305))"));

        assertEquals(expected, wktRoundTrip(client,
                "\n\nPOLYGON\n(\n(\n-64.751\n32.305\n,\n-80.437\n25.244\n,\n-66.371\n18.476\n,\n-64.751\n32.305\n)\n)\n"));
        assertEquals(expected, wktRoundTrip(client,
                "\t\tPOLYGON\t(\t(\t-64.751\t32.305\t,\t-80.437\t25.244\t,\t-66.371\t18.476\t,\t-64.751\t32.305\t)\t)\t"));
        assertEquals(expected, wktRoundTrip(client,
                "    POLYGON  (  (  -64.751  32.305  ,  -80.437  25.244  ,  -66.371  18.476  ,  -64.751  32.305  )  )  "));

        // Parsing with more than one loop should work the same.
        expected = "POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                         + "(-67.448 27.026, -67.448 25.968, -68.992 25.968, -68.992 27.026, -67.448 27.026))";

        assertEquals(expected, wktRoundTrip(client,
                "PoLyGoN\t(  (\n-64.751\n32.305   ,    -80.437\t25.244\n,-66.371 18.476,-64.751\t\t\t32.305   ),\t "
                        + "(\n-67.448\t27.026,\t-67.448\n\n25.968, -68.992     25.968, -68.992    27.026\n  , -67.448  \n27.026\t)\n)\t"));
    }

    private void assertGeographyValueWktParseError(Client client, String expectedMsg, String wkt) throws Exception {
        String stmt = "select polygonfromtext('" + wkt + "') from t";
        verifyStmtFails(client, stmt, expectedMsg);
    }

    // This is really misplaced.  But we don't have a regression
    // suite test for testing points.  We ought to, but we don't.
    private void checkOnePoint(Client client, long pk, String txt) throws Exception {
        final ClientResponse cr = client.callProcedure("@AdHoc",
                String.format("insert into location (pk, loc_point) values (%d, pointfromtext('%s'))", pk, txt));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testPointFromTextPositive() throws Exception {
        Client client = getClient();
        for (String tbl : TABLES) {
            fillTable(client, tbl, 1000);
        }
        checkOnePoint(client, 100, "point(0 90)");
        checkOnePoint(client, 101, "point(0 -90)");
        checkOnePoint(client, 102, "point(180 0)");
        checkOnePoint(client, 103, "point(-180 0)");
    }

    private void assertSelectGeographyPointValueWktParseError(Client client, String expectedMsg, String wkt) throws Exception {
        String stmt = "select pointfromtext('" + wkt + "') from t";
        verifyStmtFails(client, stmt, expectedMsg);
    }

    private void assertInsertGeographyPointValueWktParseError(Client client, String expectedMsg, String wkt, long pk) throws Exception {
        String stmt = String.format("insert into location (pk, loc_point) values (%d, pointfromtext('%s'));", pk, wkt);
        verifyStmtFails(client, stmt, expectedMsg);
    }

    public void testPointFromTextNegative() throws Exception {

        Client client = getClient();
        for (String tbl : TABLES) {
            fillTable(client, tbl, 0);
        }
        assertSelectGeographyPointValueWktParseError(client, "expected input of the form 'POINT\\(<lng> <lat>\\)", "point(20.0)");
        // Try a couple of bad latitudes.
        assertSelectGeographyPointValueWktParseError(client, "Latitude must be in the range", "point(10 100)");
        assertInsertGeographyPointValueWktParseError(client, "Latitude must be in the range", "point(10 100)", 100);
        assertSelectGeographyPointValueWktParseError(client, "Latitude must be in the range", "point(20 -100)");
        assertInsertGeographyPointValueWktParseError(client, "Latitude must be in the range", "point(20 -100)", 101);
        // Try a couple of bad longitudes.
        assertSelectGeographyPointValueWktParseError(client, "Longitude must be in the range", "point(200 20)");
        assertInsertGeographyPointValueWktParseError(client, "Longitude must be in the range", "point(200 20)", 200);
        assertSelectGeographyPointValueWktParseError(client, "Longitude must be in the range", "point(-200 20)");
        assertInsertGeographyPointValueWktParseError(client, "Longitude must be in the range", "point(-200 20)", 201);
    }

    public void testPolygonFromTextNegative() throws Exception {
        Client client = getClient();
        validateTableOfScalarLongs(client, "insert into t (pk) values (0)", new long[] {1});

        assertGeographyValueWktParseError(client,
                "does not start with POLYGON keyword", "NOT_A_POLYGON(...)");
        assertGeographyValueWktParseError(client,
                "missing left parenthesis after POLYGON", "POLYGON []");
        assertGeographyValueWktParseError(client,
                "expected left parenthesis to start a ring", "POLYGON ()");
        assertGeographyValueWktParseError(client,
                "A polygon ring must contain at least 4 points", "POLYGON (())");
        assertGeographyValueWktParseError(client,
                "expected left parenthesis to start a ring", "POLYGON(3 3, 4 4, 5 5, 3 3)");
        assertGeographyValueWktParseError(client,
                "expected a number but found ','", "POLYGON ((80 80, 60, 70 70, 90 90))");
        assertGeographyValueWktParseError(client,
                "unexpected token: '60'", "POLYGON ((80 80 60 60, 70 70, 90 90))");
        assertGeographyValueWktParseError(client,
                "unexpected end of input", "POLYGON ((80 80, 60 60, 70 70,");
        assertGeographyValueWktParseError(client,
                "expected a number but found '\\('", "POLYGON ((80 80, 60 60, 70 70, (30 15, 15 30, 15 45)))");
        assertGeographyValueWktParseError(client,
                "unexpected token: 'z'", "POLYGON ((80 80, 60 60, 70 70, 80 80)z)");
        assertGeographyValueWktParseError(client,
                "unrecognized input after WKT: 'blahblah'", "POLYGON ((80 80, 60 60, 70 70, 80 80))blahblah");
        assertGeographyValueWktParseError(client,
                "A polygon ring must contain at least 4 points", "POLYGON ((80 80, 60 60, 80 80))");
        assertGeographyValueWktParseError(client,
                "A polygon ring must contain at least 4 points", "POLYGON ((80 80, 60 60, 50 80, 80 80), ())");
        assertGeographyValueWktParseError(client,
                "A polygon ring's first vertex must be equal to its last vertex", "POLYGON ((80 80, 60 60, 70 70, 81 81))");

        // The Java WKT parser (in GeographyValue, which uses Java's StreamTokenizer) can handle coordinates
        // that are separated only by a minus sign indicating that the second coordinate is negative.
        // But boost's tokenizer (at least as its currently configured) will consider "32.305-64.571" as a single
        // token.  This seems like an acceptable discrepancy?
        assertGeographyValueWktParseError(client,
                "expected a number but found '32.305-64.751'", "POLYGON((32.305-64.751,25.244-80.437,18.476-66.371,32.305-64.751))");
        assertGeographyValueWktParseError(client,
                "Invalid input to POLYGONFROMTEXT: '200'.  Longitude must be in the range \\[-180,180\\]",  "POLYGON((0 0,  200 0,  200   45, 0   45, 0 0))");
        assertGeographyValueWktParseError(client,
                "Invalid input to POLYGONFROMTEXT: '100'.  Latitude must be in the range \\[-90,90\\]",     "POLYGON((0 0,   45 0,   45  100, 0  100, 0 0))");
        assertGeographyValueWktParseError(client,
                "Invalid input to POLYGONFROMTEXT: '-200'.  Longitude must be in the range \\[-180,180\\]", "POLYGON((0 0, -200 0, -200   45, 0   45, 0 0))");
        assertGeographyValueWktParseError(client,
                "Invalid input to POLYGONFROMTEXT: '-100'.  Latitude must be in the range \\[-90,90\\]",    "POLYGON((0 0,   45 0,   45 -100, 0 -100, 0 0))");
    }

    public void testGeographySize() throws Exception {
        Client client = getClient();

        // Make sure that we can resize a GEOGRAPHY column in a populated table if
        // we decide that we want to insert a polygon that is larger
        // than the column's current size.

        String wktFourVerts = "POLYGON ((1.0 1.0, -1.0 1.0, -1.0 -1.0, 1.0 -1.0, 1.0 1.0))";
        GeographyValue gv = GeographyValue.fromWKT(wktFourVerts);
        assertEquals(179, gv.getLengthInBytes());

        VoltTable vt = client.callProcedure("tiny_polygon.Insert", 0, gv).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        String wktFiveVerts = "POLYGON (("
                + "1.0 1.0, "
                + "-1.0 1.0, "
                + "-1.0 -1.0, "
                + "1.0 -1.0, "
                + "0.0 0.0, "
                + "1.0 1.0))";
        gv = GeographyValue.fromWKT(wktFiveVerts);
        assertEquals(203, gv.getLengthInBytes());
        verifyProcFails(client,
                "The size 203 of the value exceeds the size of the GEOGRAPHY column \\(179 bytes\\)",
                "tiny_polygon.Insert", 1, gv);

        ClientResponse cr = client.callProcedure("@AdHoc",
                "alter table tiny_polygon alter column poly geography(203);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("tiny_polygon.Insert", 1, gv).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        validateTableColumnOfScalarVarchar(client,
                "select asText(poly) from tiny_polygon order by id",
                new String[] {wktFourVerts, wktFiveVerts});

        // Restore catalog changes:
        cr = client.callProcedure("@AdHoc",
                "truncate table tiny_polygon;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                "alter table tiny_polygon alter column poly geography(179) not null;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestGeographyValueQueries.class);
        boolean success;

        final VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE T (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  POLY GEOGRAPHY(2048)\n"
                + ");\n"
                + "CREATE TABLE LOCATION (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  LOC_POINT GEOGRAPHY_POINT,\n"
                + ");\n"
                + "CREATE TABLE PT (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  POLY GEOGRAPHY\n"
                + ");\n"
                + "PARTITION TABLE PT ON COLUMN PK;\n"
                + "CREATE TABLE T_NOT_NULL (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  POLY GEOGRAPHY NOT NULL\n"
                + ");\n"
                + "CREATE TABLE PT_NOT_NULL (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  POLY GEOGRAPHY NOT NULL\n"
                + ");\n"
                + "CREATE TABLE TINY_POLYGON (\n"
                + "  ID INTEGER PRIMARY KEY,\n"
                + "  POLY GEOGRAPHY(179) NOT NULL\n"
                + ");\n"
                + "CREATE PROCEDURE select_in_t AS \n"
                + "  SELECT pk FROM t WHERE poly IN ? ORDER BY pk ASC;\n"
                + "CREATE PROCEDURE select_in_pt AS \n"
                + "  SELECT pk FROM pt WHERE poly IN ? ORDER BY pk ASC;\n"
                + "\n"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        project.setUseDDLSchema(true);
        config = new LocalCluster("geography-value-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        return builder;
    }

}
