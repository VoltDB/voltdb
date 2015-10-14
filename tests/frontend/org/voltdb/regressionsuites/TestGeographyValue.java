/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyValue;

public class TestGeographyValue extends RegressionSuite {

    public TestGeographyValue(String name) {
        super(name);
    }

    private final String BERMUDA_TRIANGLE_WKT = "POLYGON("
            + "(32.305 -64.751, "
            + "25.244 -80.437, "
            + "18.476 -66.371, "
            + "32.305 -64.751))";

    // The Bermuda Triangle with a hole inside
    private final String BERMUDA_TRIANGLE_HOLE_WKT = "POLYGON("
            + "(32.305 -64.751, "
            + "25.244 -80.437, "
            + "18.476 -66.371, "
            + "32.305 -64.751), "
            + "(28.066 -68.874, "
            + "25.361 -68.855, "
            + "28.376 -73.381,"
            + " 28.066 -68.874))";

    // (Useful for testing comparisons since it has the same number of vertices as
    // the Bermuda Triangle)
    private final String BILLERICA_TRIANGLE_WKT = "POLYGON("
            + "(42.571 -71.276, "
            + "42.547 -71.308, "
            + "42.533 -71.231, "
            + "42.571 -71.276))";

    // The dreaded "Lowell Square".  One loop,
    // five vertices (last is the same as the first)
    private final String LOWELL_SQUARE_WKT = "POLYGON("
            + "(42.641 -71.338, "
            + "42.619 -71.340, "
            + "42.617 -71.313, "
            + "42.639 -71.316, "
            + "42.641 -71.338))";

    private final GeographyValue BERMUDA_TRIANGLE_POLY = new GeographyValue(BERMUDA_TRIANGLE_WKT);
    private final GeographyValue BERMUDA_TRIANGLE_HOLE_POLY = new GeographyValue(BERMUDA_TRIANGLE_HOLE_WKT);
    private final GeographyValue BILLERICA_TRIANGLE_POLY = new GeographyValue(BILLERICA_TRIANGLE_WKT);
    private final GeographyValue LOWELL_SQUARE_POLY = new GeographyValue(LOWELL_SQUARE_WKT);

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

        vt = client.callProcedure(tbl + ".Insert", startPk, "null poly", null).getResults()[0];
        ++startPk;

        return startPk;
    }

    public void testNullValues() throws Exception {
        Client client = getClient();

        validateTableOfScalarLongs(client, "select * from t;", new long[] {});

        // Insert a null via default value
        validateTableOfScalarLongs(client, "insert into t (pk) values (0);", new long[] {1});

        VoltTable vt = client.callProcedure("@AdHoc", "select poly from t").getResults()[0];
        assertTrue(vt.toString().contains("NULL"));

        assertTrue(vt.advanceRow());
        GeographyValue gv = vt.getGeographyValue(0);
        assertTrue(vt.wasNull());
        assertEquals(null, gv);
        assertFalse(vt.advanceRow());

        // This produces a null geography since the function argument is null
        vt = client.callProcedure("@AdHoc", "select polygonfromtext(null) from t").getResults()[0];
        assertTrue(vt.advanceRow());
        gv = vt.getGeographyValue(0);
        assertTrue(vt.wasNull());
        assertEquals(null, gv);
        assertFalse(vt.advanceRow());

        // This tests the is null predicate for this type
        vt = client.callProcedure("@AdHoc", "select poly from t where poly is null").getResults()[0];
        assertTrue(vt.advanceRow());
        gv = vt.getGeographyValue(0);
        assertTrue(vt.wasNull());
        assertEquals(null, gv);
        assertFalse(vt.advanceRow());

        // Try inserting a SQL literal null, which takes a different code path.
        validateTableOfScalarLongs(client, "delete from t;", new long[] {1});
        validateTableOfScalarLongs(client, "insert into t values (0, 'boo', null);", new long[] {1});

        vt = client.callProcedure("@AdHoc", "select poly from t").getResults()[0];
        assertTrue(vt.advanceRow());
        gv = vt.getGeographyValue(0);
        assertTrue(vt.wasNull());
        assertEquals(null, gv);
        assertFalse(vt.advanceRow());

        // Insert a null by passing a null reference.
        validateTableOfScalarLongs(client, "delete from t;", new long[] {1});
        vt = client.callProcedure("t.Insert", 0, "null geog", null).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc", "select poly from t").getResults()[0];
        assertTrue(vt.advanceRow());
        gv = vt.getGeographyValue(0);
        assertTrue(vt.wasNull());
        assertEquals(null, gv);
        assertFalse(vt.advanceRow());
    }

    public void testInsertAndSimpleSelect() throws IOException, ProcCallException {
        Client client = getClient();
        String tables[] = {"pt", "t"};

        for (String tbl : tables) {
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

            vt = client.callProcedure("@AdHoc", "select polygonfromtext('" + BERMUDA_TRIANGLE_WKT + "') from " + tbl).getResults()[0];
            assertTrue(vt.advanceRow());
            assertEquals(BERMUDA_TRIANGLE_WKT, vt.getGeographyValue(0).toString());
            assertFalse(vt.advanceRow());
        }
    }

    public void testParams() throws IOException, ProcCallException {
        Client client = getClient();

        GeographyValue gv = new GeographyValue(BERMUDA_TRIANGLE_WKT);
        assertEquals(BERMUDA_TRIANGLE_WKT, gv.toString());

        VoltTable vt = client.callProcedure("t.Insert", 0, "Bermuda Triangle", gv).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc", "select * from t").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(0, vt.getLong(0));
        assertEquals("Bermuda Triangle", vt.getString(1));
        assertEquals(BERMUDA_TRIANGLE_WKT, vt.getGeographyValue(2).toString());
        assertFalse(vt.advanceRow());
    }

    public void testComparison() throws Exception {
        Client client = getClient();

        fillTable(client, "t", 0);

        // equals
        VoltTable vt = client.callProcedure("@AdHoc",
                "select pk, name, poly "
                + "from t as t1, t as t2 "
                + "where t1.poly = t2.poly "
                + "order by t1.pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {0, "Bermuda Triangle", BERMUDA_TRIANGLE_POLY},
                {1, "Bermuda Triangle with a hole", BERMUDA_TRIANGLE_HOLE_POLY},
                {2, "Billerica Triangle", BILLERICA_TRIANGLE_POLY},
                {3, "Lowell Square", LOWELL_SQUARE_POLY}},
                vt);

        // not equals
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t2.pk, t2.name "
                + "from t as t1, t as t2 "
                + "where t1.poly != t2.poly "
                + "order by t1.pk, t2.pk").getResults()[0];
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
                "select t1.pk, t1.name, t2.pk, t2.name "
                + "from t as t1, t as t2 "
                + "where t1.poly < t2.poly "
                + "order by t1.pk, t2.pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {0, "Bermuda Triangle" , 1, "Bermuda Triangle with a hole"},
                {0, "Bermuda Triangle", 2, "Billerica Triangle"},
                {0, "Bermuda Triangle", 3, "Lowell Square"},
                {2, "Billerica Triangle", 1, "Bermuda Triangle with a hole"},
                {2, "Billerica Triangle" , 3, "Lowell Square"},
                {3, "Lowell Square", 1, "Bermuda Triangle with a hole"}},
                vt);

        // less than or equal to
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t2.pk, t2.name "
                + "from t as t1, t as t2 "
                + "where t1.poly <= t2.poly "
                + "order by t1.pk, t2.pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {0, "Bermuda Triangle" , 0, "Bermuda Triangle"},
                {0, "Bermuda Triangle" , 1, "Bermuda Triangle with a hole"},
                {0, "Bermuda Triangle", 2, "Billerica Triangle"},
                {0, "Bermuda Triangle", 3, "Lowell Square"},
                {1, "Bermuda Triangle with a hole" , 1, "Bermuda Triangle with a hole"},
                {2, "Billerica Triangle", 1, "Bermuda Triangle with a hole"},
                {2, "Billerica Triangle", 2, "Billerica Triangle"},
                {2, "Billerica Triangle" , 3, "Lowell Square"},
                {3, "Lowell Square", 1, "Bermuda Triangle with a hole"},
                {3, "Lowell Square", 3, "Lowell Square"}},
                vt);

        // greater than
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t2.pk, t2.name "
                + "from t as t1, t as t2 "
                + "where t1.poly > t2.poly "
                + "order by t1.pk, t2.pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {1, "Bermuda Triangle with a hole", 0, "Bermuda Triangle"},
                {1, "Bermuda Triangle with a hole", 2, "Billerica Triangle"},
                {1, "Bermuda Triangle with a hole", 3, "Lowell Square"},
                {2, "Billerica Triangle",0 ,"Bermuda Triangle"},
                {3, "Lowell Square", 0, "Bermuda Triangle"},
                {3, "Lowell Square", 2, "Billerica Triangle"}},
                vt);

        // greater than or equal to
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t2.pk, t2.name "
                + "from t as t1, t as t2 "
                + "where t1.poly >= t2.poly "
                + "order by t1.pk, t2.pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {0, "Bermuda Triangle", 0, "Bermuda Triangle"},
                {1, "Bermuda Triangle with a hole", 0, "Bermuda Triangle"},
                {1, "Bermuda Triangle with a hole", 1, "Bermuda Triangle with a hole"},
                {1, "Bermuda Triangle with a hole", 2, "Billerica Triangle"},
                {1, "Bermuda Triangle with a hole", 3, "Lowell Square"},
                {2, "Billerica Triangle", 0 ,"Bermuda Triangle"},
                {2, "Billerica Triangle", 2, "Billerica Triangle"},
                {3, "Lowell Square", 0, "Bermuda Triangle"},
                {3, "Lowell Square", 2, "Billerica Triangle"},
                {3, "Lowell Square", 3, "Lowell Square"}},
                vt);

        // is null
        vt = client.callProcedure("@AdHoc",
                "select pk, name "
                + "from t "
                + "where poly is null "
                + "order by pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {4, "null poly"}},
                vt);

        // is not null
        vt = client.callProcedure("@AdHoc",
                "select pk, name "
                + "from t "
                + "where poly is not null "
                + "order by pk").getResults()[0];
        assertContentOfTable(new Object[][] {
                {0, "Bermuda Triangle"},
                {1, "Bermuda Triangle with a hole"},
                {2, "Billerica Triangle"},
                {3, "Lowell Square"}},
                vt);
    }

    public void testArithmetic() throws Exception {
        Client client = getClient();

        fillTable(client, "t", 0);

        verifyStmtFails(client, "select pk, poly + poly from t order by pk",
                "incompatible data type in conversion");

        verifyStmtFails(client, "select pk, poly + 1 from t order by pk",
                "incompatible data type in conversion");
    }

    public void testGroupBy() throws Exception {
        Client client = getClient();

        int pk = 0;
        pk = fillTable(client, "t", pk);
        pk = fillTable(client, "t", pk);
        pk = fillTable(client, "t", pk);

        VoltTable vt = client.callProcedure("@AdHoc",
                "select poly, count(*) "
                + "from t "
                + "group by poly "
                + "order by poly asc")
                .getResults()[0];
        assertContentOfTable(new Object[][] {
                {null, 3},
                {BERMUDA_TRIANGLE_POLY, 3},
                {BILLERICA_TRIANGLE_POLY, 3},
                {LOWELL_SQUARE_POLY, 3},
                {BERMUDA_TRIANGLE_HOLE_POLY, 3}},
                vt);
    }

    public void testUpdate() throws Exception {
        Client client = getClient();

        fillTable(client,"t", 0);

        String santaCruzWkt = "POLYGON("
                + "(36.999 -122.061, "
                + "36.950 -122.058, "
                + "36.955 -121.974, "
                + "36.999 -122.061))";

        String southValleyWkt = "POLYGON("
                + "(37.367 -122.038, "
                + "37.232 -121.980, "
                +" 37.339 -121.887, "
                + "37.367 -122.038))";

        VoltTable vt = client.callProcedure("@AdHoc",
                "update t set poly = ?, name = ? where pk = ?",
                new GeographyValue(santaCruzWkt), "Santa Cruz Triangle", 0)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc",
                "update t set poly = polygonfromtext(?), name = ? where pk = ?",
                southValleyWkt, "South Valley Triangle", 2)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        vt = client.callProcedure("@AdHoc",
                "select * from t order by pk asc")
                .getResults()[0];
        assertContentOfTable(new Object[][] {
                {0, "Santa Cruz Triangle", new GeographyValue(santaCruzWkt)},
                {1, "Bermuda Triangle with a hole", BERMUDA_TRIANGLE_HOLE_POLY},
                {2, "South Valley Triangle", new GeographyValue(southValleyWkt)},
                {3, "Lowell Square", LOWELL_SQUARE_POLY},
                {4, "null poly", null}},
                vt);
    }

    public void testNotNullConstraint() throws Exception {
        Client client = getClient();

        verifyStmtFails(client,
                "insert into t_not_null (pk) values (0)",
                "Column POLY has no default and is not nullable");

        verifyStmtFails(client,
                "insert into t_not_null values (0, 'foo', null)",
                "Attempted violation of constraint");

        verifyStmtFails(client,
                "insert into t_not_null values (0, 'foo', null)",
                "Attempted violation of constraint");

        validateTableOfScalarLongs(client,
                "insert into t_not_null values "
                + "(0, 'foo', polygonfromtext('" + BERMUDA_TRIANGLE_WKT + "'))",
                new long[] {1});

        verifyStmtFails(client,
                "update t_not_null set poly = null where pk = 0",
                "Attempted violation of constraint");
    }

    public void testIn() throws Exception {
        Client client = getClient();
        fillTable(client, "t", 0);

        VoltTable vt = client.callProcedure("select_in",
                (Object)(new GeographyValue[] {BERMUDA_TRIANGLE_POLY, null, LOWELL_SQUARE_POLY}))
                .getResults()[0];
        assertContentOfTable(new Object[][] {
                {0},
                {3}},
                vt);
    }

    private String wktRoundTrip(Client client, String wkt) throws Exception {
        VoltTable vt = client.callProcedure("@AdHoc", "select polygonfromtext(?) from t", wkt)
                .getResults()[0];
        vt.advanceRow();
        return vt.getGeographyValue(0).toString();
    }

    public void testPolygonFromTextPositive() throws Exception {
        Client client = getClient();
        validateTableOfScalarLongs(client, "insert into t (pk) values (0)", new long[] {1});

        String expected = "POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))";

        // Just a simple round trip with reasonable WKT.
        assertEquals(expected, wktRoundTrip(client, expected));

        // polygonfromtext should be case-insensitve.
        assertEquals(expected, wktRoundTrip(client,
                "Polygon((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))"));
        assertEquals(expected, wktRoundTrip(client,
                "polygon((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))"));
        assertEquals(expected, wktRoundTrip(client,
                "PoLyGoN((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))"));

        assertEquals(expected, wktRoundTrip(client,
                "\n\nPOLYGON\n(\n(\n32.305\n-64.751\n,\n25.244\n-80.437\n,\n18.476\n-66.371\n,\n32.305\n-64.751\n)\n)\n"));
        assertEquals(expected, wktRoundTrip(client,
                "\t\tPOLYGON\t(\t(\t32.305\t-64.751\t,\t25.244\t-80.437\t,\t18.476\t-66.371\t,\t32.305\t-64.751\t)\t)\t"));
        assertEquals(expected, wktRoundTrip(client,
                "    POLYGON  (  (  32.305  -64.751  ,  25.244  -80.437  ,  18.476  -66.371  ,  32.305  -64.751  )  )  "));

        // Parsing with more than one loop should work the same.
        expected = "POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                      + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))";
        assertEquals(expected, wktRoundTrip(client,
                "PoLyGoN\t(  (\n32.305\n-64.751   ,    25.244\t-80.437\n,18.476 -66.371,32.305\t\t\t-64.751   ),\t "
                        + "(\n28.066\t-68.874,\t25.361    -68.855\n,28.376      -73.381,28.066\n\n-68.874\t)\n)\t"));
    }

    private void assertWktParseError(Client client, String expectedMsg, String wkt) throws Exception {
        String stmt = "select polygonfromtext('" + wkt + "') from t";
        verifyStmtFails(client, stmt, expectedMsg);
    }

    public void testPolygonFromTextNegative() throws Exception {
        Client client = getClient();
        validateTableOfScalarLongs(client, "insert into t (pk) values (0)", new long[] {1});

        assertWktParseError(client, "does not start with POLYGON keyword", "NOT_A_POLYGON(...)");
        assertWktParseError(client, "missing left parenthesis after POLYGON", "POLYGON []");
        assertWktParseError(client, "expected left parenthesis to start a loop", "POLYGON(3 3, 4 4, 5 5, 3 3)");
        assertWktParseError(client, "expected a number but found ','", "POLYGON ((80 80, 60, 70 70, 90 90))");
        assertWktParseError(client, "unexpected token: '60'", "POLYGON ((80 80 60 60, 70 70, 90 90))");
        assertWktParseError(client, "unexpected end of input", "POLYGON ((80 80, 60 60, 70 70,");
        assertWktParseError(client, "expected a number but found '\\('", "POLYGON ((80 80, 60 60, 70 70, (30 15, 15 30, 15 45)))");
        assertWktParseError(client, "unexpected token: 'z'", "POLYGON ((80 80, 60 60, 70 70, 80 80)z)");
        assertWktParseError(client, "unrecognized input after WKT: 'blahblah'", "POLYGON ((80 80, 60 60, 70 70, 90 90))blahblah");

        // The Java WKT parser (in GeographyValue, which uses Java's StreamTokenizer) can handle coordinates
        // that are separated only by a minus sign indicating that the second coordinate is negative.
        // But boost's tokenizer (at least as its currently configured) will consider "32.305-64.571" as a single
        // token.  This seems like an acceptable discrepancy?
        assertWktParseError(client, "expected a number but found '32.305-64.751'", "POLYGON((32.305-64.751,25.244-80.437,18.476-66.371,32.305-64.751))");
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeographyValue.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE T (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  POLY GEOGRAPHY\n"
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
                + "CREATE PROCEDURE select_in AS \n"
                + "  SELECT pk FROM t WHERE poly IN ? ORDER BY pk ASC;\n"
                + "\n"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (Exception e) {
            fail();
        }

        config = new LocalCluster("geography-value-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

}
