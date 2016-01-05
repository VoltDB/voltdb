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
import org.voltdb.types.GeographyPointValue;

public class TestGeographyPointValue extends RegressionSuite {

    public TestGeographyPointValue(String name) {
        super(name);
    }

    private static final double EPSILON = 1.0e-12;
    private static final GeographyPointValue BEDFORD_PT = new GeographyPointValue(-71.2767, 42.4906);
    private static final GeographyPointValue SANTA_CLARA_PT = new GeographyPointValue(-121.9692, 37.3544);
    private static final GeographyPointValue LOWELL_PT = new GeographyPointValue(-71.3273, 42.6200);

    private int fillTable(Client client, int startPk) throws Exception {
        validateTableOfScalarLongs(client,
                "insert into t values "
                        + "(" + startPk + ", "
                        + "'Bedford', "
                        + "pointfromtext('" + BEDFORD_PT.toString() + "'));",
                new long[] {1});
        startPk++;

        // To exercise parameters, execute the insert with
        // a GeographyPointValue instance.
        VoltTable vt = client.callProcedure("t.Insert", startPk, "Santa Clara", SANTA_CLARA_PT)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        startPk++;
        validateTableOfScalarLongs(client,
                "insert into t values (" + startPk + ", 'Atlantis', null);",
                new long[] {1});
        startPk++;
        return startPk;
    }

    public void testInsertDefaultNull() throws IOException, ProcCallException {
        Client client = getClient();

        validateTableOfScalarLongs(client,
                "insert into t (pk) values (1);",
                new long[] {1});

        VoltTable vt = client.callProcedure("@AdHoc", "select pk, pt from t;").getResults()[0];
        String actual = vt.toString();
        String expectedPart = "NULL";
        assertTrue(actual + " does not contain " + expectedPart,
                actual.contains(expectedPart));

        assertTrue(vt.advanceRow());
        long id = vt.getLong(0);
        assertEquals(1, id);
        GeographyPointValue ptByIndex = vt.getPoint(1);
        assertTrue(vt.wasNull());
        assertNull(ptByIndex);

        GeographyPointValue ptByColumnName = vt.getPoint("pt");
        assertTrue(vt.wasNull());
        assertNull(ptByColumnName);

        assertFalse(vt.advanceRow());

        vt = client.callProcedure("@AdHoc", "select pt from t where pt is null;").getResults()[0];
        assertTrue(vt.advanceRow());
        ptByIndex = vt.getPoint(0);
        assert(vt.wasNull());
    }

    public void testPointFromText() throws Exception {
        final double EPSILON = 1.0e-14;
        Client client = getClient();

        validateTableOfScalarLongs(client,
                "insert into t (pk) values (1);",
                new long[] {1});

        VoltTable vt = client.callProcedure("@AdHoc",
                "select pointfromtext('point (-71.2767 42.4906)') from t;").getResults()[0];
        assertTrue(vt.advanceRow());
        GeographyPointValue pt = vt.getPoint(0);
        assertFalse(vt.wasNull());
        assertEquals(42.4906, pt.getLatitude(), EPSILON);
        assertEquals(-71.2767, pt.getLongitude(), EPSILON);
    }


    public void testPointComparison() throws Exception {
        Client client = getClient();

        fillTable(client, 0);

        // Self join to test EQ operator
        VoltTable vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t1.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt = t2.pt "
                + "order by t1.pk;").getResults()[0];

        assertApproximateContentOfTable(new Object[][] {
                {0, "Bedford", BEDFORD_PT},
                {1, "Santa Clara", SANTA_CLARA_PT}},
                vt, EPSILON);

        // Self join to test not equals operator
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.pt, t2.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt <> t2.pt "
                + "order by t1.pk, t1.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, BEDFORD_PT, SANTA_CLARA_PT},
                {1, SANTA_CLARA_PT, BEDFORD_PT}},
                vt, EPSILON);

        // Self join to test < operator
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.pt, t2.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt < t2.pt "
                + "order by t1.pk, t1.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {1, SANTA_CLARA_PT, BEDFORD_PT}},
                vt, EPSILON);

        // Self join to test <= operator
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.pt, t2.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt <= t2.pt "
                + "order by t1.pk, t1.pt, t2.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, BEDFORD_PT, BEDFORD_PT},
                {1, SANTA_CLARA_PT, SANTA_CLARA_PT},
                {1, SANTA_CLARA_PT, BEDFORD_PT}},
                vt, EPSILON);

        // Self join to test > operator
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.pt, t2.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt > t2.pt "
                + "order by t1.pk, t1.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, BEDFORD_PT, SANTA_CLARA_PT}},
                vt, EPSILON);

        // Self join to test >= operator
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.pt, t2.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt >= t2.pt "
                + "order by t1.pk, t1.pt, t2.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, BEDFORD_PT, SANTA_CLARA_PT},
                {0, BEDFORD_PT, BEDFORD_PT},
                {1, SANTA_CLARA_PT, SANTA_CLARA_PT}},
                vt, EPSILON);

        // Test IS NULL
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t1.pt "
                + "from t as t1 "
                + "where t1.pt is null "
                + "order by t1.pk, t1.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {2, "Atlantis", null}},
                vt, EPSILON);

        // Test IS NOT NULL
        vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t1.pt "
                + "from t as t1 "
                + "where t1.pt is not null "
                + "order by t1.pk, t1.pt;").getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, "Bedford", BEDFORD_PT},
                {1, "Santa Clara", SANTA_CLARA_PT}},
                vt, EPSILON);
    }

    public void testPointGroupBy() throws Exception {
        Client client = getClient();

        int pk = 0;
        pk = fillTable(client, pk);
        pk = fillTable(client, pk);
        pk = fillTable(client, pk);

        VoltTable vt = client.callProcedure("@AdHoc",
                "select pt, count(*) "
                + "from t "
                + "group by pt "
                + "order by pt asc")
                .getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
            {null, 3},
            {SANTA_CLARA_PT, 3},
            {BEDFORD_PT, 3}},
            vt, EPSILON);
    }

    public void testPointUpdate() throws Exception {
        Client client = getClient();

        fillTable(client, 0);

        final GeographyPointValue CAMBRIDGE_PT = new GeographyPointValue(-71.1106, 42.3736);
        final GeographyPointValue SAN_JOSE_PT = new GeographyPointValue(-121.8906, 37.3362);

        validateTableOfScalarLongs(client,
                "update t set "
                + "name = 'Cambridge', "
                + "pt = pointfromtext('" + CAMBRIDGE_PT + "') "
                + "where pk = 0",
                new long[] {1});

        validateTableOfScalarLongs(client,
                "update t set "
                + "name = 'San Jose', "
                + "pt = pointfromtext('" + SAN_JOSE_PT + "') "
                + "where pk = 1",
                new long[] {1});

        VoltTable vt = client.callProcedure("@AdHoc",
                "select pk, name, pt from t order by pk")
                .getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, "Cambridge", CAMBRIDGE_PT},
                {1, "San Jose", SAN_JOSE_PT},
                {2, "Atlantis", null}},
                vt, EPSILON);
    }

    public void testPointArithmetic() throws Exception {
        Client client = getClient();

        fillTable(client, 0);

        verifyStmtFails(client,
                "select pk, pt + pt from t order by pk",
                "incompatible data type in conversion");

        verifyStmtFails(client,
                "select pk, pt + 1 from t order by pk",
                "incompatible data type in conversion");
    }

    public void testPointNotNull() throws Exception {
        Client client = getClient();

        verifyStmtFails(client,
                "insert into t_not_null (pk, name) values (0, 'Westchester')",
                "Column PT has no default and is not nullable");

        verifyStmtFails(client,
                "insert into t_not_null (pk, name, pt) values (0, 'Westchester', null)",
                "CONSTRAINT VIOLATION");

        validateTableOfScalarLongs(client,
                "insert into t_not_null (pk, name, pt) values (0, 'Singapore', pointfromtext('point(103.8521 1.2905)'))",
                new long[] {1});

        VoltTable vt = client.callProcedure("@AdHoc",
                "select pk, name, pt from t_not_null order by pk")
                .getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {0, "Singapore", new GeographyPointValue(103.8521, 1.2905)}},
                vt, EPSILON);
    }

    public void testPointIn() throws Exception {
        Client client = getClient();

        fillTable(client, 0);

        Object listParam = new GeographyPointValue[] {SANTA_CLARA_PT, null, LOWELL_PT};
        VoltTable vt = client.callProcedure("sel_in", listParam)
                .getResults()[0];

        assertApproximateContentOfTable (new Object[][] {
                {1, SANTA_CLARA_PT}},
                vt, EPSILON);

        try {
            vt = client.callProcedure("sel_in",
                    (Object)(new Object[] {SANTA_CLARA_PT, null, LOWELL_PT}))
                    .getResults()[0];
            fail("Expected an exception to be thrown");
        }
        catch (RuntimeException rte) {
            // When ENG-9311 is fixed, then we shouldn't get this error and
            // the procedure call should succeed.
            assertTrue(rte.getMessage().contains("GeographyPointValue or GeographyValue instances "
                    + "are not yet supported in Object arrays passed as parameters"));
        }
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeographyPointValue.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE T (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  PT GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE TABLE T_NOT_NULL (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  PT GEOGRAPHY_POINT NOT NULL\n"
                + ");\n"
                + "\n"
                + "CREATE PROCEDURE sel_in AS"
                + "  SELECT pk, pt \n"
                + "  FROM t \n"
                + "  WHERE pt IN ?;\n"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (Exception e) {
            fail();
        }

        config = new LocalCluster("point-type-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
