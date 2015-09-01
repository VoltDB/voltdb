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
import org.voltdb.types.PointType;

public class TestPointType extends RegressionSuite {

    public TestPointType(String name) {
        super(name);
        // TODO Auto-generated constructor stub
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
        PointType ptByIndex = vt.getPoint(1);
        assertTrue(vt.wasNull());
        assertTrue(ptByIndex.isNull());

        PointType ptByColumnName = vt.getPoint("pt");
        assertTrue(vt.wasNull());
        assertTrue(ptByColumnName.isNull());

        assertFalse(vt.advanceRow());

        vt = client.callProcedure("@AdHoc", "select pt from t where pt is null;").getResults()[0];
        assertTrue(vt.advanceRow());
        ptByIndex = vt.getPoint(0);
        assert(vt.wasNull());
    }

    public void testPointFromText() throws Exception {
        Client client = getClient();

        validateTableOfScalarLongs(client,
                "insert into t (pk) values (1);",
                new long[] {1});

        VoltTable vt = client.callProcedure("@AdHoc",
                "select pointfromtext('point (42.4906 -71.2767)') from t;").getResults()[0];
        assertTrue(vt.advanceRow());
        PointType pt = vt.getPoint(0);
        assertFalse(vt.wasNull());
        assertEquals(42.4906, pt.getLatitude(), 0.001);
        assertEquals(-71.2767, pt.getLongitude(), 0.001);
    }

    private int fillTable(Client client, int startPk) throws Exception {
        validateTableOfScalarLongs(client,
                "insert into t values (" + startPk + ", 'Bedford', pointfromtext('point (42.4906 -71.2767)'));",
                new long[] {1});
        startPk++;
        validateTableOfScalarLongs(client,
                "insert into t values (" + startPk + ", 'Santa Clara', pointfromtext('point (37.3544 -121.9692)'));",
                new long[] {1});
        startPk++;
        validateTableOfScalarLongs(client,
                "insert into t values (" + startPk + ", 'Atlantis', null);",
                new long[] {1});
        startPk++;
        return startPk;
    }

    public void testPointEquality() throws Exception {
        Client client = getClient();

        fillTable(client, 0);

        // Self join to test EQ operator
        VoltTable vt = client.callProcedure("@AdHoc",
                "select t1.pk, t1.name, t1.pt "
                + "from t as t1, t as t2 "
                + "where t1.pt = t2.pt "
                + "order by pk;").getResults()[0];

        assertTrue(vt.advanceRow());
        assertEquals(0, vt.getLong(0));
        assertEquals("Bedford", vt.getString(1));
        PointType pt = vt.getPoint(2);
        assertEquals(42.4906, pt.getLatitude(), 0.001);
        assertEquals(-71.2767, pt.getLongitude(), 0.001);

        assertTrue(vt.advanceRow());
        assertEquals(1, vt.getLong(0));
        assertEquals("Santa Clara", vt.getString(1));
        pt = vt.getPoint(2);
        assertEquals(37.3544, pt.getLatitude(), 0.001);
        assertEquals(-121.9692, pt.getLongitude(), 0.001);

        assertFalse(vt.advanceRow());
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

        PointType expectedPoints[] = {
          null,
          new PointType(37.3544, -121.9692),
          new PointType(42.4906, -71.2767)
        };

        int i = 0;
        while (vt.advanceRow()) {
            PointType actualPoint = vt.getPoint(0);
            if (expectedPoints[i] == null) {
                assertTrue(vt.wasNull());
            }
            else {
                assertEquals(expectedPoints[i].getLatitude(), actualPoint.getLatitude(), 0.001);
                assertEquals(expectedPoints[i].getLongitude(), actualPoint.getLongitude(), 0.001);
            }

            assertEquals(3, vt.getLong(1));
            ++i;
        }
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestPointType.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE T (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  NAME VARCHAR(32),\n"
                + "  PT POINT\n"
                + ");\n"
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
