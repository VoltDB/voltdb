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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.PointType;

public class TestGeospatialFunctions extends RegressionSuite {

    public TestGeospatialFunctions(String name) {
        super(name);
    }

    private static void populateTables(Client client) throws Exception {
        client.callProcedure("places.Insert", 0, "Denver",
                PointType.pointFromText("POINT(39.704 -104.959)"));
        client.callProcedure("places.Insert", 1, "Albuquerque",
                PointType.pointFromText("POINT(35.113 -106.599)"));
        client.callProcedure("places.Insert", 2, "Cheyenne",
                PointType.pointFromText("POINT(41.134 -104.813)"));
        client.callProcedure("places.Insert", 3, "Fort Collins",
                PointType.pointFromText("POINT(40.585 -105.077)"));

        // A null-valued point
        client.callProcedure("places.Insert", 4, "Neverwhere", null);

        client.callProcedure("borders.Insert", 0, "Colorado",
                new GeographyValue("POLYGON(("
                        + "41.002 -102.052, "
                        + "41.002 -109.045,"
                        + "36.999 -109.045,"
                        + "36.999 -102.052,"
                        + "41.002 -102.052))"));
        client.callProcedure("borders.Insert", 1, "Wyoming",
                new GeographyValue("POLYGON(("
                        + "44.978 -104.061, "
                        + "44.978 -111.046, "
                        + "40.998 -111.046, "
                        + "40.998 -104.061, "
                        + "44.978 -104.061))"));

        // This polygon should not contain Denver, due to the hole.
        client.callProcedure("borders.Insert", 2, "Colorado with a hole around Denver",
                new GeographyValue("POLYGON("
                        + "(41.002 -102.052, "
                        + "41.002 -109.045,"
                        + "36.999 -109.045,"
                        + "36.999 -102.052,"
                        + "41.002 -102.052), "
                        + "(40.240 -104.035, "
                        + "40.240 -105.714, "
                        + "39.188 -105.714, "
                        + "39.188 -104.035,"
                        + "40.240 -104.035))"));

        // a null-valued-polygon
        client.callProcedure("borders.Insert", 3, "Wonderland", null);
    }

    public void testContains() throws Exception {
        Client client = getClient();

        populateTables(client);

        VoltTable vt = client.callProcedure("@AdHoc",
                "select places.name || ', ' || borders.name "
                + "from places, borders "
                + "where contains(borders.region, places.loc) "
                + "order by places.pk, borders.pk").getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Denver, Colorado"},
                {"Cheyenne, Wyoming"},
                {"Fort Collins, Colorado"},
                {"Fort Collins, Colorado with a hole around Denver"},
                }, vt);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeospatialFunctions.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE places (\n"
                + "  pk INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name VARCHAR(64),\n"
                + "  loc POINT\n"
                + ");\n"
                + "CREATE TABLE borders (\n"
                + "  pk INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name VARCHAR(64),\n"
                + "  region GEOGRAPHY\n"
                + ");\n"
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
