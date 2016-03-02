/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.TestGeospatialFunctions.Border;
import org.voltdb.types.GeographyPointValue;
import org.voltdb_testprocs.regressionsuites.failureprocs.GeoPointProcsWithIncompatibleParameter;
import org.voltdb_testprocs.regressionsuites.failureprocs.GeographyProcsWithIncompatibleParameter;

// the negative test case that triggers assert in valgrind and debug environment are placed
// in this file

public class TestGeospatialFunctionsExtended extends RegressionSuite {

    public TestGeospatialFunctionsExtended(String name) {
        super(name);
    }

    static private void setupGeoSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE places (\n"
                + "  pk INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name VARCHAR(64),\n"
                + "  loc GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE TABLE borders (\n"
                + "  pk INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name VARCHAR(64),\n"
                + "  region GEOGRAPHY\n"
                + ");\n"
                + "\n"
                ;
        project.addProcedures(GeographyProcsWithIncompatibleParameter.class);
        project.addProcedures(GeoPointProcsWithIncompatibleParameter.class);
        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }

    private static void populatePlaces(Client client) throws NoConnectionsException, IOException, ProcCallException {
        client.callProcedure("places.Insert", 0, "Denver",
                GeographyPointValue.fromWKT("POINT(-104.959 39.704)"));
        client.callProcedure("places.Insert", 1, "Albuquerque",
                GeographyPointValue.fromWKT("POINT(-106.599 35.113)"));
        client.callProcedure("places.Insert", 2, "Cheyenne",
                GeographyPointValue.fromWKT("POINT(-104.813 41.134)"));
        client.callProcedure("places.Insert", 3, "Fort Collins",
                GeographyPointValue.fromWKT("POINT(-105.077 40.585)"));
        client.callProcedure("places.Insert", 4, "San Jose",
                GeographyPointValue.fromWKT("POINT(-121.903692 37.325464)"));
        client.callProcedure("places.Insert", 5, "Boston",
                GeographyPointValue.fromWKT("POINT(-71.069862 42.338100)"));

        // A null-valued point
        client.callProcedure("places.Insert", 99, "Neverwhere", null);
    }

    private static void populateBorders(Client client) throws NoConnectionsException, IOException, ProcCallException {
        for (Border border : TestGeospatialFunctions.borders) {
            client.callProcedure("borders.Insert",
                                  border.getPk(),
                                  border.getName(),
                                  border.getRegion());
        }
    }

    private static void populateTables(Client client) throws NoConnectionsException, IOException, ProcCallException {
        populatePlaces(client);
        populateBorders(client);
    }
    public void testPolygonIncompatibleTypes() throws NoConnectionsException, IOException, ProcCallException {
        if (isDebug() || isValgrind()) {
            // Can't run the tests in debug/valgrind environment as EE has
            // asserts at different places to validate types
            return;
        }

        Client client = getClient();
        populateTables(client);

        // Supply legal wkt or GeographyValue arg from each test entry. Supplied value will not effect the result except it has to be
        // legal wkt. The stored procedure's execution call uses hard-coded string wkt for parameterized Geography value for negative testing.
        for (GeographyProcsWithIncompatibleParameter.TestEntries entry : GeographyProcsWithIncompatibleParameter.TestEntries.values()) {
            verifyProcFails(client, entry.getFailureMsg(), "GeographyProcsWithIncompatibleParameter", entry.getParam());
        }

    }

    public void testPointIncompatibleTypes() throws NoConnectionsException, IOException, ProcCallException {
        if (isDebug() || isValgrind()) {
            // Can't run this tests in debug/valgrind environment as EE has
            // asserts at different places to validate types
            return;
        }

        Client client = getClient();
        populateTables(client);

        // Supply legal wkt or GeographyPointValue arg from each test entry. Supplied value will not effect the result except it has to be
        // legal wkt. The stored procedure's execution call uses hard-coded string wkt for parameterized point value for negative testing.
        for (GeoPointProcsWithIncompatibleParameter.TestEntries entry : GeoPointProcsWithIncompatibleParameter.TestEntries.values()) {
            verifyProcFails(client, entry.getFailureMsg(), "GeoPointProcsWithIncompatibleParameter", entry.getParam());
        }
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeospatialFunctionsExtended.class);
        boolean success = false;


        try {
            VoltProjectBuilder project = new VoltProjectBuilder();
            setupGeoSchema(project);
            config = new LocalCluster("geography-value-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            success = config.compile(project);
        }
        catch (IOException excp) {
            fail();
        }


        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

}
