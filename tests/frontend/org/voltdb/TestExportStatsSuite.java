/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.HashinatorLite;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportStatsSuite extends TestExportBaseSocketExport {
    private static final int KFACTOR = 0;
    private static int SKIP_MEMORY_CHECK = -1;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, KFACTOR+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }

    /**
     * Wait until table statistics reach expected values.
     * This method will time out with a test failure after 10 minutes.
     * @param client VoltDB client handle
     * @param tableName Name of the table whose statistics should be analyzed
     * @param mem1 Expected memory usage for partition 0, or -1 if this check should be skipped.
     * @param tuple1 Expected tuple count on partition 0
     * @param mem2 Expected memory usage for partition 1, or -1 if this check should be skipped.
     * @param tuple2 Expected tuple count on partition 1
     * @throws Exception if an assumption is violated or the test times out
     */
    private void checkForExpectedStats(Client client, String tableName, int mem1, int tuple1, int mem2, int tuple2) throws Exception {
        boolean passed = false;
        boolean zpassed, opassed;
        zpassed = opassed = false;

        VoltTable stats = null;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean passedThisTime = false;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            while (stats.advanceRow()) {
                if (stats.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                    if (stats.getLong("PARTITION_ID") == 0 && !zpassed) {
                        if (tuple1 == stats.getLong("TUPLE_COUNT")
                                && ((mem1 == SKIP_MEMORY_CHECK) || (mem1 == stats.getLong("TUPLE_ALLOCATED_MEMORY")))) {
                            zpassed = true;
                            System.out.println("Partition Zero passed.");
                        }
                    }
                    if (stats.getLong("PARTITION_ID") == 1 && !opassed) {
                        if (tuple2 == stats.getLong("TUPLE_COUNT")
                                && ((mem2 == SKIP_MEMORY_CHECK) || (mem2 == stats.getLong("TUPLE_ALLOCATED_MEMORY")))) {
                            opassed = true;
                            System.out.println("Partition One passed.");
                        }
                    }
                }
                if (zpassed && opassed) {
                    System.out.println("All Stats Passed.");
                    passedThisTime = true;
                    break;
                }
            }
            if (passedThisTime) {
                passed = true;
                break;
            }
            Thread.sleep(1000);
        }
        System.out.println("Passed is: " + passed);
        System.out.println(stats);
        assertTrue(passed);
    }

    /**
     * Places an ad-hoc SQL query with VoltDB and verifies that the request succeeded.
     * @param client VoltDB client
     * @param sql SQL command to pass to VoltDB
     * @throws Exception if client.callProcedure() fails.
     */
    private static void callAdHocExpectSuccess( Client client, String sql ) throws Exception {
        ClientResponse response = client.callProcedure("@AdHoc", sql);
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
    }

    /**
     * Verify that the TUPLE_COUNT table statistic reflects persistent tables,
     * but not streamed tables being consumed by an exporter.
     * @throws Exception upon test failure
     */
    public void testTupleCountStatistics() throws Exception {
        System.out.println("\n\nTESTING TUPLE COUNT STATISTICS\n\n\n");
        Client client = getFullyConnectedClient();
        startListener();

        callAdHocExpectSuccess(client,
                "CREATE TABLE tuple_count_persist ( foobar SMALLINT NOT NULL, PRIMARY KEY( foobar )); " +
                "PARTITION TABLE tuple_count_persist ON COLUMN foobar; " +
                "CREATE STREAM tuple_count_export PARTITION ON COLUMN foobar EXPORT TO TARGET tuple_count_export ( foobar SMALLINT NOT NULL );");
        callAdHocExpectSuccess(client,
                "INSERT INTO tuple_count_persist VALUES ( 2 ); " +
                "INSERT INTO tuple_count_export VALUES ( 2 );");
        waitForStreamedAllocatedMemoryZero(client);

        // Verify that table stats show both insertions.
        checkForExpectedStats(client, "tuple_count_persist", SKIP_MEMORY_CHECK, 0, SKIP_MEMORY_CHECK, 1);
        checkForExpectedStats(client, "tuple_count_export", SKIP_MEMORY_CHECK, 0, SKIP_MEMORY_CHECK, 1);

        // Memory statistics need to show the persistent table but not the export.
        // We can assume no other tables have tuples since any catalog update clears the stats.
        final VoltTable[] memoryResults = client.callProcedure("@Statistics", "memory", 0).getResults();
        assertEquals(1, memoryResults.length);
        assertEquals(true, memoryResults[0].advanceRow());
        assertEquals(1, memoryResults[0].getLong("TUPLECOUNT"));

        callAdHocExpectSuccess(client, "DROP TABLE tuple_count_persist; DROP STREAM tuple_count_export;");
    }

    //
    // Only notify the verifier of the first set of rows. Expect that the rows after will be truncated
    // when the snapshot is restored
    // @throws Exception
    //
    public void testExportSnapshotTruncatesExportData() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testExportSnapshotTruncatesExportData");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        for (int i = 0; i < 40; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            ClientResponse response = client.callProcedure("Insert", params);
            ClientResponse responseGrp = client.callProcedure("Insert", paramsGrp);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            assertEquals(responseGrp.getStatus(), ClientResponse.SUCCESS);
        }

        System.out.println("Inserting Done..");
        client.drain();
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        checkForExpectedStats(client, "NO_NULLS", 9, 24, 6, 16);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        checkForExpectedStats(client, "NO_NULLS", 9, 24, 6, 16);

        //Resume will put flg on onserver export to start consuming.
        startListener();
        waitForStreamedAllocatedMemoryZero(client);

        for (int i = 40; i < 50; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            ClientResponse response = client.callProcedure("Insert", params);
            ClientResponse responseGrp = client.callProcedure("Insert", paramsGrp);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
            assertEquals(responseGrp.getStatus(), ClientResponse.SUCCESS);
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        m_config.shutDown();
        if (isNewCli) {
            m_config.startUp(true);
        } else {
            m_config.startUp(false);
        }

        client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");
        client.drain();

        // must still be able to verify the export data.
        quiesceAndVerify(client, m_verifier);

        //Allocated memory should go to 0
        //If this is failing watch out for ENG-5708
        checkForExpectedStats(client, "NO_NULLS", 0, 24, 0, 16);
    }

    public TestExportStatsSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportStatsSuite.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));
        // needed for tuple count test
        project.setUseDDLSchema(true);

        wireupExportTableToSocketExport("ALLOW_NULLS");
        wireupExportTableToSocketExport("NO_NULLS");
        wireupExportTableToSocketExport("ALLOW_NULLS_GRP");
        wireupExportTableToSocketExport("NO_NULLS_GRP");
        wireupExportTableToSocketExport("tuple_count_export");

        project.addProcedures(PROCEDURES);

        // JNI, single server
        // Use the cluster only config. Multiple topologies with the extra catalog for the
        // Add drop tests is harder. Restrict to the single (complex) topology.
        //
        //        config = new LocalSingleProcessServer("export-ddl.jar", 2,
        //                                              BackendTarget.NATIVE_EE_JNI);
        //        config.compile(project);
        //        builder.addServerConfig(config);


        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 1, KFACTOR,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, false);

        return builder;
    }
}