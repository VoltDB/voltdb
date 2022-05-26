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

package org.voltdb.export;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientUtils;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableMap;

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
        ExportLocalClusterBase.resetDir();
        super.setUp();

        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, KFACTOR+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeSocketExporterClientAndServer();
    }

    /**
     * Wait until table statistics reach expected values.
     * This method will time out with a test failure after 10 minutes.
     * @param client VoltDB client handle
     * @param streamName Name of the stream whose statistics should be analyzed
     * @param mem1 Expected memory usage for partition 0, or -1 if this check should be skipped.
     * @param tuple1 Expected tuple count on partition 0
     * @param mem2 Expected memory usage for partition 1, or -1 if this check should be skipped.
     * @param tuple2 Expected tuple count on partition 1
     * @throws Exception if an assumption is violated or the test times out
     */
    private void checkForExpectedStats(Client client, String streamName, int mem1, int tuple1, int mem2, int tuple2, boolean isPersistent) throws Exception {
        boolean passed = false;
        boolean zpassed, opassed;
        zpassed = opassed = false;

        VoltTable stats = null;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long maxWait = TimeUnit.MINUTES.toMillis(10);
        long end = System.currentTimeMillis() + maxWait;
        while (true) {
            stats = client.callProcedure("@Statistics", isPersistent ? "table" : "export", 0).getResults()[0];
            boolean passedThisTime = false;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > maxWait) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            while (stats.advanceRow()) {
                String memoryColumnName = isPersistent ? "TUPLE_ALLOCATED_MEMORY" : "TUPLE_PENDING";
                String tableColumnName = isPersistent ? "TABLE_NAME": "SOURCE";
                if (stats.getString(tableColumnName).equalsIgnoreCase(streamName)) {
                    if (stats.getLong("PARTITION_ID") == 0 && !zpassed) {
                        if (tuple1 == stats.getLong("TUPLE_COUNT")
                                && ((mem1 == SKIP_MEMORY_CHECK) || (mem1 == stats.getLong(memoryColumnName)))) {
                            zpassed = true;
                            System.out.println("Partition Zero passed.");
                        }
                        System.out.println("tuple1:"+tuple1+" TUPLE_COUNT:"+stats.getLong("TUPLE_COUNT")+" mem1:"+mem1+" "+memoryColumnName+":"+stats.getLong(memoryColumnName));
                    }
                    if (stats.getLong("PARTITION_ID") == 1 && !opassed) {
                        if (tuple2 == stats.getLong("TUPLE_COUNT")
                                && ((mem2 == SKIP_MEMORY_CHECK) || (mem2 == stats.getLong(memoryColumnName)))) {
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
        waitForExportRowsToBeDelivered(client, ImmutableMap.of("S_NO_NULLS", 0L, "TUPLE_COUNT_EXPORT", 0L));

        // Verify that table stats show both insertions.
        checkForExpectedStats(client, "tuple_count_persist", SKIP_MEMORY_CHECK, 0, SKIP_MEMORY_CHECK, 1, true);
        checkForExpectedStats(client, "tuple_count_export", SKIP_MEMORY_CHECK, 0, SKIP_MEMORY_CHECK, 1, false);

        // Memory statistics need to show the persistent table but not the export.
        // We can assume no other tables have tuples since any catalog update clears the stats.
        final VoltTable[] memoryResults = client.callProcedure("@Statistics", "memory", 0).getResults();
        assertEquals(1, memoryResults.length);
        assertEquals(true, memoryResults[0].advanceRow());
        assertEquals(1, memoryResults[0].getLong("TUPLECOUNT"));

        callAdHocExpectSuccess(client, "DROP TABLE tuple_count_persist; DROP STREAM tuple_count_export;");
    }

    public void testStatisticsWithCatlogAndDeploymentUpdate() throws Exception {
        System.out.println("\n\nTESTING testStatisticsWithCatlogAndDeploymentUpdate STATISTICS\n\n\n");
        Client client = getFullyConnectedClient();

        callAdHocExpectSuccess(client,
                "CREATE STREAM tuple_count_export PARTITION ON COLUMN foobar EXPORT TO TARGET tuple_count_export ( foobar SMALLINT NOT NULL );");

        VoltTable stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        while (stats.advanceRow()) {
            if ("TUPLE_COUNT_EXPORT".equalsIgnoreCase(stats.getString("SOURCE"))) {
                assert("ACTIVE".equalsIgnoreCase(stats.getString("STATUS")));
            }
        }

        // drop stream
        callAdHocExpectSuccess(client, "DROP STREAM tuple_count_export;");
        stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        while (stats.advanceRow()) {
            if ("TUPLE_COUNT_EXPORT".equalsIgnoreCase(stats.getString("SOURCE"))) {
                assert("DROPPED".equalsIgnoreCase(stats.getString("STATUS")));
            }
        }

        // recreate stream
        callAdHocExpectSuccess(client,
                "CREATE STREAM tuple_count_export PARTITION ON COLUMN foobar EXPORT TO TARGET tuple_count_export ( foobar SMALLINT NOT NULL );");
        stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        while (stats.advanceRow()) {
            if ("TUPLE_COUNT_EXPORT".equalsIgnoreCase(stats.getString("SOURCE"))) {
                assert("ACTIVE".equalsIgnoreCase(stats.getString("STATUS")));
            }
        }

        // update deployment, no TUPLE_COUNT_EXPORT connector
        String deploymentURL = Configuration.getPathToCatalogForTest("stats_no_connector.xml");
        String depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)), Constants.UTF8ENCODING);
        client.callProcedure("@UpdateApplicationCatalog", null, depBytes);
        stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        while (stats.advanceRow()) {
            if ("TUPLE_COUNT_EXPORT".equalsIgnoreCase(stats.getString("SOURCE"))) {
                assert("ACTIVE".equalsIgnoreCase(stats.getString("STATUS")));
            }
        }

        // add connector back
        deploymentURL = Configuration.getPathToCatalogForTest("stats_full.xml");
        depBytes = new String(ClientUtils.fileToBytes(new File(deploymentURL)), Constants.UTF8ENCODING);
        client.callProcedure("@UpdateApplicationCatalog", null, depBytes);
        stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        while (stats.advanceRow()) {
            if ("TUPLE_COUNT_EXPORT".equalsIgnoreCase(stats.getString("SOURCE"))) {
                assert("ACTIVE".equalsIgnoreCase(stats.getString("STATUS")));
            }
        }
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
        String targetStream = "S_NO_NULLS";

        for (int i = 0; i < 40; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, targetStream, i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams(targetStream, i, rowdata);
            ClientResponse response = client.callProcedure("ExportInsertNoNulls", params);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }

        System.out.println("Inserting Done..");
        client.drain();
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        checkForExpectedStats(client, targetStream, 24, 24, 16, 16, false);

        client.callProcedure("@SnapshotSave", "/tmp/" + System.getProperty("user.name"), "testnonce", (byte) 1);
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        checkForExpectedStats(client, targetStream, 24, 24, 16, 16, false);

        //Resume will put flg on onserver export to start consuming.
        startListener();
        m_verifier.waitForTuples(client);

        for (int i = 40; i < 50; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, targetStream, i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams(targetStream, i, rowdata);
            ClientResponse response = client.callProcedure("ExportInsertNoNulls", params);
            assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        }
        m_verifier.waitForTuples(client);
        m_config.shutDown();

        // Restore the snapshot
        m_config.startUp(false);
        client = getClient();
        client.callProcedure("@SnapshotRestore", "/tmp/" + System.getProperty("user.name"), "testnonce");

        // must still be able to verify the export data.
        m_verifier.verifyRows();

        //Allocated memory should go to 0
        //If this is failing watch out for ENG-5708
        //SnapshotRestore resets export sequence number
        checkForExpectedStats(client, targetStream, 0, 0, 0, 0, false);
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
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));
        // needed for tuple count test
        project.setUseDDLSchema(true);

        wireupExportTableToSocketExport("S_NO_NULLS");
        wireupExportTableToSocketExport("tuple_count_export");
        project.addProcedures(NONULLS_PROCEDURES);

        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 1, KFACTOR,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("stats_full.xml"));


        // A catalog change that enables snapshots
        config = new LocalCluster("export-ddl-cluster-rep2.jar",  2, 1, KFACTOR,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));
        // needed for tuple count test
        project.setUseDDLSchema(true);
        compile = config.compile(project);
        assertTrue(compile);
        MiscUtils.copyFile(project.getPathToDeployment(), Configuration.getPathToCatalogForTest("stats_no_connector.xml"));
        return builder;
    }
}
