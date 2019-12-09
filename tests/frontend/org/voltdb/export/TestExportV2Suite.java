/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.FlakyTestRule;
import org.voltdb.SQLStmt;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.MiscUtils;

public class TestExportV2Suite extends TestExportBaseSocketExport {
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

    private static final int k_factor = 1;

    static final String SCHEMA =
            "CREATE TABLE kv (" +
                    "key bigint not null, " +
                    "nondetval bigint not null, " +
                    "PRIMARY KEY(key)" +
                    "); " +
                    "PARTITION TABLE kv ON COLUMN key;" +
                    "CREATE INDEX idx_kv ON kv(nondetval);";

    public static class NonDeterministicProc extends VoltProcedure {
        public static final SQLStmt sql = new SQLStmt("insert into kv \nvalues ?, ?");
        public VoltTable run(long key) {
            long id = VoltDB.instance().getHostMessenger().getHostId();
            voltQueueSQL(sql, key, id);
            voltExecuteSQL();
            return new VoltTable(new ColumnInfo("", VoltType.BIGINT));
        }
    }

    private static  final String PROCEDURE =
            "CREATE PROCEDURE FROM CLASS org.voltdb.export.TestExportV2Suite$NonDeterministicProc;" +
            "PARTITION PROCEDURE TestExportV2Suite$NonDeterministicProc ON TABLE kv COLUMN key;";
    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        ExportLocalClusterBase.resetDir();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeSocketExporterClientAndServer();
    }

    @Test
    public void testExportMultiTable() throws Exception
    {
        System.out.println("testExportMultiTable");
        final Client client = getClient();
        m_streamNames.addAll(Arrays.asList("S_ALLOW_NULLS", "S_NO_NULLS"));
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        for (int i = 0; i < 100; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client,
                    "S_ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("S_ALLOW_NULLS", i, rowdata);
            client.callProcedure("ExportInsertAllowNulls", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(client,
                    "S_NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("S_NO_NULLS", i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        // Trigger hash mismatch
        if (MiscUtils.isPro()) {
            client.callProcedure("TestExportV2Suite$NonDeterministicProc", 100);
            verifyTopologyAfterHashMismatch(client);
            VoltTable vt = client.callProcedure("@Statistics", "export", 0).getResults()[0];

            // All replicas are gone
            while (vt.advanceRow()) {
                assert("TRUE".equals(vt.getString("ACTIVE")));
            }
        }

        for (int i= 100 ; i < 200; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client,
                    "S_ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("S_ALLOW_NULLS", i, rowdata);
            client.callProcedure("ExportInsertAllowNulls", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(client,
                    "S_NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("S_NO_NULLS", i, rowdata);
            client.callProcedure("ExportInsertNoNulls", params);
        }
        quiesceAndVerifyTarget(client, m_streamNames, m_verifier, DEFAULT_DELAY_MS, true);
    }

    @Test
    public void testExportControlParams() throws Exception {
        System.out.println("testExportControlParams");
        final Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        String[] targets = {"S_ALLOW_NULLS"};
        ClientResponse r = client.callProcedure("@ExportControl", "S_ALLOW_NULLS", targets, "release");
        assert(r.getStatus() == ClientResponse.SUCCESS);
        assert(r.getResults()[0].getRowCount() == 0);
    }

    public TestExportV2Suite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        LocalCluster config;

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(3));

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportV2Suite.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-allownulls-ddl-with-target.sql"));

        wireupExportTableToSocketExport("S_ALLOW_NULLS");
        wireupExportTableToSocketExport("S_NO_NULLS");
        project.addProcedures(ALLOWNULLS_PROCEDURES);
        project.addProcedures(NONULLS_PROCEDURES);

        //non-determinisic proc
        project.addLiteralSchema(SCHEMA);
        project.addLiteralSchema(PROCEDURE);
        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 12, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
    private void verifyTopologyAfterHashMismatch(Client client) {
        //allow time to get the stats
        final long maxSleep = TimeUnit.MINUTES.toMillis(5);
        boolean done = false;
        long start = System.currentTimeMillis();
        while (!done) {
            boolean inprogress = false;
            try {
                Thread.sleep(5000);
                VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
                while (vt.advanceRow()) {
                    long pid = vt.getLong(0);
                    if (pid != MpInitiator.MP_INIT_PID) {
                        String replicasStr = vt.getString(1);
                        String[] replicas = replicasStr.split(",");
                        if (replicas.length != 1) {
                            inprogress = true;
                            break;
                        }
                    }
                }
                if (!inprogress) {
                   return;
                }

                if (maxSleep < (System.currentTimeMillis() - start)) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        assert(done);
    }

}
