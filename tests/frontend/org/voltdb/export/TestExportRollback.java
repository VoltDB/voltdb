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

import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportRollback extends TestExportBaseSocketExport {
    private static final int k_factor = 1;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        ExportLocalClusterBase.resetDir();
        super.setUp();

        callbackSucceded = true;
        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeSocketExporterClientAndServer();
        assertTrue(callbackSucceded);
    }

    private boolean callbackSucceded = true;
    class RollbackCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() != ClientResponse.USER_ABORT) {
                callbackSucceded = false;
                System.err.println(clientResponse.getStatusString());
            }
        }
    }

    //
    // Sends many tuples to an Export enabled VoltServer and verifies the receipt
    // of each in the Export stream. Some procedures rollback (after a real insert).
    // Tests that streams are correct in the face of rollback.
    //
    public void testExportRollback() throws Exception {
        System.out.println("testExportRollback");
        final Client client = getClient();
        String targetStream = "S_NO_NULLS";

        final double rollbackPerc = 0.15;
        long seed = 0;
        System.out.println("TestExportRollback seed " + seed);
        java.util.Random r = new java.util.Random(seed);

        // exportxxx: should pick more random data
        final Object[] rowdata = TestSQLTypesSuite.m_midValues;

        // roughly 10k rows is a full buffer it seems
        for (int pkey = 0; pkey < 40000; pkey++) {
            if ((pkey % 1000) == 0) {
                System.out.println("Rollback test added " + pkey + " rows");
            }
            final Object[] params = convertValsToParams(targetStream, pkey, rowdata);
            double random = r.nextDouble();
            if (random <= rollbackPerc) {
                // note - do not update the el verifier as this rollsback
                boolean done = false;
                do {
                    if (done != true) {
                        done = client.callProcedure(new RollbackCallback(), "ExportRollbackInsertNoNulls", params);
                    }
                    if (done == false) {
                        client.backpressureBarrier();
                    }
                } while (!done);
            }
            else {
                m_verifier.addRow(client,
                        targetStream, pkey, convertValsToRow(pkey, 'I', rowdata));
                boolean done = false;
                do {
                    if (done != true) {
                        done = client.callProcedure(new NullCallback(), "ExportInsertNoNulls", params);
                    }
                    if (done == false) {
                        client.backpressureBarrier();
                    }
                } while (!done);
            }
        }
        System.out.println("Draining client....");
        client.drain();
        System.out.println("Client Drained....");

        m_verifier.waitForTuplesAndVerify(client);
    }

    public TestExportRollback(final String name) {
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
            new MultiConfigSuiteBuilder(TestExportRollback.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));

        wireupExportTableToSocketExport("S_NO_NULLS");

        project.addProcedures(NONULLS_PROCEDURES);

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);

        return builder;
    }
}
