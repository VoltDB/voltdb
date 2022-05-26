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

package org.voltdb;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportLocalClusterBase;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportConstraintsSuite extends TestExportBaseSocketExport {
    private static final int k_factor = 0;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        ExportLocalClusterBase.resetDir();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
        m_verifier.m_verifySequenceNumber = false;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeSocketExporterClientAndServer();
    }

    public void testExportConstraints() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testExportConstraints");
        Client client = getClient();
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info");
        }

        ClientResponse response;
        response = client.callProcedure("@AdHoc", "create stream inotnull partition on column i (i integer not null)");
        response = client.callProcedure("@AdHoc", "create stream varcharlimit partition on column i (i varchar(2) not null)");
        boolean thrown = false;
        try {
            response = client.callProcedure("@AdHoc", "insert into inotnull values(null)");
        } catch (ProcCallException pce) {
            assertTrue(pce.getClientResponse().getStatusString().contains("CONSTRAINT VIOLATION"));
            thrown = true;
        }
        assertTrue(thrown);

        thrown = false;
        try {
            response = client.callProcedure("@AdHoc", "insert into inotnull values(6.5)");
        } catch (ProcCallException pce) {
            thrown = true;
        }
        assertTrue(thrown);

        thrown = false;
        try {
            response = client.callProcedure("@AdHoc", "insert into varcharlimit values(NULL)");
        } catch (ProcCallException pce) {
            assertTrue(pce.getClientResponse().getStatusString().contains("CONSTRAINT VIOLATION"));
            thrown = true;
        }
        assertTrue(thrown);

        thrown = false;
        try {
            response = client.callProcedure("@AdHoc", "insert into varcharlimit values('123')");
        } catch (ProcCallException pce) {
            thrown = true;
        }
        assertTrue(thrown);

        client.close();
        client = getClient();
        //Make sure server is still hanging in there.
        assertNotNull(client);
    }

    public TestExportConstraintsSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportConstraintsSuite.class);

        project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-constraints.jar", 4, 1, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }
}
