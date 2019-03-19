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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.VoltFile;

public class TestMigrateExport extends TestExportBaseSocketExport {

    public TestMigrateExport(String s) {
        super(s);
    }

    private static final int k_factor = 1;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }


    public void testMigrateExport() throws Exception
    {
        System.out.println("testMigrateExport");
        final Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        for (int i=0; i < 1000; i++) {
            client.callProcedure("@AdHoc", "INSERT INTO NIBBLE_EXPORT VALUES(" + i + ", CURRENT_TIMESTAMP(), 'xx', 'yy');");
        }
        Thread.sleep(30000);
        VoltTable vt = client.callProcedure("@Statistics", "EXPORT").getResults()[0];
        System.out.println(vt.toFormattedString());
        vt = client.callProcedure("@AdHoc", "select count(*) from NIBBLE_EXPORT").getResults()[0];
    }

    static public junit.framework.Test suite() throws Exception {
        StringBuilder schema = new StringBuilder();
        schema.append("CREATE table NIBBLE_EXPORT (" +
                "PKEY          INTEGER          NOT NULL," +
                " A_TIMESTAMP   TIMESTAMP        NOT NULL," +
                " A_INLINE_S1   VARCHAR(4)       NOT NULL," +
                " A_INLINE_S2   VARCHAR(63)      NOT NULL," +
                ") using TTL 5 seconds on column A_TIMESTAMP MIGRATE to TARGET NIBBLE_EXPORT;" +
                " \nPARTITION table NIBBLE_EXPORT on column PKEY;" +
                " \nCREATE INDEX MINDEX1 ON NIBBLE_EXPORT(A_TIMESTAMP);" +
                " \nCREATE MIGRATING INDEX MINDEX2 ON NIBBLE_EXPORT();");
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestMigrateExport.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addLiteralSchema(schema.toString());
        wireupExportTableToSocketExport("NIBBLE_EXPORT");

        LocalCluster config = new LocalCluster("testNibbleExport.jar", 10, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, false);

        return builder;
    }

}
