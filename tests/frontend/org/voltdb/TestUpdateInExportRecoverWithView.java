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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

/**
 * Export to nowhere and build view. Rejoin nodes and verify view is intact.
 * @author akhanzode
 */
public class TestUpdateInExportRecoverWithView extends JUnit4LocalClusterTest {

    @Test
    public void testUpdateInExportViewWithRecover() throws Exception {
        System.out.println("testUpdateInExportViewWithRecover");

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);

        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.NoOpExporter");
        VoltProjectBuilder project = new VoltProjectBuilder();
        if (MiscUtils.isPro()) {
            project.configureLogging(null, null, false, true, 200, 20000, 300);
        }
        project.setUseDDLSchema(true);
        Properties props = new Properties();
        project.addExport(true, ServerExportEnum.CUSTOM, props);
        LocalCluster db = new LocalCluster("exportview.jar", 2, 1, 0, 2, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        boolean compile = db.compile(project);
        assertTrue(compile);
        if (db.isValgrind()) {
            return;
        }

        db.setHasLocalServer(false);
        db.startUp();

        ClientConfig clientConfig = new ClientConfig();
        Client client = ClientFactory.createClient(clientConfig);
        String addr = db.getListenerAddresses().get(0);
        System.out.println("Connecting to server at: " + addr);
        client.createConnection(addr);
        Client adminClient = ClientFactory.createClient(clientConfig);
        String adminAddr = db.getAdminAddress(0);
        adminClient.createConnection(adminAddr);

        ClientResponse response = client.callProcedure("@AdHoc", "CREATE TABLE foo (PKEY INTEGER NOT NULL, VAL INTEGER)");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.callProcedure("@AdHoc", "CREATE STREAM EXPORT_WITH_VIEW PARTITION ON COLUMN PKEY (PKEY INTEGER NOT NULL, VAL INTEGER)");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.callProcedure("@AdHoc", "CREATE VIEW V_EXPORT_WITH_VIEW (PKEY, KEYCOUNT) AS\n" +
                                        "SELECT PKEY, COUNT(*) FROM EXPORT_WITH_VIEW GROUP BY PKEY");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.callProcedure("@AdHoc", "insert into EXPORT_WITH_VIEW values(0,0)");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        client.drain();
        response = client.callProcedure("@AdHoc", "select KEYCOUNT from V_EXPORT_WITH_VIEW");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].asScalarLong(), 1);
        response = client.callProcedure("@AdHoc", "update V_EXPORT_WITH_VIEW set KEYCOUNT = 10");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("@AdHoc", "select KEYCOUNT from V_EXPORT_WITH_VIEW");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].asScalarLong(), 10);
        Thread.sleep(500);
        if (MiscUtils.isPro()) {
            db.shutdown(adminClient);
            db.waitForNodesToShutdown();
        } else {
            client.close();
            db.shutdownSave(adminClient);
            db.waitForNodesToShutdown();
        }
        db.startUp(false);
        Thread.sleep(500);
        client = ClientFactory.createClient(clientConfig);
        client.createConnection(addr);
        response = client.callProcedure("@AdHoc", "select KEYCOUNT from V_EXPORT_WITH_VIEW");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(response.getResults()[0].asScalarLong(), 10);
        db.shutDown();
    }

    public TestUpdateInExportRecoverWithView() {
    }
}
