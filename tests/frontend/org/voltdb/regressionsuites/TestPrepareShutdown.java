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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.voltdb.BackendTarget;
import org.voltdb.TestCSVFormatterSuiteBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.VoltFile;

public class TestPrepareShutdown extends RegressionSuite
{
    public TestPrepareShutdown(String name) {
        super(name);
    }

    public void testPrepareShutdown() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        final Client client2 = this.getClient();
        for (int i = 0; i < 50; i++) {
            client2.callProcedure(new Callback(), "ArbitraryDurationProc", 6000);
        }

        //push import data async
        String[] myData = new String[5000];
        for (int i =0; i < 5000; i++) {
            myData[i] = i + ",1,2,3,4,5,6,7,8,9,10";
        }
        TestCSVFormatterSuiteBase.pushDataAsync(7001, myData);

        final Client client = getAdminClient();
        ClientResponse resp = client.callProcedure("@PrepareShutdown");
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);

        //test sys proc that is not allowed.
        try {
            client2.callProcedure("@SystemInformation", "OVERVIEW");
            fail("Unallowed sys proc is executed.");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("@SystemInformation:" + e.getMessage());
            assertTrue("Server shutdown in progress - new transactions are not processed.".equals(e.getMessage()));
        }

        //test query that is not allowed
        try {
            client2.callProcedure("ArbitraryDurationProc", 0);
            fail("Unallowed proc is executed.");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("ArbitraryDurationProc:" + e.getMessage());
            assertTrue("Server shutdown in progress - new transactions are not processed.".equals(e.getMessage()));
        }
        long sum = Long.MAX_VALUE;
        while (sum > 0) {
            resp = client2.callProcedure("@Statistics", "liveclients", 0);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable t = resp.getResults()[0];
            long trxn=0, bytes=0, msg=0;
            if (t.advanceRow()) {
                trxn = t.getLong(6);
                bytes = t.getLong(7);
                msg = t.getLong(8);
                sum =  trxn + bytes + msg;
            }
            System.out.printf("Outstanding transactions: %d, buffer bytes :%d, response messages:%d\n", trxn, bytes, msg);
            Thread.sleep(2000);
        }
        assertTrue (sum == 0);

        //check import OUTSTANDING_REQUESTS
       sum = Long.MAX_VALUE;
        while (sum > 0) {
            resp = client2.callProcedure("@Statistics", "IMPORTER", 0);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable t = resp.getResults()[0];
            if (t.advanceRow()) {
                sum = t.getLong(8);
            }
            System.out.printf("Outstanding importer transactions: %d\n", sum);
            Thread.sleep(500);
        }
        assertTrue (sum == 0);
        try{
            client.callProcedure("@Shutdown");
            fail("@Shutdown fails via admin mode");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("@Shutdown: cluster has been shutdown via admin mode ");
        }
    }

    static public junit.framework.Test suite() throws Exception {

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPrepareShutdown.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();

        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        additionalEnv.put("voltdbbundlelocation", bundleLocation);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(ArbitraryDurationProc.class.getResource("clientfeatures.sql"));
        project.addProcedures(ArbitraryDurationProc.class);
        project.setUseDDLSchema(true);
        project.addPartitionInfo("indexme", "pkey");

        // configure socket importer 1
        Properties props = buildProperties(
                "port", "7001",
                "decode", "true",
                "procedure", "indexme.insert");

        Properties formatConfig = buildProperties(
                "nullstring", "test",
                "separator", ",",
                "blank", "empty",
                "escape", "\\",
                "quotechar", "\"",
                "nowhitespace", "true");

        project.addImport(true, "custom", "csv", "socketstream.jar", props, formatConfig);

        LocalCluster config = new LocalCluster("prepare_shutdown_importer.jar", 4, 1, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compileWithAdminMode(project, VoltDB.DEFAULT_ADMIN_PORT, false);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }

    class Callback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            assertTrue(clientResponse.getStatus() == ClientResponse.SUCCESS);
        }
    }
}
