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

package org.voltdb.regressionsuites;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.InvocationDispatcher;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestShutdownSave extends RegressionSuite
{
    public TestShutdownSave(String name) {
        super(name);
    }

    public void testShutdownSave() throws Exception {
        if (!MiscUtils.isPro()) return;
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        Client client2 = this.getClient();
        for (int i = 0; i < 256; ++i) {
            client2.callProcedure("KV.INSERT", i);
        }

        final Client client = getAdminClient();
        ClientResponse resp = client.callProcedure("@PrepareShutdown");
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
        long sigil = resp.getResults()[0].asScalarLong();

        //test sys proc that is not allowed.
        try {
            client2.callProcedure("@SystemInformation", "OVERVIEW");
            fail("Unallowed sys proc is executed.");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("@SystemInformation:" + e.getMessage());
            assertEquals("incorrect status from @SystemInformation",
                    InvocationDispatcher.SHUTDOWN_MSG, e.getMessage());
        }

        //test query that is not allowed
        try {
            client2.callProcedure("ArbitraryDurationProc", 0);
            fail("Unallowed proc is executed.");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("ArbitraryDurationProc:" + e.getMessage());
            assertEquals("incorrect status from ArbitraryDurationProc",
                    InvocationDispatcher.SHUTDOWN_MSG, e.getMessage());
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

        try{
            client.callProcedure("@Shutdown", sigil);
            fail("@Shutdown fails via admin mode");
        } catch (ProcCallException e) {
            if (!e.getMessage().contains("Connection to database host")) {
                throw e;
            }
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("@Shutdown: cluster has been shutdown via admin mode ");
        }

        LocalCluster cluster = (LocalCluster)m_config;
        cluster.waitForNodesToShutdown();

        for (int i = 0; i < HOST_COUNT; ++i) {
            File snapDH = getSnapshotPathForHost(cluster, i);

            File terminusFH = new File(snapDH.getParentFile(), VoltDB.TERMINUS_MARKER);
            assertTrue("("+ i +") terminus file " + terminusFH + " is not accessible",
                    terminusFH.exists() && terminusFH.isFile() && terminusFH.canRead());
            String nonce;
            try (BufferedReader br = new BufferedReader(new FileReader(terminusFH))) {
                nonce = br.readLine();
            }
            assertTrue("(" + i + ") no nonce written to terminus file " + terminusFH,
                    nonce != null && !nonce.trim().isEmpty());

            File [] finished = snapDH.listFiles(new FileFilter() {
                @Override
                public boolean accept(File f) {
                    return    f.isFile()
                           && f.getName().startsWith(nonce)
                           && f.getName().endsWith(".finished");
                }
            });
            assertTrue("(" + i + ") snapshot did not finish " + Arrays.asList(finished),
                    finished.length == 1 && finished[0].exists() && finished[0].isFile());
        }
        m_config.startUp(false);
        client2 = this.getClient();

        ClientResponse cr = client2.callProcedure("@Statistics", "PROCEDUREPROFILE", 0);
        assertEquals("statistics invocation failed: " + cr.getStatusString() , ClientResponse.SUCCESS, cr.getStatus());
        VoltTable t = cr.getResults()[0];
        while (t.advanceRow()) {
            assertNotSame("detected replayed transactions", "KV.insert", t.getString(1));
        }

        cr = client2.callProcedure("@AdHoc", "SELECT COUNT(*) FROM KV;");
        assertEquals("count(*) invocation failed: " + cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        assertEquals("content did not restore", 256L, cr.getResults()[0].asScalarLong());

        m_config.shutDown();

        for (int i = 0; i < HOST_COUNT; ++i) {
            File snapDH = getSnapshotPathForHost(cluster, i);

            File terminusFH = new File(snapDH.getParentFile(), VoltDB.TERMINUS_MARKER);
            assertFalse("("+ i +") terminus file " + terminusFH + " is accessible",
                    terminusFH.exists() && terminusFH.isFile() && terminusFH.canRead());
        }
    }

    static File getSnapshotPathForHost(LocalCluster cluster, int hostId) {
        List<File> subRoots = cluster.getSubRoots();
        return new File (subRoots.get(hostId), "/voltdbroot/snapshots");
    }
    static int HOST_COUNT = 3;

    static public junit.framework.Test suite() throws Exception {

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestShutdownSave.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();

        // String bundleLocation = System.getProperty("user.dir") + "/bundles";
        // additionalEnv.put("voltdbbundlelocation", bundleLocation);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(ArbitraryDurationProc.class.getResource("clientfeatures.sql"));
        project.addProcedure(ArbitraryDurationProc.class);
        project.setUseDDLSchema(true);
        project.addPartitionInfo("indexme", "pkey");
        if (MiscUtils.isPro()) {
            project.configureLogging(true, true, 2, 2, 64);
        }

        LocalCluster config = new LocalCluster("prepare_shutdown_importer.jar", 4, HOST_COUNT, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compileWithAdminMode(project, -1, false);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
