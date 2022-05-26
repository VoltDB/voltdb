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


import org.voltdb.BackendTarget;
import org.voltdb.SnapshotInitiationInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures with no param
 */
public class TestSaveSnapshotAtDefaultLocation extends RegressionSuite {

    static LocalCluster m_config;
    static final int SITES_PER_HOST = 8;
    static final int HOSTS = 3;
    static final int K = MiscUtils.isPro() ? 1 : 0;

    public TestSaveSnapshotAtDefaultLocation(String name) {
        super(name);
    }

    //
    // Also does some basic smoke tests
    // of @SnapshotScan and @SnapshotDelete
    //
    public void testSnapshotSaveForBackup() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSnapshotSaveForBackup");
        Client client = getClient();

        ClientResponse cr = client.callProcedure("@AdHoc", "INSERT INTO foo values (1)");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        cr = client.callProcedure("@SnapshotSave");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        VoltTable scanResults = null;
        final long endTime = System.currentTimeMillis() + 120*1000;
        //Now scan and make sure we have magic marker in snapshot. Since its non-blocking wait for 2 min.
        while (System.currentTimeMillis() < endTime) {
            scanResults = client.callProcedure("@SnapshotScan", VoltDB.instance().getSnapshotPath()).getResults()[0];
            if (scanResults.getRowCount() != 0) break;
            Thread.sleep(5000);
        }
        assertNotNull(scanResults);
        System.out.println(scanResults.toFormattedString());
        assertEquals(1, scanResults.getRowCount());
        assertEquals( 10, scanResults.getColumnCount());
        scanResults.advanceRow();
        //Make sure nonce has MAGIC in it.
        assertTrue(scanResults.getString("NONCE").startsWith(SnapshotInitiationInfo.MAGIC_NONCE_PREFIX));

        client.close();
    }


    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public junit.framework.Test suite() throws Exception {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSaveSnapshotAtDefaultLocation.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("CREATE TABLE foo (bar BIGINT NOT NULL);");
        project.addPartitionInfo("foo", "bar");
        // get a server config for the native backend with one sites/partitions
        m_config = new LocalCluster("base-cluster-with-inprocess.jar", SITES_PER_HOST, HOSTS, K, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        boolean basecompile = m_config.compile(project);
        assertTrue(basecompile);
        builder.addServerConfig(m_config);
        return builder;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
}
