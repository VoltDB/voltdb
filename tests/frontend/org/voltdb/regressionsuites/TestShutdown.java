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

import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestShutdown extends RegressionSuite
{
    public TestShutdown(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("");
        return builder;
    }

    public void testShutdown() throws Exception {
        final Client client = getClient();
        // sleep a little so that we have time for the IPC backend to actually be running
        // so it can screw us on empty results
        Thread.sleep(1000);

        boolean lostConnect = false;
        try {
            client.callProcedure("@Shutdown").getResults();
        }
        catch (ProcCallException pce) {
            lostConnect = pce.getMessage().contains("was lost before a response was received");
        }
        assertTrue(lostConnect);
        while (!((LocalCluster)getServerConfig()).areAllNonLocalProcessesDead()) {
            Thread.sleep(500);
        }
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestShutdown.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        LocalCluster config = new LocalCluster("decimal-default.jar", 4, 5, 3, BackendTarget.NATIVE_EE_JNI);
        config.setHasLocalServer(false);
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
