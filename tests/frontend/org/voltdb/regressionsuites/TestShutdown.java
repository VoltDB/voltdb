/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import junit.framework.Test;
import junit.framework.TestCase;

import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.DeploymentBuilder;

public class TestShutdown extends RegressionSuite {
    private static final Class<? extends TestCase> TESTCASECLASS = TestShutdown.class;

    public TestShutdown(String name) {
        super(name);
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
        DeploymentBuilder db = new DeploymentBuilder(4, 5, 3);
        LocalCluster cluster = LocalCluster.configure(TESTCASECLASS.getSimpleName(), "", db);
        cluster.bypassInProcessServerThread();
        assertNotNull("LocalCluster failed to compile", cluster);
        return new MultiConfigSuiteBuilder(TESTCASECLASS, cluster);
    }
}
