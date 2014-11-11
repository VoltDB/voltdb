/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestQueryTimeout extends RegressionSuite {

    private void loadData(Client client) throws IOException, ProcCallException {
        int scale = 5000;

        for (int i = 0; i < scale; i++) {
            client.callProcedure("VOTES.insert", i, "MA", i % 6);
        }
        System.out.println("Finish loading " + scale + " rows");
    }

    public void testReadWriteProcTimeout() throws IOException, ProcCallException, InterruptedException {
        System.out.println("test read/write proc timeout disabled...");

        Client client = this.getClient();
        loadData(client);

        VoltTable vt = client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
        System.out.println(vt);

        checkDeploymentPropertyValue(client, "querytimeout", "2000");

        try {
            client.callProcedure("LongRunningReadOnlyProc");
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains("A SQL query was terminated after 2.00 seconds"));
        }

        // 'LongRunningReadWriteProc' has the same Read query as 'LongRunningReadOnlyProc'
        // but it should not be timed out.
        try {
            client.callProcedure("LongRunningReadWriteProc");
        } catch(Exception ex) {
            fail("Write procedure should not be timed out");
        }
    }

    //
    // Suite builder boilerplate
    //

    public TestQueryTimeout(String name) {
        super(name);
    }
    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.querytimeout.LongRunningReadOnlyProc.class,
        org.voltdb_testprocs.regressionsuites.querytimeout.LongRunningReadWriteProc.class
    };

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestQueryTimeout.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE VOTES ( " +
                "phone_number INTEGER NOT NULL, " +
                "state VARCHAR(2) NOT NULL, " +
                "contestant_number INTEGER NOT NULL);" +

                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        project.addProcedures(PROCEDURES);

        project.setQueryTimeout(2000);
        boolean success;

        config = new LocalCluster("querytimeout-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("querytimeout-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
