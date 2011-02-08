/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.nio.channels.SocketChannel;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAdminMode extends RegressionSuite
{
    public TestAdminMode(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));");
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);", "T.A1: 0");
        builder.addStmtProcedure("CountA", "SELECT COUNT(*) FROM T");
        return builder;
    }

    // Check that we can start in admin mode, access the DB only from the admin
    // port, then switch out of admin mode and access the DB from both ports,
    // then back in again
    public void testBasicAdminFunction() throws Exception {
        final Client client = ClientFactory.createClient();
        final Client adminclient = ClientFactory.createClient();

        try {
            client.createConnection("localhost");
            adminclient.createConnection("localhost", 32323);

            // Try to use the normal port and verify that the server reports
            // that it is unavailable (and that nothing happened via the admin port)
            boolean admin_start = false;
            try
            {
                client.callProcedure("InsertA", 0, 1000);
            }
            catch (ProcCallException e)
            {
                assertEquals("Server did not report itself as unavailable on production port",
                             ClientResponse.SERVER_UNAVAILABLE, e.getClientResponse().getStatus());
                admin_start = true;
            }
            assertTrue("Server did not report itself as unavailable on production port",
                       admin_start);
            VoltTable[] results = adminclient.callProcedure("CountA").getResults();
            assertEquals(0, results[0].asScalarLong());

            // verify that the admin port works for general use in admin mode
            // add several tuples
            for (int i=0; i < 100; i++) {
                adminclient.callProcedure("InsertA", i, 1000+i);
            }
            adminclient.drain();
            results = adminclient.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());

            // exit admin mode and get busy from both ports
            adminclient.callProcedure("@AdminModeExit");
            results = client.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());
            results = adminclient.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());

            // verify admin mode sysprocs not available on production port
            boolean admin_failed = false;
            try
            {
                client.callProcedure("@AdminModeEnter");
            }
            catch (ProcCallException e)
            {
                admin_failed = true;
                assertTrue("Server returned an unexpected error",
                           e.getClientResponse().getStatusString().
                           contains("is not available to this client"));
            }
            assertTrue("Server allowed admin mode sysproc on production port",
                       admin_failed);

            admin_failed = false;
            try
            {
                client.callProcedure("@AdminModeExit");
            }
            catch (ProcCallException e)
            {
                admin_failed = true;
                assertTrue("Server returned an unexpected error",
                           e.getClientResponse().getStatusString().
                           contains("is not available to this client"));
            }
            assertTrue("Server allowed admin mode sysproc on production port",
                       admin_failed);

            // turn admin mode back on.
            adminclient.callProcedure("@AdminModeEnter");

            // XXX-ADMIN add polling here although it shouldn't matter for
            // this synchronous, slow access.  We'll add another test for
            // clearing the backlog.

            // Try to use the normal port and verify that the server reports
            // that it is unavailable (and that nothing happened via the admin port)
            boolean admin_reentered = false;
            try
            {
                client.callProcedure("InsertA", 0, 1000);
            }
            catch (ProcCallException e)
            {
                assertEquals("Server did not report itself as unavailable on production port",
                             ClientResponse.SERVER_UNAVAILABLE, e.getClientResponse().getStatus());
                admin_reentered = true;
            }
            assertTrue("Server did not report itself as unavailable on production port",
                       admin_reentered);
            results = adminclient.callProcedure("CountA").getResults();
            assertEquals(100, results[0].asScalarLong());
        }
        finally {
            adminclient.close();
            client.close();
        }
    }

    // Somewhat hacky test of the LIVECLIENTS @Statistics selector
    public void testBacklogAndPolling() throws Exception
    {
        final Client adminclient = ClientFactory.createClient();
        SocketChannel channel = getClientChannel();

        try {
            adminclient.createConnection("localhost", 32323);
            VoltTable[] results = adminclient.callProcedure("@Statistics",
                                                            "LIVECLIENTS",
                                                            0).getResults();
            System.out.println(results[0].toString());
            results = adminclient.callProcedure("@AdminModeExit").getResults();
            System.out.println(results[0].toString());
            // queue up a bunch of invocations but don't read the responses
            for (int i = 0; i < 10000; i++)
            {
                ConnectionUtil.sendInvocation(channel, "InsertA", i, 10000 + i);
            }
            results = adminclient.callProcedure("@Statistics",
                                                "LIVECLIENTS",
                                                0).getResults();
            System.out.println(results[0].toString());
            assertEquals(2, results[0].getRowCount());
            results[0].advanceRow();
            // After queuing 10000 invocations and not reading any,
            // we should have some combination of outstanding work lingering around
            assertTrue((results[0].getLong("OUTSTANDING_RESPONSE_MESSAGES") +
                        results[0].getLong("OUTSTANDING_TRANSACTIONS")) > 0);
            Thread.sleep(6000);
            channel.close();
            // make sure there's only one connection in the list
            results = adminclient.callProcedure("@Statistics",
                                                "LIVECLIENTS",
                                                0).getResults();
            // Izzy told me to comment out the two below assertions
            //assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            //assertEquals(1, results[0].getLong("ADMIN"));
            System.out.println(results[0].toString());
        }
        finally {
            channel.close();
            adminclient.close();
        }
    }

    static class Callback implements ProcedureCallback {
        boolean m_stats = false;
        public Callback(boolean stats)
        {
            m_stats = stats;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse)
                throws Exception {

            if (clientResponse.getStatus() != ClientResponse.SUCCESS ||
                clientResponse.getStatus() != ClientResponse.SERVER_UNAVAILABLE)
            {
                //System.err.println(clientResponse.getStatusString());
                return;
            }
            if (m_stats)
            {
                //System.out.println(clientResponse.getResults()[0].toString());
            }
        }
    }

//    I wanted a test that would hammer the various actions but it runs long
//    enough that it's not really suited for a test suite.  Leaving it here
//    for now.
//    public void testSomething() throws Exception
//    {
//        final Client adminclient = ClientFactory.createClient();
//        final Client client = ClientFactory.createClient();
//        boolean production = true;
//        try {
//            adminclient.createConnection("localhost", 32323);
//            client.createConnection("localhost");
//            VoltTable[] results = adminclient.callProcedure("@Statistics",
//                                                            "LIVECLIENTS",
//                                                            0).getResults();
//            System.out.println(results[0].toString());
//            results = adminclient.callProcedure("@AdminModeExit").getResults();
//            System.out.println(results[0].toString());
//            // queue up a bunch of invocations but don't read the responses
//            for (int i = 0; i < 20000000; i++)
//            {
//                client.callProcedure(new Callback(false), "InsertA", i, i+10000);
//                if (i % 200 == 0)
//                {
//                    adminclient.callProcedure(new Callback(true), "@Statistics",
//                                              "LIVECLIENTS", 0);
//                }
//                if (i % 3001 == 0)
//                {
//                    if (production)
//                    {
//                        adminclient.callProcedure(new Callback(false),
//                                                  "@AdminModeEnter");
//                        production = false;
//                    }
//                    else
//                    {
//                        adminclient.callProcedure(new Callback(false),
//                                                  "@AdminModeExit");
//                        production = true;
//                    }
//                }
//            }
//            client.drain();
//        }
//        finally
//        {
//            adminclient.close();
//            client.close();
//        }
//    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdminMode.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        LocalSingleProcessServer config = new LocalSingleProcessServer("admin-mode1.jar", 2, BackendTarget.NATIVE_EE_JNI);
        // Start in admin mode
        success = config.compileWithAdminMode(project, 32323, true);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
