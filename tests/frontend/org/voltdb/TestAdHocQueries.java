/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.io.IOException;
import java.net.UnknownHostException;
import junit.framework.TestCase;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestAdHocQueries extends AdHocQueryTester {

    Client m_client;

    public void testSP() throws Exception {
        VoltDB.Configuration config = setUpSPDB();
        ServerThread localServer = new ServerThread(config);

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            m_client = ClientFactory.createClient();
            m_client.createConnection("localhost");

            VoltTable modCount;

            // Unlike TestAdHocPlans, TestAdHocQueries runs the queries against actual (minimal) data.
            // Load that, here.
            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO PARTED1 VALUES (0, 0);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO PARTED1 VALUES (1, 1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO PARTED2 VALUES (0, 0);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO PARTED2 VALUES (2, 2);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO PARTED3 VALUES (0, 0);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO PARTED3 VALUES (3, 3);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO REPPED1 VALUES (0, 0);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO REPPED1 VALUES (1, 1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO REPPED2 VALUES (0, 0);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc", "INSERT INTO REPPED2 VALUES (2, 2);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            runAllAdHocSPtests();
        }
        finally {
            if (m_client != null) m_client.close();
            m_client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    /**
     * @param query
     * @param hashable - used to pick a single partition for running the query
     * @param spPartialSoFar - counts from prior SP queries to compensate for unpredictable hashing
     * @param expected - expected value of MP query (and of SP query, adjusting by spPartialSoFar, and only if validatingSPresult).
     * @param validatingSPresult - disables validation for non-deterministic SP results (so we don't have to second-guess the hashinator)
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    @Override
    public int runQueryTest(String query, int hashable, int spPartialSoFar, int expected, int validatingSPresult)
            throws IOException, NoConnectionsException, ProcCallException {
        VoltTable result;
        result = m_client.callProcedure("@AdHoc", query).getResults()[0];
        System.out.println(result.toString());
        assertEquals(expected, result.getRowCount());

        result = m_client.callProcedure("@AdHoc", query, hashable).getResults()[0];
        int spResultImplicit = result.getRowCount();
        System.out.println(result.toString());
        if (validatingSPresult != 0) {
            assertEquals(expected, spPartialSoFar + spResultImplicit);
        }

        result = m_client.callProcedure("@AdHocSP", query, hashable).getResults()[0];
        int spResultExplicit = result.getRowCount();
        System.out.println(result.toString());
        if (validatingSPresult != 0) {
            assertEquals(expected, spPartialSoFar + spResultExplicit);
        }
        assertEquals(spResultExplicit, spResultImplicit);

        return spResultExplicit;
    }

    String m_catalogJar = "adhoc.jar";
    String m_pathToCatalog = Configuration.getPathToCatalogForTest(m_catalogJar);
    String m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");

    public void testSimple() throws Exception {
        VoltProjectBuilder builder = getSimpleBuilder();
        boolean success = builder.compile(m_pathToCatalog, 2, 1, 0);
        assertTrue(success);

        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = m_pathToCatalog;
        config.m_pathToDeployment = m_pathToDeployment;

        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            client = ClientFactory.createClient();
            client.createConnection("localhost");

            runSimpleQueries(client);
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    public void testMultiPartitionAdHoc() throws Exception {
        VoltProjectBuilder builder = getSimpleBuilder();

        LocalCluster cluster = new LocalCluster(m_catalogJar, 2, 2, 1,
                                                BackendTarget.NATIVE_EE_JNI,
                                                LocalCluster.FailureState.ALL_RUNNING,
                                                false, false);
        cluster.setMaxHeap(256);
        boolean success = cluster.compile(builder);
        assert(success);

        MiscUtils.copyFile(builder.getPathToDeployment(), m_pathToDeployment);

        cluster.setHasLocalServer(false);
        cluster.startUp();

        Client client = null;

        try {
            // do the test
            client = ClientFactory.createClient();
            client.createConnection("localhost", cluster.port(1));

            runSimpleQueries(client);
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (cluster != null) {
                cluster.shutDown();
            }
            cluster = null;

            // no clue how helpful this is
            System.gc();
        }
    }

    VoltProjectBuilder getSimpleBuilder() throws IOException {
        String simpleSchema =
                "create table BLAH (" +
                "IVAL bigint default 0 not null, " +
                "TVAL timestamp default null," +
                "DVAL decimal default null," +
                "PRIMARY KEY(IVAL));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("BLAH", "IVAL");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?, ?);", null);
        builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);");
        return builder;
    }

    private void runSimpleQueries(Client client) throws UnknownHostException, IOException,
            NoConnectionsException, ProcCallException {
        VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1);").getResults()[0];
        assertEquals(1, modCount.getRowCount());
        assertEquals(1, modCount.asScalarLong());

        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());

        // test single-partition stuff
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;", 0).getResults()[0];
        assertEquals(0, result.getRowCount());
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH;", 1).getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());

        try {
            client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (0, 0, 0);", 1);
            fail("Badly partitioned insert failed to throw expected exception");
        }
        catch (Exception e) {}

        try {
            client.callProcedure("@AdHoc", "SLEECT * FROOM NEEEW_OOORDERERER;");
            fail("Bad SQL failed to throw expected exception");
        }
        catch (Exception e) {}

        // try a huge bigint literal
        modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5);").getResults()[0];
        modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5);").getResults()[0];
        assertEquals(1, modCount.getRowCount());
        assertEquals(1, modCount.asScalarLong());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26.123012';").getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL > '2011-06-24 10:30:25';").getResults()[0];
        assertEquals(2, result.getRowCount());
        System.out.println(result.toString());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL < '2011-06-24 10:30:27';").getResults()[0];
        System.out.println(result.toString());
        // We inserted a 1,1,1 row way earlier
        assertEquals(2, result.getRowCount());

        // try something like the queries in ENG-1242
        try {
            client.callProcedure("@AdHoc", "select * from blah; dfvsdfgvdf select * from blah WHERE IVAL = 1;");
            fail("Bad SQL failed to throw expected exception");
        }
        catch (Exception e) {}
        client.callProcedure("@AdHoc", "select\n* from blah;");

        // try a decimal calculation (ENG-1093)
        modCount = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (2, '2011-06-24 10:30:26', 1.12345*1);").getResults()[0];
        assertEquals(1, modCount.getRowCount());
        assertEquals(1, modCount.asScalarLong());
        result = client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 2;").getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());

        // try query batches
        VoltTable[] batchResults = client.callProcedure("@AdHoc",
                "INSERT INTO BLAH VALUES (100, '2012-05-21 12:00:00.000000', 1000);" +
                "INSERT INTO BLAH VALUES (101, '2012-05-21 12:01:00.000000', 1001) ; " +
                "INSERT INTO BLAH VALUES (102, '2012-05-21 12:02:00.000000', 1002);;;;" +
                "INSERT INTO BLAH VALUES (103, '2012-05-21 12:03:00.000000', 1003); " +
                "INSERT INTO BLAH VALUES (104, '2012-05-21 12:04:00.000000', 1004) ;;"
                ).getResults();
        assertEquals(5, batchResults.length);
        for (VoltTable batchResult : batchResults) {
            assertTrue(batchResult.getRowCount() == 1);
            assertTrue(batchResult.asScalarLong() == 1);
        }
        batchResults = client.callProcedure("@AdHoc",
                "SELECT * FROM BLAH WHERE IVAL = 102;" +
                "SELECT * FROM BLAH WHERE DVAL >= 1001 AND DVAL <= 1002;" +
                "SELECT * FROM BLAH WHERE DVAL >= 1002 AND DVAL <= 1004;"
                ).getResults();
        assertEquals(3, batchResults.length);
        assertTrue(batchResults[0].getRowCount() == 1);
        assertTrue(batchResults[1].getRowCount() == 2);
        assertTrue(batchResults[2].getRowCount() == 3);
    }
}
