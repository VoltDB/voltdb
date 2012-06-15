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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
    private final static boolean m_debug = true;

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

        result = m_client.callProcedure("@AdHoc_RW_SP", query, hashable).getResults()[0];
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
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();

            VoltTable modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            VoltTable result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH;").getResults()[0];
            assertEquals(1, result.getRowCount());
            System.out.println(result.toString());

            // test single-partition stuff
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH;", 0).getResults()[0];
            assertEquals(0, result.getRowCount());
            System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH;", 1).getResults()[0];
            assertEquals(1, result.getRowCount());
            System.out.println(result.toString());

            try {
                env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (0, 0, 0);", 1);
                fail("Badly partitioned insert failed to throw expected exception");
            }
            catch (Exception e) {}

            try {
                env.m_client.callProcedure("@AdHoc", "SLEECT * FROOM NEEEW_OOORDERERER;");
                fail("Bad SQL failed to throw expected exception");
            }
            catch (Exception e) {}

            // try a huge bigint literal
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5);").getResults()[0];
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            assertEquals(1, result.getRowCount());
            System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26.123012';").getResults()[0];
            assertEquals(1, result.getRowCount());
            System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL > '2011-06-24 10:30:25';").getResults()[0];
            assertEquals(2, result.getRowCount());
            System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL < '2011-06-24 10:30:27';").getResults()[0];
            System.out.println(result.toString());
            // We inserted a 1,1,1 row way earlier
            assertEquals(2, result.getRowCount());

            // try something like the queries in ENG-1242
            try {
                env.m_client.callProcedure("@AdHoc", "select * from blah; dfvsdfgvdf select * from blah WHERE IVAL = 1;");
                fail("Bad SQL failed to throw expected exception");
            }
            catch (Exception e) {}
            env.m_client.callProcedure("@AdHoc", "select\n* from blah;");

            // try a decimal calculation (ENG-1093)
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (2, '2011-06-24 10:30:26', 1.12345*1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 2;").getResults()[0];
            assertEquals(1, result.getRowCount());
            System.out.println(result.toString());
        }
        finally {
            env.tearDown();
        }
    }

    public void testAdHocBatches() throws Exception {
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            Batcher batcher = new Batcher(env);

            // a few inserts (with a couple of ignored blank statements)
            batcher.add("INSERT INTO BLAH VALUES (100, '2012-05-21 12:00:00.000000', 1000)", 1);
            batcher.add("", null);
            batcher.add("INSERT INTO BLAH VALUES (101, '2012-05-21 12:01:00.000000', 1001)", 1);
            batcher.add("", null);
            batcher.add("INSERT INTO BLAH VALUES (102, '2012-05-21 12:02:00.000000', 1002)", 1);
            batcher.add("INSERT INTO BLAH VALUES (103, '2012-05-21 12:03:00.000000', 1003)", 1);
            batcher.add("INSERT INTO BLAH VALUES (104, '2012-05-21 12:04:00.000000', 1004)", 1);
            batcher.run();

            // a few selects using data inserted by previous batch
            batcher.add("SELECT * FROM BLAH WHERE IVAL = 102", 1);
            batcher.add("SELECT * FROM BLAH WHERE DVAL >= 1001 AND DVAL <= 1002", 2);
            batcher.add("SELECT * FROM BLAH WHERE DVAL >= 1002 AND DVAL <= 1004", 3);
            batcher.run();

            // mixed reads and writes (start from a clean slate)
            batcher.addUnchecked("DELETE FROM BLAH");
            batcher.run();
            batcher.add("INSERT INTO BLAH VALUES (100, '2012-05-21 12:00:00.000000', 1000)", 1);
            batcher.add("INSERT INTO BLAH VALUES (101, '2012-05-21 12:00:00.000000', 1001)", 1);
            batcher.add("INSERT INTO BLAH VALUES (102, '2012-05-21 12:00:00.000000', 1002)", 1);
            batcher.add("DELETE FROM BLAH WHERE IVAL = 100", 1);
            batcher.add("SELECT * FROM BLAH", 2);
            batcher.add("DELETE FROM BLAH WHERE IVAL = 101", 1);
            batcher.add("SELECT * FROM BLAH WHERE IVAL = 101", 0);
            batcher.add("UPDATE BLAH SET DVAL = 0 WHERE IVAL = 102", 1);
            batcher.run();

            // mix replicated and partitioned
            batcher.addUnchecked("DELETE FROM PARTED1");
            batcher.addUnchecked("DELETE FROM REPPED1");
            for (int i = 1; i <= 10; i++) {
                batcher.add(String.format("INSERT INTO PARTED1 VALUES (%d, %d)", i, 100+i), 1);
                batcher.add(String.format("INSERT INTO REPPED1 VALUES (%d, %d)", i, 100+i), 1);
            }
            batcher.run();
            batcher.add("SELECT * FROM PARTED1", 10);
            batcher.add("SELECT * FROM REPPED1", 10);
            batcher.add("DELETE FROM PARTED1 WHERE PARTVAL > 5", 5);
            batcher.add("DELETE FROM REPPED1 WHERE REPPEDVAL > 5", 5);
            batcher.add("SELECT * FROM PARTED1", 5);
            batcher.add("SELECT * FROM REPPED1", 5);
            batcher.run();

            // roll-back entire batch if one statement fails (start from a clean slate)
            batcher.addUnchecked("DELETE FROM BLAH");
            batcher.run();
            batcher.add("INSERT INTO BLAH VALUES (100, '2012-05-21 12:00:00.000000', 1000)", 1);
            batcher.run();
            // this should succeed, but won't due to the failure below
            batcher.add("INSERT INTO BLAH VALUES (101, '2012-05-21 12:00:00.000000', 1001)", 0);
            // this will fail the batch due to a PK constraint violation
            batcher.add("INSERT INTO BLAH VALUES (100, '2012-05-21 12:00:00.000000', 1000)", 0);
            batcher.runWithException();
            // expect 1 row, not 2.
            batcher.add("SELECT * FROM BLAH", 1);
            batcher.run();
        }
        finally {
            env.tearDown();
        }
    }

    /**
     * Builds and validates query batch runs.
     */
    private static class Batcher {
        private final TestEnv m_env;
        private final List<Integer> m_expectedCounts = new ArrayList<Integer>();
        private final List<String> m_queries = new ArrayList<String>();

        public Batcher(final TestEnv env) {
            m_env = env;
        }

        void add(String query, Integer expectedCount) {
            m_queries.add(query);
            if (expectedCount != null) {
                m_expectedCounts.add(expectedCount);
            }
        }

        void addUnchecked(String query) {
            m_queries.add(query);
            m_expectedCounts.add(-1);
        }

        void run() throws Exception {
            runAndCheck(false);
        }

        void runWithException() throws Exception {
            runAndCheck(true);
        }

        private void runAndCheck(boolean expectException) throws Exception {
            try {
                VoltTable[] results = m_env.m_client.callProcedure("@AdHoc",
                        StringUtils.join(m_queries, "; ")).getResults();
                int i = 0;
                assertEquals(m_expectedCounts.size(), results.length);
                for (String query : m_queries) {
                    int expectedCount = m_expectedCounts.get(i);
                    if (expectedCount >= 0) {
                        String s = query.toLowerCase().trim();
                        if (!s.isEmpty()) {
                            if (   s.startsWith("insert")
                                || s.startsWith("update")
                                || s.startsWith("delete")) {
                                assertEquals(String.format("%s (row count):",query),
                                             1, results[i].getRowCount());
                                assertEquals(String.format("%s (result count):",query),
                                             expectedCount, results[i].asScalarLong());
                            } else {
                                assertEquals(String.format("%s (row count):",query),
                                             expectedCount, results[i].getRowCount());
                            }
                            i++;
                        }
                    }
                }
            }
            catch(ProcCallException e) {
                assertTrue("Unexpected exception for batch: " + e.getMessage(), expectException);
            }
            finally {
                m_queries.clear();
                m_expectedCounts.clear();
            }
        }
    }

    /**
     * Test environment with configured schema and server.
     */
    private static class TestEnv {

        final VoltProjectBuilder m_builder;
        LocalCluster m_cluster;
        Client m_client = null;

        TestEnv(String pathToCatalog, String pathToDeployment,
                     int siteCount, int hostCount, int kFactor) {
            m_builder = new VoltProjectBuilder();
            try {
                m_builder.addLiteralSchema("create table BLAH (" +
                                           "IVAL bigint default 0 not null, " +
                                           "TVAL timestamp default null," +
                                           "DVAL decimal default null," +
                                           "PRIMARY KEY(IVAL));");
                m_builder.addPartitionInfo("BLAH", "IVAL");
                m_builder.addStmtProcedure("Insert", "INSERT into BLAH values (?, ?, ?);", null);
                m_builder.addStmtProcedure("InsertWithDate", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);");

                // add more partitioned and replicated tables, PARTED[1-3] and REPED[1-2]
                AdHocQueryTester.setUpSchema(m_builder, pathToCatalog, pathToDeployment);
            }
            catch (Exception e) {
                e.printStackTrace();
                fail("Failed to set up schema");
            }

            m_cluster = new LocalCluster(pathToCatalog, siteCount, hostCount, kFactor,
                                         BackendTarget.NATIVE_EE_JNI,
                                         LocalCluster.FailureState.ALL_RUNNING,
                                         m_debug, false);
            boolean success = m_cluster.compile(m_builder);
            assert(success);

            try {
                MiscUtils.copyFile(m_builder.getPathToDeployment(), pathToDeployment);
            }
            catch (Exception e) {
                fail(String.format("Failed to copy \"%s\" to \"%s\"", m_builder.getPathToDeployment(), pathToDeployment));
            }

            m_cluster.setHasLocalServer(true);
        }

        void setUp() {
            m_cluster.startUp();

            try {
                // do the test
                m_client = ClientFactory.createClient();
                m_client.createConnection("localhost", m_cluster.port(0));
            }
            catch (UnknownHostException e) {
                e.printStackTrace();
                fail(String.format("Failed to connect to localhost:%d", m_cluster.port(0)));
            }
            catch (IOException e) {
                e.printStackTrace();
                fail(String.format("Failed to connect to localhost:%d", m_cluster.port(0)));
            }
        }

        void tearDown() {
            if (m_client != null) {
                try {
                    m_client.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Failed to close client");
                }
            }
            m_client = null;

            if (m_cluster != null) {
                try {
                    m_cluster.shutDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Failed to shut down cluster");
                }
            }
            m_cluster = null;

            // no clue how helpful this is
            System.gc();
        }
    }

}
