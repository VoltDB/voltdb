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

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

// XXX Once ReadOnlySlow support MP queries, can extend from AdHocQueryTester again
public class TestReadOnlySlowQueries extends ReadOnlySlowQueryTester {

    Client m_client;
    private final static boolean m_debug = false;
    public static final boolean retry_on_mismatch = true;

    @AfterClass
    public static void tearDownClass()
    {
        try {
            VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        }
        catch (IOException e) {};
    }


    @Test
    public void testSP() throws Exception {
        System.out.println("Starting testSP");
        VoltDB.Configuration config = setUpSPDB();
        ServerThread localServer = new ServerThread(config);

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            m_client = ClientFactory.createClient();
            m_client.createConnection("localhost", config.m_port);

            VoltTable modCount;

            //Hashes to partition 0
            int hashableA;
            //Hashes to partition 1
            int hashableB;
            //Hashes to partition 0
            int hashableC;
            //Hashes to partition 1
            int hashableD;
            if (TheHashinator.getConfiguredHashinatorType() == HashinatorType.LEGACY) {
                hashableA = 4;
                hashableB = 1;
                hashableC = 2;
                hashableD = 3;
            } else {
                hashableA = 8;
                hashableB = 2;
                hashableC = 1;
                hashableD = 4;
            }

            //If things break you can use this to find what hashes where and fix the constants
//            for (int ii = 0; ii < 10; ii++) {
//                System.out.println("Partition " + TheHashinator.getPartitionForParameter(VoltType.INTEGER.getValue(), ii) + " param " + ii);
//            }

            // Unlike TestAdHocPlans, TestAdHocQueries runs the queries against actual (minimal) data.
            // Load that, here.
            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED1 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED1 VALUES (%d, %d);", hashableB, hashableB)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED2 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED2 VALUES (%d, %d);", hashableC, hashableC)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED3 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO PARTED3 VALUES (%d, %d);", hashableD, hashableD)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED1 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED1 VALUES (%d, %d);", hashableB, hashableB)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED2 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];

            modCount = m_client.callProcedure("@AdHoc", String.format("INSERT INTO REPPED2 VALUES (%d, %d);", hashableC, hashableC)).getResults()[0];


            runAllAdHocSPtests(hashableA, hashableB, hashableC, hashableD);
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
            System.out.println("Ending testSP");
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
    public void runQueryTest(String query, int hashable, int spPartialSoFar, int expected, boolean validateResult)
            throws IOException, NoConnectionsException, ProcCallException {

        //System.out.println("@ReadOnlySlow:");
        VoltTable result2 = m_client.callProcedure("@ReadOnlySlow", query).getResults()[0];

        VoltTable lrrResult = LRRHelper.getTableFromFileTable(result2);
        //System.out.println(lrrResult.toString());
        assertEquals("Result sizes don't match: expected " + expected + ", read "+ lrrResult.getRowCount(),expected, lrrResult.getRowCount());


        if (validateResult) {
            // Compare to @AdHoc results
            //System.out.println("@AdHoc:");
            VoltTable result = m_client.callProcedure("@AdHoc", query).getResults()[0];
            //System.out.println(result.toString());
            assertEquals(expected, result.getRowCount());

            assert(result.equals(lrrResult));
        }

    }

    public static String m_catalogJar = "adhoc.jar";
    public static String m_pathToCatalog = Configuration.getPathToCatalogForTest(m_catalogJar);
    public static String m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");


    @Test
    public void testSinglePartition() throws Exception {
        System.out.println("Starting testSinglePartition");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 1, 1, 0);
        try {
            env.setUp();

            VoltTable result;
            VoltTable files;

            VoltTable modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1, 1.0, 'foo');").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            files = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH;").getResults()[0];
            assertEquals(1, files.getRowCount());
            System.out.println(files.toString());
            result = LRRHelper.getTableFromFileTable(files);
            System.out.println(result.toString());
            assertEquals(1, result.getRowCount());

            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5, 3.14, 'green');").getResults()[0];
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5, 3.14, 'eggs');").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            System.out.println(result.toString());
            assertEquals(1, result.getRowCount());
            files = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            assertEquals(1, files.getRowCount());
            System.out.println(files.toString());
            result = LRRHelper.getTableFromFileTable(files);
            System.out.println(result.toString());
            assertEquals(1, result.getRowCount());


        }
        finally {
            env.tearDown();
            System.out.println("Ending testSinglePartition");
        }
    }

    @Test
    public void testMultiPartition() throws Exception {
        System.out.println("Starting testMultiPartition");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();

            VoltTable result;
            VoltTable files;

            VoltTable modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (1, 1, 1, 1.0, 'foo');").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            try {
                files = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH;").getResults()[0];
                fail("Failed to throw Planning Error for multi-partition transaction");
            } catch (Exception e) {}

            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5, 3.14, 'green');").getResults()[0];
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5, 3.14, 'eggs');").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());


            try {
                result = env.m_client.callProcedure("@ReadOnlySlow", "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26.123012';").getResults()[0];
                fail("Failed to throw Planning Error for multi-partition transaction");
            } catch (Exception e) {}



        }
        finally {
            env.tearDown();
            System.out.println("Ending testMultiPartition");
        }
    }


    /**
     * Test environment with configured schema and server.
     */
    public static class TestEnv {

        final VoltProjectBuilder m_builder;
        LocalCluster m_cluster;
        Client m_client = null;

        TestEnv(String pathToCatalog, String pathToDeployment,
                     int siteCount, int hostCount, int kFactor) {

            // hack for no k-safety in community version
            if (!MiscUtils.isPro()) {
                kFactor = 0;
            }

            m_builder = new VoltProjectBuilder();
            //Increase query tmeout as long literal queries taking long time.
            m_builder.setQueryTimeout(60000);
            try {
                if (siteCount > 1) {
                    // Multi-partition
                    m_builder.addLiteralSchema("create table BLAH (" +
                                           "IVAL bigint default 0 not null, " +
                                           "TVAL timestamp default null," +
                                           "DVAL decimal default null," +
                                           "FVAL float default null," +
                                           "SVAL varchar(10) default null," +
                                           "PRIMARY KEY(IVAL));\n" +
                                           "PARTITION TABLE BLAH ON COLUMN IVAL;\n" +
                                           "\n" +
                                           "");
                } else {
                    // Single-partition
                    m_builder.addLiteralSchema("create table BLAH (" +
                            "IVAL bigint default 0 not null, " +
                            "TVAL timestamp default null," +
                            "DVAL decimal default null," +
                            "FVAL float default null," +
                            "SVAL varchar(10) default null," +
                            "PRIMARY KEY(IVAL));\n" +

                            "\n" +
                            "");
                }

            }
            catch (Exception e) {
                e.printStackTrace();
                fail("Failed to set up schema");
            }

            m_cluster = new LocalCluster(pathToCatalog, siteCount, hostCount, kFactor,
                                         BackendTarget.NATIVE_EE_JNI,
                                         LocalCluster.FailureState.ALL_RUNNING,
                                         m_debug);
            m_cluster.setHasLocalServer(true);
            boolean success = m_cluster.compile(m_builder);
            assert(success);

            try {
                MiscUtils.copyFile(m_builder.getPathToDeployment(), pathToDeployment);
            }
            catch (Exception e) {
                fail(String.format("Failed to copy \"%s\" to \"%s\"", m_builder.getPathToDeployment(), pathToDeployment));
            }
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

        boolean isValgrind() {
            if (m_cluster != null)
                return m_cluster.isValgrind();
            return true;
        }

        boolean isMemcheckDefined() {
            return (m_cluster != null) ? m_cluster.isMemcheckDefined() : true;
        }
    }

}
