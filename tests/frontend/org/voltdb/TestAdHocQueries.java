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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.voltdb.regressionsuites.RegressionSuite.assertContentOfTable;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.sysprocs.AdHocNTBase;
import org.voltdb.types.TimestampType;

public class TestAdHocQueries extends AdHocQueryTester {

    private Client m_client;
    private final static boolean m_debug = false;

    @AfterClass
    public static void tearDownClass() {
        try {
            FileUtils.deleteDirectory(new File("/tmp/" + System.getProperty("user.name")));
        } catch (IOException ignored) {}
    }

    @Test
    public void testProcedureAdhoc() throws Exception {
        final VoltDB.Configuration config = setUpSPDB();
        final ServerThread localServer = new ServerThread(config);

        try {
            localServer.start();
            localServer.waitForInitialization();

            // do the test
            m_client = ClientFactory.createClient();
            m_client.createConnection("localhost", config.m_port);

            m_client.callProcedure("@AdHoc", "insert into PARTED1 values ( 23, 3 )");

            //
            // Test that a basic multipartition select works as well as a parameterized
            // query (it's in the procedure)
            //
            VoltTable[] results = m_client.callProcedure(
                    "executeSQLSP", 23, "select * from PARTED1").getResults();
            assertTrue(results[0].advanceRow());
            assertTrue(results[1].advanceRow());

            results = m_client.callProcedure("executeSQLMP", 23, "select * from PARTED1")
                    .getResults();
            assertTrue(results[0].advanceRow());
            assertTrue(results[1].advanceRow());

            //
            // Validate that doing an insert from a RO procedure fails
            //
            try {
                m_client.callProcedure("executeSQLSP", 24, "insert into parted1 values (24,5)");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException ignored) {}

            try {
                m_client.callProcedure("executeSQLMP", 24, "insert into parted1 values (24,5)");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException ignored) {}

            //
            // Validate one sql statement per
            //
            try {
                m_client.callProcedure("executeSQLSP",
                        24, "insert into parted1 values (24,5); select * from parted1;");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException ignored) {}

            try {
                m_client.callProcedure("executeSQLSP", 24, "drop table parted1");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException ignored) {}


            //
            // Validate that an insert does work from a write procedure
            //
            m_client.callProcedure("executeSQLSPWRITE", 24, "insert into parted1 values (24, 4);");
            m_client.callProcedure("executeSQLMPWRITE", 25, "insert into parted1 values (25, 5);");

            //
            // Query the inserts and all the rest do it once for singe and once for multi
            //
            results = m_client.callProcedure("executeSQLMP",
                    24, "select * from parted1 order by partval").getResults();

            assertEquals(3, results[0].getRowCount());
            for (int ii = 3; ii < 6; ii++) {
                assertTrue(results[0].advanceRow());
                assertEquals(20 + ii, results[0].getLong(0));
                assertEquals(ii, results[0].getLong(1));
            }

            //Output from the first preplanned statement
            assertEquals(3, results[1].getRowCount());
            assertTrue(results[1].advanceRow());
            assertEquals(23, results[1].getLong(0));
            assertEquals(3, results[1].getLong(1));

            //Output from the second adhoc statement
            assertEquals(1, results[2].getRowCount());
            assertTrue(results[2].advanceRow());
            assertEquals(24, results[2].getLong(0));
            assertEquals(4, results[2].getLong(1));

            //Output from the second preplanned statement
            assertEquals(3, results[3].getRowCount());
            assertTrue(results[3].advanceRow());
            assertEquals(23, results[3].getLong(0));
            assertEquals(3, results[3].getLong(1));

            results = m_client.callProcedure("executeSQLSP",
                    24, "select * from parted1 order by partval").getResults();

            //These constants break when partitioning changes
            //Recently 23, 24, and 25, started hashing to the same place /facepalm
            for (int ii = 0; ii < 4; ii++) {
                //The third statement does an exact equality match
                if (ii == 2) {
                    assertEquals( 1, results[ii].getRowCount());
                    assertTrue(results[ii].advanceRow());
                    assertEquals(24, results[ii].getLong(0));
                    assertEquals( 4, results[ii].getLong(1));
                    continue;
                }
                assertEquals( 3, results[ii].getRowCount());
                assertTrue(results[ii].advanceRow());
                assertEquals(23, results[ii].getLong(0));
                assertEquals( 3, results[ii].getLong(1));
                assertTrue(results[ii].advanceRow());
                assertEquals(24, results[ii].getLong(0));
                assertEquals( 4, results[ii].getLong(1));
                assertTrue(results[ii].advanceRow());
                assertEquals(25, results[ii].getLong(0));
                assertEquals( 5, results[ii].getLong(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            if (m_client != null) m_client.close();
            m_client = null;

            localServer.shutdown();
            localServer.join();
        }
    }

    @Test
    public void testSP() throws Exception {
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
            int hashableA = 8;
            //Hashes to partition 1
            int hashableB = 2;
            //Hashes to partition 0
            int hashableC = 1;
            //Hashes to partition 1
            int hashableD = 4;

            // Unlike TestAdHocPlans, TestAdHocQueries runs the queries against actual (minimal) data.
            // Load that, here.
            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO PARTED1 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO PARTED1 VALUES (%d, %d);", hashableB, hashableB)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO PARTED2 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO PARTED2 VALUES (%d, %d);", hashableC, hashableC)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO PARTED3 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO PARTED3 VALUES (%d, %d);", hashableD, hashableD)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO REPPED1 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO REPPED1 VALUES (%d, %d);", hashableB, hashableB)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO REPPED2 VALUES (%d, %d);", hashableA, hashableA)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            modCount = m_client.callProcedure("@AdHoc",
                    String.format("INSERT INTO REPPED2 VALUES (%d, %d);", hashableC, hashableC)).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            // verify that inserts to a table partitioned on an integer get handled correctly - results not used later
            for (int i = -7; i <= 7; i++) {
                modCount = m_client.callProcedure("@AdHoc",
                        String.format("INSERT INTO PARTED4 VALUES (%d, %d);", i, i)).getResults()[0];
                assertEquals(1, modCount.getRowCount());
                assertEquals(1, modCount.asScalarLong());
            }

            runAllAdHocSPtests(hashableA, hashableB, hashableC, hashableD);
        } finally {
            if (m_client != null) {
                m_client.close();
                m_client = null;
            }

            localServer.shutdown();
            localServer.join();
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
        assertEquals(expected, result.getRowCount());

        result = m_client.callProcedure("@AdHocSpForTest", query, hashable).getResults()[0];
        int spResult = result.getRowCount();
        if (validatingSPresult != 0) {
            assertEquals(expected, spPartialSoFar + spResult);
        }
        return spResult;
    }

    public static String m_catalogJar = "adhoc.jar";
    public static String m_pathToCatalog = Configuration.getPathToCatalogForTest(m_catalogJar);
    public static String m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");

    @Test
    public void testENG21651() {
        final TestEnv env = new TestEnv("CREATE TABLE PROD (ID int NOT NULL, EXT_ID varchar(30));",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        long ids[] = {1617463880188133744L,1617463880188133743L,1617463880188133742L,1617463880188134165L,7463880188133420L};
        try {
            env.setUp();
            for (int i = 0; i < ids.length; i++) {
                env.m_client.callProcedure("@AdHoc",
                        "INSERT INTO PROD (ID, EXT_ID) VALUES(" + i + ",'" + Long.toString(ids[i]) + "')");
            }

            VoltTable result = env.m_client.callProcedure("@AdHoc",
                    "select ID, EXT_ID, CAST(EXT_ID AS BIGINT) FROM PROD").getResults()[0];
            while (result.advanceRow()) {
                int id = (int) result.getLong(0);
                long extId = result.getLong(2);
                System.out.println("cast " + ids[id] + " to:" + extId);
                assert(ids[id] == extId);
            }
        } catch (IOException | ProcCallException e) {
            fail("failed: " + e.getMessage());
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG15335() {
        final TestEnv env = new TestEnv("CREATE TABLE P0(id INTEGER NOT NULL);\n" +
                "PARTITION TABLE P0 ON COLUMN id;\n" +
                "CREATE TABLE R21(VCHAR_INLINE VARCHAR(14));\n" +
                "DROP INDEX didx0 IF EXISTS;",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            Stream.of("SELECT id FROM P0;",
                    "SELECT VCHAR_INLINE FROM R21 WHERE (SELECT MIN(VCHAR_INLINE) FROM R21) LIKE 'abc%';")
                    .forEachOrdered(query -> {
                        try {
                            assertEquals("Query \"" + query + "\" Should have passed",
                                    ClientResponse.SUCCESS,
                                    env.m_client.callProcedure("@AdHoc", query).getStatus());
                        } catch (IOException | ProcCallException e) {
                            fail("Should have passed query \"" + query + "\": " + e.getMessage());
                        }
                    });
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG15572() {
        // Test subqueries on either side of LIKE/START WITH expression
        final TestEnv env = new TestEnv("CREATE TABLE R0(v VARCHAR(100));",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            Stream.of("SELECT * FROM R0 WHERE (SELECT TOP 1 V FROM R0 ORDER BY V) STARTS WITH V LIMIT 1;",
                    "SELECT * FROM R0 WHERE V STARTS WITH (SELECT MAX('s') FROM R0);",
                    "SELECT * FROM R0 WHERE (SELECT TOP 1 V FROM R0 ORDER BY V) LIKE V LIMIT 1;",
                    "SELECT * FROM R0 WHERE V LIKE (SELECT MAX('s') FROM R0);")
            .forEachOrdered(query -> {
                try {
                    assertEquals("Query \"" + query + "\" Should have passed",
                            ClientResponse.SUCCESS,
                            env.m_client.callProcedure("@AdHoc", query).getStatus());
                } catch (IOException | ProcCallException e) {
                    fail("Should have passed query \"" + query + "\": " + e.getMessage());
                }
            });
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG18533() {        // presence of order-by in 2nd sub-query used to make planner think it's cross-partition
        final TestEnv env = new TestEnv(
                "CREATE TABLE RPT_PERF_BREAKDOWN (\n" +
                        "    CUSTOM2 varchar(30),\n" +
                        "    CLIENT_ID integer NOT NULL,\n" +
                        "    IMPRESSIONS integer\n" +
                        ");\n" +
                        "PARTITION TABLE RPT_PERF_BREAKDOWN ON COLUMN CLIENT_ID;",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            // populate table
            Stream.of("INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 0, 0);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 0, 1);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 0, 2);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 0, 3);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 0, 4);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 0, 5);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 1, 0);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 1, 1);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 1, 2);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 1, 3);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 1, 4);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 2, 1);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 2, 2);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 2, 3);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 2, 4);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 2, 5);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foo', 3, 0);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 3, 2);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 3, 3);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 3, 4);\n" +
                    "INSERT INTO RPT_PERF_BREAKDOWN VALUES('foox', 3, 5);\n")
                    .forEach(query -> {
                        try {
                            assertEquals("Insertion \"" + query + "\" Should have worked",
                                    ClientResponse.SUCCESS,
                                    env.m_client.callProcedure("@AdHoc", query).getStatus());
                        } catch (IOException | ProcCallException e) {
                            fail("Should have inserted a row: \"" + query + "\": " + e.getMessage());
                        }
                    });
            Stream.of(Pair.of("SELECT t1.IMPRESSIONS FROM (\n" +
                            "    SELECT CLIENT_ID, CUSTOM2, sum(IMPRESSIONS) as IMPRESSIONS, cast(SUM(IMPRESSIONS) as decimal) AS metric\n" +
                            "    FROM rpt_perf_breakdown\n" +
                            "    GROUP BY CLIENT_ID,CUSTOM2) t1\n" +
                            "LEFT OUTER JOIN (\n" +
                            "    SELECT CLIENT_ID, CUSTOM2, sum(IMPRESSIONS) as IMPRESSIONS, cast(SUM(IMPRESSIONS) as decimal) AS metric\n" +
                            "    FROM rpt_perf_breakdown\n" +
                            "    GROUP BY CLIENT_ID, CUSTOM2\n" +
                            "    ORDER BY metric) t2\n" +
                            "ON t1.CLIENT_ID = t2.CLIENT_ID AND t1.CUSTOM2=t2.CUSTOM2\n" +
                            "ORDER BY t1.IMPRESSIONS;",
                    new Object[][] {{0}, {1}, {2}, {4}, {9}, {11}, {13}, {14}}),
                    Pair.of("SELECT t1.IMPRESSIONS FROM (\n" +
                                    "    SELECT CLIENT_ID, CUSTOM2, sum(IMPRESSIONS) as IMPRESSIONS, cast(SUM(IMPRESSIONS) as decimal) AS metric\n" +
                                    "    FROM rpt_perf_breakdown WHERE CLIENT_ID > 0\n" +
                                    "    GROUP BY CLIENT_ID,CUSTOM2) t1\n" +
                                    "LEFT OUTER JOIN (\n" +
                                    "    SELECT CLIENT_ID, CUSTOM2, sum(IMPRESSIONS) as IMPRESSIONS, cast(SUM(IMPRESSIONS) as decimal) AS metric\n" +
                                    "    FROM rpt_perf_breakdown WHERE CLIENT_ID < 5\n" +
                                    "    GROUP BY CLIENT_ID, CUSTOM2\n" +
                                    "    ORDER BY metric) t2\n" +
                                    "ON t1.CLIENT_ID = t2.CLIENT_ID AND t1.CUSTOM2=t2.CUSTOM2\n" +
                                    "ORDER BY t1.IMPRESSIONS;",
                            new Object[][] {{0}, {1}, {2}, {9}, {13}, {14}}),
                    Pair.of("SELECT t1.IMPRESSIONS FROM (\n" +
                                    "    SELECT CLIENT_ID, CUSTOM2, sum(IMPRESSIONS) as IMPRESSIONS, cast(SUM(IMPRESSIONS) as decimal) AS metric\n" +
                                    "    FROM rpt_perf_breakdown WHERE CLIENT_ID = 1\n" +
                                    "    GROUP BY CLIENT_ID,CUSTOM2) t1\n" +
                                    "LEFT OUTER JOIN (\n" +
                                    "    SELECT CLIENT_ID, CUSTOM2, sum(IMPRESSIONS) as IMPRESSIONS, cast(SUM(IMPRESSIONS) as decimal) AS metric\n" +
                                    "    FROM rpt_perf_breakdown WHERE CLIENT_ID = 1\n" +
                                    "    GROUP BY CLIENT_ID, CUSTOM2\n" +
                                    "    ORDER BY metric) t2\n" +
                                    "ON t1.CUSTOM2=t2.CUSTOM2\n" +
                                    "ORDER BY t1.IMPRESSIONS;",
                            new Object[][] {{1}, {9}}))
                    .forEach(queryAndResult -> {
                        final String query = queryAndResult.getFirst();
                        final Object[][] expected = queryAndResult.getSecond();
                        try {
                            final ClientResponse response = env.m_client.callProcedure("@AdHoc", query);
                            assertEquals("Query \"" + query + "\" Should have passed",
                                    ClientResponse.SUCCESS, response.getStatus());
                            assertContentOfTable(expected, response.getResults()[0]);
                        } catch (IOException | ProcCallException e) {
                            fail("Should have passed query \"" + query + "\": " + e.getMessage());
                        }
                    });
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG16982() {
        final TestEnv env = new TestEnv(
                "CREATE TABLE P5(ID INTEGER, VCHAR_INLINE VARCHAR(42), VCHAR_OUTLINE_MIN VARCHAR(16), VCHAR VARCHAR);\n" +
                "CREATE UNIQUE INDEX IDX_P5_IV ON P5 (VCHAR) WHERE ID > 0;",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            Stream.of("insert INTO P5(VCHAR, VCHAR_OUTLINE_MIN, ID) VALUES('', '', 5712);",
                    "UPDATE P5 SET VCHAR = VCHAR_OUTLINE_MIN;",
                    "UPDATE P5 SET VCHAR = VCHAR_INLINE;")
                    .forEachOrdered(query -> {
                        try {
                            assertEquals("Query \"" + query + "\" Should have passed",
                                    ClientResponse.SUCCESS,
                                    env.m_client.callProcedure("@AdHoc", query).getStatus());
                        } catch (IOException | ProcCallException e) {
                            fail("Should have passed query \"" + query + "\": " + e.getMessage());
                        }
                    });
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testSimple() throws Exception {
        System.out.println("Starting testSimple");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();

            VoltTable modCount = env.m_client.callProcedure("@AdHoc",
                    "INSERT INTO BLAH VALUES (1, 1, 1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            VoltTable result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH;").getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());

            // test single-partition stuff
            // TODO: upgrade to use @GetPartitionKeys instead of TheHashinator interface
            VoltTable result1 = env.m_client.callProcedure("@AdHocSpForTest",
                    "SELECT * FROM BLAH;", 2).getResults()[0];
            //System.out.println(result1.toString());
            VoltTable result2 = env.m_client.callProcedure("@AdHocSpForTest",
                    "SELECT * FROM BLAH;", 0).getResults()[0];
            //System.out.println(result2.toString());
            assertEquals(1, result1.getRowCount() + result2.getRowCount());
            assertEquals(0, result1.getRowCount());
            assertEquals(1, result2.getRowCount());

            try {
                env.m_client.callProcedure("@AdHocSpForTest",
                        "INSERT INTO BLAH VALUES (0, 0, 0);", 2);
                fail("Badly partitioned insert failed to throw expected exception");
            } catch (Exception ignored) {}

            try {
                env.m_client.callProcedure("@AdHoc", "SLEECT * FROOM NEEEW_OOORDERERER;");
                fail("Bad SQL failed to throw expected exception");
            } catch (Exception ignored) {}

            // try a huge bigint literal
            env.m_client.callProcedure("@AdHoc",
                    "INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.123012', 5);");
            modCount = env.m_client.callProcedure("@AdHoc",
                    "INSERT INTO BLAH VALUES (974599638818488301, '2011-06-24 10:30:28', 5);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE IVAL = 974599638818488300;").getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE TVAL = '2011-06-24 10:30:26.123012';").getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE TVAL > '2011-06-24 10:30:25';").getResults()[0];
            assertEquals(2, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE TVAL < '2011-06-24 10:30:27';").getResults()[0];
            //System.out.println(result.toString());
            // We inserted a 1,1,1 row way earlier
            assertEquals(2, result.getRowCount());

            // try something like the queries in ENG-1242
            try {
                env.m_client.callProcedure("@AdHoc",
                        "select * from blah; dfvsdfgvdf select * from blah WHERE IVAL = 1;");
                fail("Bad SQL failed to throw expected exception");
            } catch (Exception ignored) {}
            env.m_client.callProcedure("@AdHoc", "select\n* from blah;");

            // try a decimal calculation (ENG-1093)
            modCount = env.m_client.callProcedure("@AdHoc",
                    "INSERT INTO BLAH VALUES (2, '2011-06-24 10:30:26', 1.12345*1);").getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE IVAL = 2;").getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());

            // test wasNull asScalarLong
            long value;
            boolean wasNull;
            result = env.m_client.callProcedure("@AdHoc",
                    "select top 1 cast(null as tinyInt) from BLAH").getResults()[0];
            value = result.asScalarLong();
            wasNull = result.wasNull();
            assertEquals(VoltType.NULL_TINYINT, value);
            assertTrue(wasNull);

            result = env.m_client.callProcedure("@AdHoc",
                    "select top 1 cast(null as smallInt) from BLAH").getResults()[0];
            value = result.asScalarLong();
            wasNull = result.wasNull();
            assertEquals(VoltType.NULL_SMALLINT, value);
            assertTrue(wasNull);

            result = env.m_client.callProcedure("@AdHoc",
                    "select top 1 cast(null as integer) from BLAH").getResults()[0];
            value = result.asScalarLong();
            wasNull = result.wasNull();
            assertEquals(VoltType.NULL_INTEGER, value);
            assertTrue(wasNull);

            result = env.m_client.callProcedure("@AdHoc",
                    "select top 1 cast(null as bigint) from BLAH").getResults()[0];
            value = result.asScalarLong();
            wasNull = result.wasNull();
            assertEquals(VoltType.NULL_BIGINT, value);
            assertTrue(wasNull);
        } finally {
            env.tearDown();
            System.out.println("Ending testSimple");
        }
    }

    @Test
    public void testAdHocLengthLimit() throws Exception {
        System.out.println("Starting testAdHocLengthLimit");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);

        env.setUp();
        // by pass valgrind due to ENG-7843
        if (env.isValgrind() || env.isMemcheckDefined()) {
            env.tearDown();
            System.out.println("Skipped testAdHocLengthLimit");
            return;
        }
        try {
            StringBuilder adHocQueryTemp = new StringBuilder("SELECT * FROM VOTES WHERE PHONE_NUMBER IN (");
            int i = 0;
            while (adHocQueryTemp.length() <= Short.MAX_VALUE * 2) {
                String randPhone = RandomStringUtils.randomNumeric(10);
                VoltTable result = env.m_client.callProcedure("@AdHoc",
                        "INSERT INTO VOTES VALUES(?, ?, ?);", randPhone, "MA", i).getResults()[0];
                assertEquals(1, result.getRowCount());
                adHocQueryTemp.append(randPhone);
                adHocQueryTemp.append(", ");
                i++;
            }
            adHocQueryTemp.replace(adHocQueryTemp.length()-2, adHocQueryTemp.length(), ");");
            // assure that adhoc query text can exceed 2^15 length, but the literals still cannot exceed 2^15
            assert(adHocQueryTemp.length() > Short.MAX_VALUE);
            assert(i < Short.MAX_VALUE);
            VoltTable result = env.m_client.callProcedure("@AdHoc", adHocQueryTemp.toString()).getResults()[0];
            assertEquals(i, result.getRowCount());
        } finally {
            env.tearDown();
            System.out.println("Ending testAdHocLengthLimit");
        }
    }

    @Test
    public void testAdHocWithParams() throws Exception {
        System.out.println("Starting testAdHocWithParams");
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();

            VoltTable modCount = env.m_client.callProcedure("@AdHoc",
                    "INSERT INTO BLAH VALUES (?, ?, ?);", 1, 1, 1).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());

            VoltTable result;
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE IVAL = ?;", 1).getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());

            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE IVAL = ?;", 2).getResults()[0];
            assertEquals(0, result.getRowCount());
            //System.out.println(result.toString());

            // test single-partition stuff
            // TODO: upgrade to use @GetPartitionKeys instead of TheHashinator interface
            VoltTable result1 = env.m_client.callProcedure("@AdHocSpForTest",
                    "SELECT * FROM BLAH WHERE IVAL = ?;", 2, 1).getResults()[0];
            //System.out.println(result1.toString());
            VoltTable result2 = env.m_client.callProcedure("@AdHocSpForTest",
                    "SELECT * FROM BLAH WHERE IVAL = ?;", 0, 1).getResults()[0];
            //System.out.println(result2.toString());
            assertEquals(1, result1.getRowCount() + result2.getRowCount());
            assertEquals(0, result1.getRowCount());
            assertEquals(1, result2.getRowCount());

            try {
                env.m_client.callProcedure("@AdHocSpForTest", "INSERT INTO BLAH VALUES (?, ?, ?);",
                        2, 0, 0, 0);
                fail("Badly partitioned insert failed to throw expected exception");
            } catch (Exception ignored) {}

            try {
                env.m_client.callProcedure("@AdHoc",
                        "SLEECT * FROOM NEEEW_OOORDERERER WHERE NONESUCH = ?;", 1);
                fail("Bad SQL failed to throw expected exception");
            } catch (Exception ignored) {}

            // try a huge bigint literal
            env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (?, ?, ?)",
                    974599638818488300L, "2011-06-24 10:30:26.123000", 5);
            modCount = env.m_client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (?, ?, ?)",
                    974599638818488301L, "2011-06-24 10:30:28.000000", 5).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = ?;",
                    974599638818488300L).getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE IVAL = ?;",
                    "974599638818488300").getResults()[0];
            //System.out.println(result.toString());
            assertEquals(1, result.getRowCount());
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL = ?;",
                    dateFormat.parse("2011-06-24 10:30:26.123")).getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL > ?;",
                    dateFormat.parse("2011-06-24 10:30:25.000")).getResults()[0];
            assertEquals(2, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc", "SELECT * FROM BLAH WHERE TVAL < ?;",
                    dateFormat.parse("2011-06-24 10:30:27.000000")).getResults()[0];
            //System.out.println(result.toString());
            // We inserted a 1,1,1 row way earlier
            assertEquals(2, result.getRowCount());

            // try something like the queries in ENG-1242
            try {
                env.m_client.callProcedure("@AdHoc",
                        "select * from blah; dfvsdfgvdf select * from blah WHERE IVAL = ?;", 1);
                fail("Bad SQL failed to throw expected exception");
            } catch (Exception ignored) {}
            env.m_client.callProcedure("@AdHoc", "select\n* from blah;");

            // try a decimal calculation (ENG-1093)
            modCount = env.m_client.callProcedure("@AdHoc",
                    "INSERT INTO BLAH VALUES (?, ?, ?);", 2, "2011-06-24 10:30:26", 1.12345).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE IVAL = ?;", 2).getResults()[0];
            assertEquals(1, result.getRowCount());
            //System.out.println(result.toString());
            result = env.m_client.callProcedure("@AdHoc",
                    "SELECT * FROM BLAH WHERE IVAL = ?;", "2").getResults()[0];
            //System.out.println(result.toString());
            assertEquals(1, result.getRowCount());
            // ENG-14210 more than 1025 parameters
            Object[] params = new Object[1201];
            // The first parameter is the query text.
            params[0] = "SELECT * FROM BLAH WHERE IVAL IN (" +
                    String.join(",", Collections.nCopies(1200, "?")) +
                    ");";
            for (int i = 1; i <= 1200; i++) {
                params[i] = i;
            }
            try {
                env.m_client.callProcedure("@AdHoc", params);
                fail("The AdHoc query with more than 1025 parameters should fail, but it did not.");
            } catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains("The statement's parameter count 1200 must not exceed the maximum 1025"));
            }
        } finally {
            env.tearDown();
            System.out.println("Ending testAdHocWithParams");
        }
    }

    @Test
    public void testAdHocQueryForStackOverFlowCondition() throws Exception {
        System.out.println("Starting testLongAdHocQuery");

        VoltDB.Configuration config = setUpSPDB();
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost", config.m_port);
        try {
            for (int len = 2000; len < 100000; len += 1000) {
                String sql = getQueryForLongQueryTable(len);
                m_client.callProcedure("@AdHoc", sql);
            }
            fail("Query was expected to generate stack overflow error");
        } catch (Exception exception) {
            final String expectedMsg = "Encountered stack overflow error. " +
                          "Try reducing the number of predicate expressions in the query.";
            assertTrue("Expected text \"" + expectedMsg + "\" did not appear in exception "
                    + "\"" + exception.getMessage() + "\"", exception.getMessage().contains(expectedMsg));
        } finally {
            if (m_client != null) {
                m_client.close();
                m_client = null;
            }
            localServer.shutdown();
            localServer.join();
        }
    }

    private void verifyIncorrectParameterMessage(TestEnv env, String adHocQuery, Object[] params) {
        int expected = 0;
        for (int i=0; i < adHocQuery.length(); i++ ) {
            if (adHocQuery.charAt(i) == '?' ) {
                expected++;
            }
        }
        String errorMsg = String.format("Incorrect number of parameters passed: expected %d, passed %d",
                expected, params.length);
        try {
            switch (params.length) {
            case 1:
                env.m_client.callProcedure("@AdHoc", adHocQuery, params[0]);
                break;
            case 2:
                env.m_client.callProcedure("@AdHoc", adHocQuery, params[0], params[1]);
                break;
            case 3:
                env.m_client.callProcedure("@AdHoc", adHocQuery, params[0], params[1], params[2]);
                break;
            case 4:
                env.m_client.callProcedure("@AdHoc", adHocQuery, params[0], params[1], params[2], params[3]);
                break;
            case 5:
                env.m_client.callProcedure("@AdHoc", adHocQuery, params[0], params[1], params[2], params[3], params[4]);
                break;
            default:
                // guard against other number of parameters tests
                fail("This test does not support other than 1-5 parameters!");
            }
            // expecting failure above
            fail();
        } catch(Exception ex) {
            assertTrue(ex.getMessage().contains(errorMsg));
        }
    }

    @Test
    public void testIndexFunction() {
        // Test that tuple insert that violates index functions should not succeed: ENG-16013
        final TestEnv env = new TestEnv("CREATE TABLE R3(i INTEGER NOT NULL, IPV6 VARCHAR(100));\n" +
                "CREATE INDEX DIDR1 ON R3(INET6_ATON(IPV6));",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            Stream.of(
                    Pair.of("INSERT INTO R3 VALUES(0, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(1, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(2, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(3, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(4, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(5, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(6, '2001:db8:85a3:0:0:8a2e:370:7334');", true),
                    Pair.of("INSERT INTO R3 VALUES(7, '2001:db8:85a3:0000:0000:8a2e:370:7334');", true),
                    Pair.of("UPDATE R3 SET IPV6 = '2001:db8:85a3:0000:0000:8a2e:370:7334' WHERE IPV6 = '2001:db8:85a3:0:0:8a2e:370:7334';", true),
                    Pair.of("UPDATE R3 SET IPV6 = 'foobar' WHERE IPV6 = '2001:db8:85a3:0:0:8a2e:370:7334';", true))
                    .forEachOrdered(queryAndStatus -> {
                        final String query = queryAndStatus.getFirst();
                        final boolean success = queryAndStatus.getSecond();
                        try {
                            assertEquals(String.format("Query \"%s\" should have %s", query,
                                    success ? "passed" : "failed"),
                                    success ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                                    env.m_client.callProcedure("@AdHoc", query).getStatus());
                        } catch (IOException | ProcCallException e) {
                            Assert.assertFalse(String.format("Query \"%s\" should have failed", query), success);
                        }
                    });
            verifyIncorrectParameterMessage(env, "SELECT COUNT(*) FROM R3;", new Integer[]{2});
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG17134() throws Exception {
        // Multiple table joins, including non-inner joins.
        final TestEnv env = new TestEnv(
                "CREATE TABLE PRPLISCLAIMINFO (REGISTNO varchar(22));\n" +
                        "CREATE TABLE PRPLISDISPATCHSURVEYLOG (REGISTNO varchar(22) NOT NULL);\n" +
                        "CREATE TABLE PRPLISMAININFO (REGISTNO varchar(22), MAKECODE varchar(10));\n" +
                        "CREATE TABLE PRPLISORGANIZATIONCACHE (COMCODE varchar(10));\n" +
                        "CREATE INDEX PRPLISORGANIZATIONCACHE_INDEX1 ON PRPLISORGANIZATIONCACHE (COMCODE);\n" +
                        "CREATE TABLE PRPLISRISKINFO (REGISTNO varchar(22), RISKLEVEL integer);\n",
                m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            assertEquals("Insertions failed?", ClientResponse.SUCCESS,
                    env.m_client.callProcedure("@AdHoc",
                            "INSERT INTO PRPLISCLAIMINFO VALUES('foo');\n" +
                                    "INSERT INTO PRPLISDISPATCHSURVEYLOG  VALUES('foo');\n" +
                                    "INSERT INTO PRPLISMAININFO VALUES('foo', 'baz');\n" +
                                    "INSERT INTO PRPLISORGANIZATIONCACHE VALUES('baz');\n" +
                                    "INSERT INTO PRPLISRISKINFO VALUES('foo', 1);\n")
                            .getStatus());
            final String query = "SELECT COUNT(*) FROM PRPLISCLAIMINFO a\n" +
                    "LEFT JOIN PRPLISDISPATCHSURVEYLOG b on a.registno = b.registno \n" +
                    "JOIN PRPLISMAININFO d on a.registno = d.registno \n" +
                    "JOIN PRPLISRISKINFO c on a.registno = c.registno \n" +
                    "LEFT JOIN PRPLISORGANIZATIONCACHE e on e.comcode = d.makecode \n" +
                    "WHERE C.RISKLEVEL =1;";
            final ClientResponse response = env.m_client.callProcedure("@AdHoc", query);
            assertEquals(String.format("Query \"%s\" should have succeeded", query),
                    ClientResponse.SUCCESS, response.getStatus());
            final VoltTable result = response.getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(String.format("Query \"%s\" should return 1", query),
                    1, result.getLong(0));
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG13840() throws Exception {
        final TestEnv env = new TestEnv(
                "CREATE TABLE R1(i int);\nCREATE TABLE R2(i int);",
                m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            env.m_client.callProcedure("@AdHoc",
                    "SELECT i FROM R2,\n" +
                            "   (SELECT MAX(i) FROM R2\n" +
                            "    WHERE i > (SELECT COUNT(R2.i) FROM R1\n" +
                            "               WHERE i IS NOT DISTINCT FROM R2.i\n" +
                            "               GROUP BY i ORDER BY i))\n" +
                            "    TA2;");
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testIndexViolationOnView() {
        // Test that tuple insert that violates index functions of a view should fail gracefully: ENG-15787
        final TestEnv env = new TestEnv("CREATE TABLE R3(i INTEGER NOT NULL, IPV6 VARCHAR(100));\n" +
                "CREATE VIEW VR3(IPV6) AS SELECT IPV6 FROM R3 GROUP BY IPV6;\n" +
                "CREATE INDEX DIDR1 ON VR3(INET6_ATON(IPV6));",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            Stream.of(
                    Pair.of("INSERT INTO R3 VALUES(0, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(1, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(2, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(3, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(4, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(5, ':c::40:b47');", false),
                    Pair.of("INSERT INTO R3 VALUES(6, '2001:db8:85a3:0:0:8a2e:370:7334');", true),
                    Pair.of("INSERT INTO R3 VALUES(6, '2001:db8:85a3:0000:0000:8a2e:370:7334');", true),
                    Pair.of("UPDATE R3 SET IPV6 = '2001:db8:85a3:0000:0000:8a2e:370:7334' WHERE IPV6 = '2001:db8:85a3:0:0:8a2e:370:7334';", true),
                    Pair.of("UPDATE R3 SET IPV6 = 'foobar' WHERE IPV6 = '2001:db8:85a3:0:0:8a2e:370:7334';", true))
                    .forEachOrdered(queryAndStatus -> {
                        final String query = queryAndStatus.getFirst();
                        final boolean success = queryAndStatus.getSecond();
                        try {
                            assertEquals(String.format("Query \"%s\" should have %s", query,
                                    success ? "passed" : "failed"),
                                    success ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                                    env.m_client.callProcedure("@AdHoc", query).getStatus());
                        } catch (IOException | ProcCallException e) {
                            Assert.assertFalse(String.format("Query \"%s\" should have failed", query), success);
                        }
                    });
            verifyIncorrectParameterMessage(env, "SELECT COUNT(*) FROM R3;", new Integer[]{2});
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testIndexViolationOnView2() {
        // Another sympton as ENG-15787 with the same root cause: ENG-15971
        final TestEnv env = new TestEnv("CREATE TABLE R3(VCHAR VARCHAR(15), id INTEGER NOT NULL);\n" +
                "CREATE VIEW VR3(VCHAR, ID) AS SELECT VCHAR, COUNT(*) FROM R3 GROUP BY VCHAR;\n" +
                "CREATE INDEX DIDX1 ON VR3(STR(ID, 10));",
                m_catalogJar, m_pathToDeployment, 2, 2, 1);
        try {
            env.setUp();
            Stream.of(
                    Pair.of("INSERT INTO R3 VALUES('abc2RD', 0);", false),
                    Pair.of("INSERT INTO R3 VALUES('abc2RD', 1);", false),
                    Pair.of("INSERT INTO R3 VALUES('abc2RD', 2);", false))
                    .forEachOrdered(queryAndStatus -> {
                        final String query = queryAndStatus.getFirst();
                        final boolean success = queryAndStatus.getSecond();
                        try {
                            assertEquals(String.format("Query \"%s\" should have %s", query,
                                    success ? "passed" : "failed"),
                                    success ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                                    env.m_client.callProcedure("@AdHoc", query).getStatus());
                        } catch (IOException | ProcCallException e) {
                            Assert.assertFalse(String.format("Query \"%s\" should have failed", query), success);
                        }
                    });
            verifyIncorrectParameterMessage(env, "SELECT COUNT(*) FROM R3;", new Integer[]{0});
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testAdHocWithParamsNegative() {
        final TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 2, 1);

        try {
            env.setUp();

            String adHocQuery;
            // no constants
            adHocQuery = "SELECT * FROM AAA WHERE a1 = ? and a2 = ?;";
            verifyIncorrectParameterMessage(env, adHocQuery, new Integer[]{1});
            verifyIncorrectParameterMessage(env, adHocQuery, new Integer[]{1, 1, 1});

            // mix question mark and constants
            adHocQuery = "SELECT * FROM AAA WHERE a1 = ? and a2 = 'a2' and a3 = 'a3';";
            verifyIncorrectParameterMessage(env, adHocQuery, new String[]{"a1", "a2"});
            verifyIncorrectParameterMessage(env, adHocQuery, new String[]{"a1", "a2", "a3"});

            // constants only
            adHocQuery = "SELECT * FROM AAA WHERE a1 = 'a1';";
            verifyIncorrectParameterMessage(env, adHocQuery, new String[]{"a2"});
            verifyIncorrectParameterMessage(env, adHocQuery, new String[]{"a2", "a3"});

            adHocQuery = "SELECT * FROM AAA WHERE a1 = 'a1' and a2 = 'a2';";
            verifyIncorrectParameterMessage(env, adHocQuery, new String[]{"a1"});
            verifyIncorrectParameterMessage(env, adHocQuery, new String[]{"a1", "a2"});

            //
            // test batch with extra parameter call
            //
            String errorMsg = AdHocNTBase.AdHocErrorResponseMessage;
            // test batch question mark parameter guards

            try {
                env.m_client.callProcedure("@AdHoc",
                        "SELECT * FROM AAA WHERE a1 = 'a1'; SELECT * FROM AAA WHERE a2 = 'a2';", "a2");
                fail();
            } catch (Exception ex) {
                assertEquals(errorMsg, ex.getMessage());
            }

            try {
                env.m_client.callProcedure("@AdHoc",
                        "SELECT * FROM AAA WHERE a1 = 'a1'; SELECT * FROM AAA WHERE a2 = ?;", "a2");
                fail();
            } catch (Exception ex) {
                assertEquals(errorMsg, ex.getMessage());
            }
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testAdHocBatches() throws Exception {
        final TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 1, 0);
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
            System.out.println("Running problem batch");
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
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testXopenSubSelectQueries() throws Exception {
        final TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            try {
                env.m_client.callProcedure("@AdHoc",
                        "UPDATE STAFF SET GRADE=10*STAFF.GRADE WHERE STAFF.EMPNUM NOT IN (SELECT WORKS.EMPNUM " +
                                " FROM WORKS WHERE STAFF.EMPNUM = WORKS.EMPNUM);");
            } catch (ProcCallException ignored) {
                fail("did fail on subquery In/Exists in UPDATE statement");
            }

            try {
                env.m_client.callProcedure("@AdHoc",
                        "SELECT 'ZZ', EMPNUM, EMPNAME, -99 FROM STAFF WHERE NOT EXISTS (SELECT * FROM WORKS " +
                                "WHERE WORKS.EMPNUM = STAFF.EMPNUM) ORDER BY EMPNUM;");
            } catch (Exception ignored) {
                fail("did fail on exists clause");
            }

            try {
                env.m_client.callProcedure("@AdHoc",
                        "SELECT STAFF.EMPNAME FROM STAFF WHERE STAFF.EMPNUM IN \n" +
                                "(SELECT WORKS.EMPNUM FROM WORKS WHERE WORKS.PNUM IN \n" +
                                "(SELECT PROJ.PNUM FROM PROJ WHERE PROJ.CITY='Tampa'));");
            } catch (Exception ignored) {
                fail("did fail on subquery");
            }

            try {
                env.m_client.callProcedure("@AdHoc",
                        "SELECT PNAME FROM PROJ WHERE 'Tampa' NOT BETWEEN CITY AND 'Vienna' AND PNUM > 'P2';");
            } catch (ProcCallException ignored) {
                fail("failed on static clause");
            }
            try {
                env.m_client.callProcedure("@AdHoc", "ROLLBACK;");
                fail("did not fail on invalid SQL verb");
            } catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("this type of sql statement is not supported") > 0);
            }
        } finally {
            env.tearDown();
        }
    }

    @Test
    // ENG-4151 a bad string timestamp insert causes various errors when a constraint
    // violation occurs because the bad timestamp column is serialized as a string
    // instead of a timestamp. It was mis-handling the timestamp datatype during planning.
    // Testing with ad hoc because that's how it was discovered.
    public void testTimestampInsert() throws Exception {
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 1, 1, 0);
        try {
            env.setUp();
            // bad timestamp should result in a clean compiler error.
            try {
                String sql = "INSERT INTO TS_CONSTRAINT_EXCEPTION VALUES ('aaa','{}');";
                env.m_client.callProcedure("@AdHoc", sql).getResults();
                fail("Compilation should have failed.");
            } catch(ProcCallException e) {
                assertTrue(e.getMessage().contains("invalid format for a constant timestamp value"));
            }
            String sql = String.format("INSERT INTO TS_CONSTRAINT_EXCEPTION VALUES ('%s','{}');",
                    new TimestampType().toString());
            VoltTable modCount = env.m_client.callProcedure("@AdHoc", sql).getResults()[0];
            assertEquals(1, modCount.getRowCount());
            assertEquals(1, modCount.asScalarLong());
            // double insert should cause a clean constraint violation, not a crash
            try {
                modCount = env.m_client.callProcedure("@AdHoc", sql).getResults()[0];
                assertEquals(1, modCount.getRowCount());
                assertEquals(1, modCount.asScalarLong());
            } catch(ProcCallException e) {
                assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            }
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG15117() throws Exception {
        final String ddl = "CREATE TABLE SJYH_DENGLU2 (\n" +
                "DL_USER_ID varchar(22) DEFAULT '' NOT NULL,\n" +
                "DL_ACCOUNT_ID varchar(32) DEFAULT '' NOT NULL,\n" +
                "DL_REGISTER_TIME varchar(8) DEFAULT '' NOT NULL,\n" +
                "DL_REGISTER_DATE varchar(8) DEFAULT '' NOT NULL,\n" +
                "DL_REGISTER_IP varchar(18) DEFAULT '' NOT NULL,\n" +
                "DL_REGISTER_ADDR varchar(15) DEFAULT '' NOT NULL,\n" +
                "DL_REGISTER_IP_CITY varchar(32) DEFAULT '' NOT NULL,\n" +
                "DL_ACCOUNT_TYPE varchar(2) DEFAULT '' NOT NULL,\n" +
                "DL_PHONE_NO varchar(15) DEFAULT '' NOT NULL,\n" +
                "DL_INDUSTRY_TYPE varchar(8) DEFAULT '' NOT NULL,\n" +
                "SCORE float,\n" +
                "TRXSTATUS varchar(1) DEFAULT '1',\n" +
                "KEY_INFO varchar(256)\n" +
                ");\n" +
                "\n" +
                "PARTITION TABLE SJYH_DENGLU2 ON COLUMN DL_USER_ID;";

        final TestEnv env = new TestEnv(ddl, m_catalogJar, m_pathToDeployment, 2, 1, 0);

        try {
            env.setUp();
            Batcher batcher = new Batcher(env);

            // insert some data
            batcher.add("insert into SJYH_DENGLU2 values ('113001','6600003001','22:49','','14.105.100.124','aaa','1000003002','1','18423485412','000002',0,'1','1')", 1);
            batcher.add("insert into SJYH_DENGLU2 values ('113001','6600003001','22:49','','14.105.100.124','aaa','1000003001','1','18423485412','000002',0,'1','1')", 1);
            batcher.add("insert into SJYH_DENGLU2 values ('113001','6600003001','22:49','','14.105.100.124','aaa','1000003000','1','18423485412','000002',0,'1','1')", 1);
            batcher.run();

            // run the same subselect query for multiple times, we should always get the same&correct answer
            Stream.generate(() -> "select * from (select count(*) from SJYH_DENGLU2 where DL_USER_ID = '113001' ) as result;")
                    .limit(5)
                    .forEachOrdered(stmt -> {
                        try {
                            final ClientResponse cr = env.m_client.callProcedure("@AdHoc", stmt);
                            assertContentOfTable(new Object[][]{{3}}, cr.getResults()[0]);
                        } catch (IOException | ProcCallException e) {
                            fail("Query \"" + stmt + "\" should have worked fine");
                        }
                    });
        } finally {
            env.tearDown();
        }
    }
    @Test
    public void testENG15719PartialIndex() throws Exception {
        testENG15719PartialIndex(false);
        testENG15719PartialIndex(true);
    }

    private void testENG15719PartialIndex(boolean partitioned) throws Exception {
        final TestEnv env = new TestEnv("CREATE TABLE foo(i int not null, j int);\n" +
                (partitioned ? "partition table foo on column i;\n" : "") +
                "create index partial_index on foo(i) where abs(i) > 0;\n",
                m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            final Batcher batcher = new Batcher(env);
            final int nrep = 3; // how many repetitions each tuple gets inserted
            IntStream.range(0, 5).forEach(n -> batcher.add(nrep,
                    String.format("INSERT INTO foo VALUES(%d, %d);\n", n, n + 1), 1));
            batcher.run();
            Stream.of(Pair.of("true", IntStream.of(nrep * 5, 5, 0, 4, nrep * 10)),
                    Pair.of("abs(i) > 0", IntStream.of(nrep * 4, 4, 1, 4, nrep * 10)),
                    Pair.of("abs(i) > 0 AND j > 3", IntStream.of(nrep * 2, 2, 3, 4, nrep * 7)))
                    .forEach(pair -> {
                        final List<String> sqls = Stream.of("COUNT(*)", "COUNT (distinct i)", "MIN(i)", "MAX(i)", "SUM(i)")
                                .map(aggregate -> String.format("SELECT %s FROM foo WHERE %s;", aggregate, pair.getFirst()))
                                .collect(Collectors.toList());
                        final List<Integer> expected = pair.getSecond().boxed().collect(Collectors.toList());
                        assertEquals("Query number/result count mismatch", sqls.size(), expected.size());
                        for(int index = 0; index < sqls.size(); ++index) {
                            final String sql = sqls.get(index);
                            final int expectedResult = expected.get(index);
                            try {
                                assertContentOfTable(new Object[][]{{expectedResult}},
                                        env.m_client.callProcedure("@AdHoc", sql).getResults()[0]);
                            } catch (AssertionError e) {
                                fail(String.format("Assertion error on %s: %s", sql, e.getMessage()));
                            } catch (Exception e) {
                                fail(String.format("Query %s should have worked fine", sql));
                            }
                        }
                    });
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG15836() throws Exception {
        final String ddl = "CREATE TABLE ENG15836 (\n" +
                "PROD_KEY integer NOT NULL,\n" +
                "SDATE INTEGER\n" +
                ");\n" +
                "\n" +
                "CREATE INDEX ENG15836_IDX ON ENG15836 (PROD_KEY, SDATE);";

        final TestEnv env = new TestEnv(ddl,
                m_catalogJar, m_pathToDeployment, 2, 1, 0);

        try {
            env.setUp();
            Batcher batcher = new Batcher(env);

            // insert some data
            batcher.add("INSERT INTO ENG15836 ( PROD_KEY,  SDATE) VALUES( 886, 1);", 1);
            batcher.add("INSERT INTO ENG15836 ( PROD_KEY,  SDATE) VALUES( 886, 1);", 1);
            batcher.add("INSERT INTO ENG15836 ( PROD_KEY,  SDATE) VALUES( 887, 2);", 1);
            batcher.add("INSERT INTO ENG15836 ( PROD_KEY,  SDATE) VALUES( 887, 2);", 1);
            batcher.add("INSERT INTO ENG15836 ( PROD_KEY,  SDATE) VALUES( 890, 4);", 1);
            batcher.add("INSERT INTO ENG15836 ( PROD_KEY,  SDATE) VALUES( 890, 4);", 1);
            batcher.run();

            final ClientResponse cr = env.m_client.callProcedure("@AdHoc",
                    "SELECT COUNT(*) FROM ENG15836 apb WHERE PROD_KEY IN (886, 887, 890) AND apb.sdate <= 100;");

            assertContentOfTable(new Object[][]{{6}}, cr.getResults()[0]);
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG20068() throws Exception {
        /**
         * The bug using the query would use the only index in the executor, but such
         * executor would ignore the "initial_expression" that sets cursor at correct
         * starting position before further processing (post-filtering, etc). Before
         * this fix, the query would return all 14 rows because of lack of "initial_expression"
         * evaluation.
         */
        final String ddl =
            "CREATE TABLE ENBA (\n" +
            "RULE_TYPE varchar(25) NOT NULL,\n" +
            "RULE_TYPE_ID integer NOT NULL,\n" +
            "PARM1 varchar(300),\n" +
            "PARM2 varchar(300),\n" +
            "PARM3 varchar(300),\n" +
            ");\n" +
            "CREATE INDEX I_ENBA_I1 ON ENBA(RULE_TYPE, RULE_TYPE_ID, PARM1, PARM2);";
        final TestEnv env = new TestEnv(ddl,
                m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            Batcher batcher = new Batcher(env);
            Stream.of("'450887680', '524288000'",
                    "'419430400', '450887680'",
                    "'4194340', '62914560'",
                    "'314572800', '41943400'",
                    "'31457280', '4194340'",
                    "'262144000', '314572800'",
                    "'209715200', '262144000'",
                    "'18774368', '26214400'",
                    "'157286400', '209715200'",
                    "'1503238554', '1717986918'",
                    "'125829120', '157286400'",
                    "'12582912', '15728640'",
                    "'104857600', '125829120'",
                    "'10485760', '12582912'")
                .forEach(entry ->
                        batcher.add(String.format("INSERT INTO ENBA VALUES(" +
                                "'S_RED_LT_ML_TGT_BAL', 1, 'J4U Data', %s);\n",
                                entry), 1));
            batcher.run();
            final ClientResponse cr = env.m_client.callProcedure("@AdHoc",
                    "SELECT COUNT(*) FROM ENBA WHERE RULE_TYPE = 'S_RED_LT_ML_TGT_BAL'\n" +
                    "AND RULE_TYPE_ID = 1 and PARM1 = 'J4U Data'\n" +
                    "AND 9054208 <= CAST(PARM3 as bigint) AND 9054208 > CAST(PARM2 as bigint);");
            assertContentOfTable(new Object[][]{{1}}, cr.getResults()[0]);
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG20394() throws Exception {
        /**
         * The bug using the query would use the only index in the executor, but such
         * executor would ignore the "initial_expression" that sets cursor at correct
         * starting position before further processing (post-filtering, etc). Before
         * this fix, the query would return all 14 rows because of lack of "initial_expression"
         * evaluation.
         */
        final String ddl =
            "CREATE TABLE ENBA (\n" +
            "RULE_TYPE varchar(25) NOT NULL,\n" +
            "RULE_TYPE_ID integer NOT NULL,\n" +
            "PARM1 varchar(300),\n" +
            "PARM2 varchar(300),\n" +
            "PARM4 varchar(300),\n" +
            "PARM5 varchar(300),\n" +
            ");\n" +
            "CREATE INDEX I_ENBA_I1 ON ENBA(RULE_TYPE, RULE_TYPE_ID, PARM1, PARM2);";
        final TestEnv env = new TestEnv(ddl,
                m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            Batcher batcher = new Batcher(env);
            Stream.of("1, '31', '45', 'zzzz', 'abcd'",
                    "1, '46', '60', 'dcba', 'zzzz'",
                    "0, 'Voldemort', 'yyyy', 'zzzz', 'abcd'")
                .forEach(entry ->
                        batcher.add(String.format("INSERT INTO ENBA VALUES('S_SM_LATCH_ACT', %s);\n",
                                entry), 1));
            batcher.run();
            final ClientResponse cr = env.m_client.callProcedure("@AdHoc",
                    "SELECT PARM1 FROM ENBA where RULE_TYPE = 'S_SM_LATCH_ACT' AND RULE_TYPE_ID = 1 \n" +
                    "AND cast(PARM1 AS bigint) <= 34;");
            assertContentOfTable(new Object[][]{{"31"}}, cr.getResults()[0]);
        } finally {
            env.tearDown();
        }
    }

    @Test
    public void testENG20461() throws Exception {
        /**
         * The bug using the query would silently drop the CAST operation in the SELECT
         * predicate that compares a VARCHAR column casted to INTEGER with a integer constant,
         * and by doing so the comparison would be executed as comparing column values against
         * stringified integer constant.
         */
        final String ddl =
            "CREATE TABLE ENBA (\n" +
            "RULE_TYPE varchar(25) NOT NULL,\n" +
            "RULE_TYPE_ID integer NOT NULL,\n" +
            "PARM1 varchar(300),\n" +
            "PARM2 varchar(300));\n" +
            "CREATE INDEX I_ENBA_I1 ON ENBA(RULE_TYPE, RULE_TYPE_ID, PARM1, PARM2);";
        final TestEnv env = new TestEnv(ddl, m_catalogJar, m_pathToDeployment, 2, 1, 0);
        try {
            env.setUp();
            Batcher batcher = new Batcher(env);
            Stream.of("'31', '45'", "'46', '60'", "'8', '15'")
                .forEach(entry ->
                        batcher.add(String.format("INSERT INTO ENBA VALUES('S_SM_LATCH_ACT', 1, %s);\n",
                                entry), 1));
            batcher.run();
            // Both queries select/count 1st and 3rd rows
            ClientResponse cr = env.m_client.callProcedure("@AdHoc",
                    "SELECT COUNT(*) FROM ENBA where RULE_TYPE = 'S_SM_LATCH_ACT' AND RULE_TYPE_ID = 1 \n" +
                    "AND cast(PARM1 AS bigint) <= 34;");
            assertContentOfTable(new Object[][]{{2}}, cr.getResults()[0]);
            cr = env.m_client.callProcedure("@AdHoc",
                    "SELECT COUNT(*) FROM ENBA where RULE_TYPE = 'S_SM_LATCH_ACT' AND RULE_TYPE_ID = 1 \n" +
                    "AND cast(cast(PARM1 AS float) AS bigint) <= 34;");
            assertContentOfTable(new Object[][]{{2}}, cr.getResults()[0]);
        } finally {
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

        Batcher(final TestEnv env) {
            m_env = env;
        }

        void add(String query, Integer expectedCount) {
            m_queries.add(query);
            if (expectedCount != null) {
                m_expectedCounts.add(expectedCount);
            }
        }

        void add(int nrep, String query, Integer expectedCount) {
            for(int index = 0; index < nrep; ++index) {
                add(query, expectedCount);
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
                                if (expectedCount != results[i].getRowCount()) {
                                    System.out.println("Mismatched result from statement " + i +
                                            " expecting " + expectedCount + " rows and getting:\n" + results[i]);
                                }
                                assertEquals(String.format("%s (row count):",query),
                                             expectedCount, results[i].getRowCount());
                            }
                            i++;
                        }
                    }
                }
            } catch(ProcCallException e) {
                assertTrue("Unexpected exception for batch: " + e.getMessage(), expectException);
            } finally {
                m_queries.clear();
                m_expectedCounts.clear();
            }
        }
    }

    /**
     * Test environment with configured schema and server.
     */
    public static class TestEnv extends RegressionSuite.RegresssionEnv {
        TestEnv(String ddl, String pathToCatalog, String pathToDeployment,
                int siteCount, int hostCount, int kFactor) {
            super(ddl, pathToCatalog, pathToDeployment, siteCount, hostCount,kFactor, m_debug);
        }
        TestEnv(String pathToCatalog, String pathToDeployment,
                int siteCount, int hostCount, int kFactor) {
            super("create table BLAH (" +
                            "IVAL bigint default 0 not null, " +
                            "TVAL timestamp default null," +
                            "DVAL decimal default null," +
                            "PRIMARY KEY(IVAL));\n" +
                            "PARTITION TABLE BLAH ON COLUMN IVAL;\n" +
                            "\n" +
                            "CREATE TABLE AAA (A1 VARCHAR(2), A2 VARCHAR(2), A3 VARCHAR(2));\n" +
                            "CREATE TABLE BBB (B1 VARCHAR(2), B2 VARCHAR(2), B3 VARCHAR(2) NOT NULL UNIQUE);\n" +
                            "CREATE TABLE CCC (C1 VARCHAR(2), C2 VARCHAR(2), C3 VARCHAR(2));\n" +
                            "\n" +
                            "CREATE TABLE CHAR_TEST (COL1 VARCHAR(254));\n" +
                            "CREATE TABLE INT_TEST (COL1 INTEGER);\n" +
                            "CREATE TABLE SMALL_TEST (COL1 SMALLINT);\n" +
                            "CREATE TABLE REAL_TEST (REF VARCHAR(1),COL1 REAL);\n" +
                            "CREATE TABLE REAL3_TEST (COL1 REAL,COL2 REAL,COL3 REAL);\n" +
                            "CREATE TABLE DOUB_TEST (REF VARCHAR(1),COL1 FLOAT);\n" +
                            "CREATE TABLE DOUB3_TEST (COL1 FLOAT,COL2 FLOAT\n" +
                            "   PRECISION,COL3 FLOAT);\n" +
                            "\n" +
                            "-- Users may provide an explicit precision for FLOAT_TEST.COL1\n" +
                            "\n" +
                            "CREATE TABLE FLOAT_TEST (REF VARCHAR(1),COL1 FLOAT);\n" +
                            "\n" +
                            "CREATE TABLE INDEXLIMIT(COL1 VARCHAR(2), COL2 VARCHAR(2),\n" +
                            "   COL3 VARCHAR(2), COL4 VARCHAR(2), COL5 VARCHAR(2),\n" +
                            "   COL6 VARCHAR(2), COL7 VARCHAR(2));\n" +
                            "\n" +
                            "CREATE TABLE WIDETABLE (WIDE VARCHAR(118));\n" +
                            "CREATE TABLE WIDETAB (WIDE1 VARCHAR(38), WIDE2 VARCHAR(38), WIDE3 VARCHAR(38));\n" +
                            "\n" +
                            "CREATE TABLE TEST_TRUNC (TEST_STRING VARCHAR (6));\n" +
                            "\n" +
                            "CREATE TABLE WARNING(TESTCHAR VARCHAR(6), TESTINT INTEGER);\n" +
                            "\n" +
                            "CREATE TABLE TV (dec3 DECIMAL(3), dec1514 DECIMAL(15,14),\n" +
                            "                 dec150 DECIMAL(15,0), dec1515 DECIMAL(15,15));\n" +
                            "\n" +
                            "CREATE TABLE TU (smint SMALLINT, dec1514 DECIMAL(15,14),\n" +
                            "                 integr INTEGER, dec1515 DECIMAL(15,15));\n" +
                            "\n" +
                            "CREATE TABLE STAFF\n" +
                            "  (EMPNUM   VARCHAR(3) NOT NULL UNIQUE,\n" +
                            "   EMPNAME  VARCHAR(20),\n" +
                            "   GRADE    DECIMAL(4),\n" +
                            "   CITY     VARCHAR(15));\n" +
                            "\n" +
                            "CREATE TABLE PROJ\n" +
                            "  (PNUM     VARCHAR(3) NOT NULL UNIQUE,\n" +
                            "   PNAME    VARCHAR(20),\n" +
                            "   PTYPE    VARCHAR(6),\n" +
                            "   BUDGET   DECIMAL(9),\n" +
                            "   CITY     VARCHAR(15));\n" +
                            "\n" +
                            "CREATE TABLE WORKS\n" +
                            "  (EMPNUM   VARCHAR(3) NOT NULL,\n" +
                            "   PNUM     VARCHAR(3) NOT NULL,\n" +
                            "   HOURS    DECIMAL(5),\n" +
                            "   UNIQUE(EMPNUM,PNUM));\n" +
                            "\n" +
                            "CREATE TABLE INTS\n" +
                            "  (INT1      SMALLINT NOT NULL,\n" +
                            "   INT2      SMALLINT NOT NULL);\n" +
                            "CREATE TABLE VOTES\n" +
                            "  (PHONE_NUMBER BIGINT NOT NULL,\n" +
                            "   STATE     VARCHAR(2) NOT NULL,\n" +
                            "   CONTESTANT_NUMBER  INTEGER NOT NULL);\n" +
                            "\n" +
                            "CREATE PROCEDURE TestProcedure AS INSERT INTO AAA VALUES(?,?,?);\n" +
                            "CREATE PROCEDURE Insert AS INSERT into BLAH values (?, ?, ?);\n" +
                            "CREATE PROCEDURE InsertWithDate AS \n" +
                            "  INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);\n" +
                            "\n" +
                            "CREATE TABLE TS_CONSTRAINT_EXCEPTION\n" +
                            "  (TS TIMESTAMP UNIQUE NOT NULL,\n" +
                            "   COL1 VARCHAR(2048)); \n" +
                            "",
                    pathToCatalog, pathToDeployment, siteCount, hostCount, kFactor, m_debug);
        }
    }

}
