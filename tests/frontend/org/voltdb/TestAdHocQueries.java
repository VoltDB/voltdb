/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;

public class TestAdHocQueries extends AdHocQueryTester {

    Client m_client;
    private final static boolean m_debug = false;

    @Test
    public void testProcedureAdhoc() throws Exception {
        VoltDB.Configuration config = setUpSPDB();
        ServerThread localServer = new ServerThread(config);

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
            VoltTable results[] = m_client.callProcedure(
                    "executeSQLSP",
                    23,
                    "select * from PARTED1").getResults();
            assertTrue(
                    results[0].advanceRow());
            assertTrue(results[1].advanceRow());

            results = m_client.callProcedure(
                    "executeSQLMP",
                    23,
                    "       select * from PARTED1").getResults();
            assertTrue(
                    results[0].advanceRow());
            assertTrue(results[1].advanceRow());

            //
            // Validate that doing an insert from a RO procedure fails
            //
            try {
                m_client.callProcedure("executeSQLSP", 24, "insert into parted1 values (24,5)");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException e) {}

            try {
                m_client.callProcedure("executeSQLMP", 24, "insert into parted1 values (24,5)");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException e) {}

            //
            // Validate one sql statement per
            //
            try {
                m_client.callProcedure("executeSQLSP", 24, "insert into parted1 values (24,5); select * from parted1;");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException e) {}

            try {
                m_client.callProcedure("executeSQLSP", 24, "drop table parted1");
                fail("Procedure call should not have succeded");
            } catch (ProcCallException e) {}


            //
            // Validate that an insert does work from a write procedure
            //
            m_client.callProcedure("executeSQLSPWRITE", 24, "insert into parted1 values (24, 4);");
            m_client.callProcedure("executeSQLMPWRITE", 25, "insert into parted1 values (25, 5);");

            //
            // Query the inserts and all the rest do it once for singe and once for multi
            //
            results = m_client.callProcedure("executeSQLMP", 24, "select * from parted1 order by partval").getResults();

            assertEquals( 3, results[0].getRowCount());
            for (int ii = 3; ii < 6; ii++) {
                assertTrue(results[0].advanceRow());
                assertEquals(20 + ii, results[0].getLong(0));
                assertEquals(ii, results[0].getLong(1));
            }

            //Output from the first preplanned statement
            assertEquals( 3, results[1].getRowCount());
            assertTrue(results[1].advanceRow());
            assertEquals( 23, results[1].getLong(0));
            assertEquals( 3, results[1].getLong(1));

            //Output from the second adhoc statement
            assertEquals( 1, results[2].getRowCount());
            assertTrue(results[2].advanceRow());
            assertEquals( 24, results[2].getLong(0));
            assertEquals( 4, results[2].getLong(1));

            //Output from the second preplanned statement
            assertEquals( 3, results[3].getRowCount());
            assertTrue(results[3].advanceRow());
            assertEquals( 23, results[3].getLong(0));
            assertEquals( 3, results[3].getLong(1));


            results = m_client.callProcedure("executeSQLSP", 24, "select * from parted1 order by partval").getResults();

            assertEquals( 1, results[0].getRowCount());
            assertTrue(results[0].advanceRow());
            assertEquals(24, results[0].getLong(0));
            assertEquals( 4, results[0].getLong(1));

            //Output from the first preplanned statement
            assertEquals( 1, results[1].getRowCount());
            assertTrue(results[1].advanceRow());
            assertEquals( 24, results[1].getLong(0));
            assertEquals( 4, results[1].getLong(1));

            //Output from the second adhoc statement
            assertEquals( 1, results[2].getRowCount());
            assertTrue(results[2].advanceRow());
            assertEquals( 24, results[2].getLong(0));
            assertEquals( 4, results[2].getLong(1));

            //Output from the second preplanned statement
            assertEquals( 1, results[3].getRowCount());
            assertTrue(results[3].advanceRow());
            assertEquals( 24, results[3].getLong(0));
            assertEquals( 4, results[3].getLong(1));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
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
        int spResult = result.getRowCount();
        System.out.println(result.toString());
        if (validatingSPresult != 0) {
            assertEquals(expected, spPartialSoFar + spResult);
        }

        return spResult;
    }

    String m_catalogJar = "adhoc.jar";
    String m_pathToCatalog = Configuration.getPathToCatalogForTest(m_catalogJar);
    String m_pathToDeployment = Configuration.getPathToCatalogForTest("adhoc.xml");

    @Test
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

    @Test
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

    @Test
    public void testXopenSubSelectQueries() throws Exception {
        TestEnv env = new TestEnv(m_catalogJar, m_pathToDeployment, 2, 1, 0);
        String adHocQuery;
        try {
            env.setUp();

            adHocQuery = "  UPDATE STAFF \n" +
                    "          SET GRADE=10*STAFF.GRADE \n" +
                    "          WHERE STAFF.EMPNUM NOT IN \n" +
                    "                (SELECT WORKS.EMPNUM \n" +
                    "                      FROM WORKS \n" +
                    "                      WHERE STAFF.EMPNUM = WORKS.EMPNUM);";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on subquery");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("not support subqueries") > 0);
            }
            adHocQuery = "     SELECT 'ZZ', EMPNUM, EMPNAME, -99 \n" +
                    "           FROM STAFF \n" +
                    "           WHERE NOT EXISTS (SELECT * FROM WORKS \n" +
                    "                WHERE WORKS.EMPNUM = STAFF.EMPNUM) \n" +
                    "                ORDER BY EMPNUM;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on exists clause");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("not support subqueries") > 0);
            }
            adHocQuery = "   SELECT STAFF.EMPNAME \n" +
                    "          FROM STAFF \n" +
                    "          WHERE STAFF.EMPNUM IN \n" +
                    "                  (SELECT WORKS.EMPNUM \n" +
                    "                        FROM WORKS \n" +
                    "                        WHERE WORKS.PNUM IN \n" +
                    "                              (SELECT PROJ.PNUM \n" +
                    "                                    FROM PROJ \n" +
                    "                                    WHERE PROJ.CITY='Tampa')); \n" +
                    "";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on subquery");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("not support subqueries") > 0);
            }
            adHocQuery = "SELECT FIRST1.EMPNUM, SECOND2.EMPNUM \n" +
                    "          FROM STAFF FIRST1, STAFF SECOND2 \n" +
                    "          WHERE FIRST1.CITY = SECOND2.CITY \n" +
                    "          AND FIRST1.EMPNUM < SECOND2.EMPNUM;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on selfjoin");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("not support self joins") > 0);
            }
            adHocQuery = "SELECT PNAME \n" +
                    "         FROM PROJ \n" +
                    "         WHERE 'Tampa' NOT BETWEEN CITY AND 'Vienna' \n" +
                    "                           AND PNUM > 'P2';";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on static clause");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("does not support WHERE clauses containing only constants") > 0);
            }
            adHocQuery = "CREATE TABLE ICAST2 (C1 INT, C2 FLOAT);";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on invalid ");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("Unsupported SQL verb in statement") > 0);
            }
            adHocQuery = "CREATE INDEX IDX_PROJ_PNAME ON PROJ(PNAME);";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on invalid SQL verb");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("Unsupported SQL verb in statement") > 0);
            }
            adHocQuery = "ROLLBACK;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on invalid SQL verb");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("Unsupported SQL verb in statement") > 0);
            }
            adHocQuery = "DROP TABLE PROJ;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on invalid SQL verb");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("Unsupported SQL verb in statement") > 0);
            }
            adHocQuery = "PARTITION TABLE PROJ ON COLUMN PNUM;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail with unexpected token");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("unexpected token: PARTITION") > 0);
            }
            adHocQuery = "CREATE PROCEDURE AS SELECT 1 FROM PROJ;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail with unexpected token");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("unexpected token: AS") > 0);
            }
            adHocQuery = "CREATE PROCEDURE FROM CLASS bar.Foo;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail with unexpected token");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("unexpected token: FROM") > 0);
            }
            adHocQuery = "SELECT PNUM \n" +
                    "          FROM WORKS \n" +
                    "          WHERE PNUM > 'P1' \n" +
                    "          GROUP BY PNUM \n" +
                    "          HAVING COUNT(*) > 1;";
            try {
                env.m_client.callProcedure("@AdHoc", adHocQuery);
                fail("did not fail on having clause");
            }
            catch (ProcCallException pcex) {
                assertTrue(pcex.getMessage().indexOf("not support the HAVING clause") > 0);
            }
        }
        finally {
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
            }
            catch(ProcCallException e) {
                assertTrue(e.getMessage().contains("Error compiling"));
            }
            // double insert should cause a clean constraint violation, not a crash
            try {
                String sql = String.format("INSERT INTO TS_CONSTRAINT_EXCEPTION VALUES ('%s','{}');",
                                           new TimestampType().toString());
                VoltTable modCount = env.m_client.callProcedure("@AdHoc", sql).getResults()[0];
                assertEquals(1, modCount.getRowCount());
                assertEquals(1, modCount.asScalarLong());
                modCount = env.m_client.callProcedure("@AdHoc", sql).getResults()[0];
                assertEquals(1, modCount.getRowCount());
                assertEquals(1, modCount.asScalarLong());
            }
            catch(ProcCallException e) {
                assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            }
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
                                           "CREATE PROCEDURE TestProcedure AS INSERT INTO AAA VALUES(?,?,?);\n" +
                                           "CREATE PROCEDURE Insert AS INSERT into BLAH values (?, ?, ?);\n" +
                                           "CREATE PROCEDURE InsertWithDate AS \n" +
                                           "  INSERT INTO BLAH VALUES (974599638818488300, '2011-06-24 10:30:26.002', 5);\n" +
                                           "\n" +
                                           "CREATE TABLE TS_CONSTRAINT_EXCEPTION\n" +
                                           "  (TS TIMESTAMP UNIQUE NOT NULL,\n" +
                                           "   COL1 VARCHAR(2048)); \n" +
                                           "");

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
                                         m_debug);
            m_cluster.setHasLocalServer(false);
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
    }

}
