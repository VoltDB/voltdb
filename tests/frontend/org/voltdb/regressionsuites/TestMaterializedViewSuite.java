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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AddPerson;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AddThing;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AggAges;
import org.voltdb_testprocs.regressionsuites.matviewprocs.AggThings;
import org.voltdb_testprocs.regressionsuites.matviewprocs.DeletePerson;
import org.voltdb_testprocs.regressionsuites.matviewprocs.Eng798Insert;
import org.voltdb_testprocs.regressionsuites.matviewprocs.OverflowTest;
import org.voltdb_testprocs.regressionsuites.matviewprocs.SelectAllPeople;
import org.voltdb_testprocs.regressionsuites.matviewprocs.TruncateMatViewDataMP;
import org.voltdb_testprocs.regressionsuites.matviewprocs.TruncatePeople;
import org.voltdb_testprocs.regressionsuites.matviewprocs.TruncateTables;
import org.voltdb_testprocs.regressionsuites.matviewprocs.UpdatePerson;

import com.google_voltpatches.common.collect.Lists;

import junit.framework.Test;

public class TestMaterializedViewSuite extends RegressionSuite {

    // Constants to control whether to abort a procedure invocation with explicit sabotage
    // or to allow it to run normally.
    private static final int SABOTAGE = 2;
    private static final int NORMALLY = 0;

    private static final int[] yesAndNo = new int[]{1, 0};
    private static final int[] never = new int[]{0};

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        AddPerson.class, DeletePerson.class, UpdatePerson.class, AggAges.class,
        SelectAllPeople.class, AggThings.class, AddThing.class, OverflowTest.class,
        Eng798Insert.class, TruncateMatViewDataMP.class,
        TruncateTables.class, TruncatePeople.class
    };

    // For comparing tables with FLOAT columns
    private static final double EPSILON = 0.000001;

    public TestMaterializedViewSuite(String name) {
        super(name);
    }

    private void truncateBeforeTest(Client client) {
        // TODO Auto-generated method stub
        VoltTable[] results = null;
        try {
            results = client.callProcedure("TruncateMatViewDataMP").getResults();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
            fail("Unexpected:" + e);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Unexpected:" + e);
        } catch (ProcCallException e) {
            e.printStackTrace();
            fail("Unexpected:" + e);
        }
        int nStatement = 0;
        for (VoltTable countTable : results) {
            ++nStatement;
            try {
                long count = countTable.asScalarLong();
                assertEquals("COUNT statement " + nStatement + "/" +
                results.length + " should have found no undeleted rows.", 0, count);
            }
            catch (Exception exc) {
                System.out.println("validation query " + nStatement + " got a bad result: " + exc);
                throw exc;
            }
        }
    }

    private void assertAggNoGroupBy(Client client, String tableName, String... values) throws IOException, ProcCallException
    {
        assertTrue(values != null);
        VoltTable[] results = client.callProcedure("@AdHoc", "SELECT * FROM " + tableName).getResults();

        assertTrue(results != null);
        assertEquals(1, results.length);
        VoltTable t = results[0];
        assertTrue(values.length <= t.getColumnCount());
        assertEquals(1, t.getRowCount());
        t.advanceRow();
        for (int i=0; i<values.length; ++i) {
            // if it's integer
            if (t.getColumnType(i) == VoltType.TINYINT ||
                t.getColumnType(i) == VoltType.SMALLINT ||
                t.getColumnType(i) == VoltType.INTEGER ||
                t.getColumnType(i) == VoltType.BIGINT) {
                long value = t.getLong(i);
                if (values[i].equals("null")) {
                    assertTrue(t.wasNull());
                }
                else {
                    assertEquals(Long.parseLong(values[i]), value);
                }
            }
            else if (t.getColumnType(i) == VoltType.FLOAT) {
                double value = t.getDouble(i);
                if (values[i].equals("null")) {
                    assertTrue(t.wasNull());
                }
                else {
                    assertEquals(Double.parseDouble(values[i]), value);
                }
            }
        }
    }

    private void subtestENG7872SinglePartition() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "0");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "0");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "0", "null");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "0", "null", "null");

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 4L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 5L, 31L, null, null, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "5");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "2");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "2", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "3", "900.0", "5");

        results = client.callProcedure("DeletePerson", 1, 2L, NORMALLY).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "1");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "2", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "2", "1000.0", "5");

        results = client.callProcedure("UpdatePerson", 1, 3L, 31L, 200, 9).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "2");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "1", "3");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "3", "200.0", "9");
    }

    private void verifyENG6511(Client client) throws IOException, ProcCallException
    {
        VoltTable vresult = null;
        VoltTable tresult = null;
        String prefix = "Assertion failed comparing the view content and the AdHoc query result ";

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511 ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(v2) AS vmin, MAX(v2) AS vmax FROM ENG6511 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511expL ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1+1, d2*2, COUNT(*), MIN(v2) AS vmin, MAX(v2) AS vmax FROM ENG6511 GROUP BY d1+1, d2*2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511expL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511expR ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(abs(v1)) AS vmax FROM ENG6511 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511expR: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511expLR ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1+1, d2*2, COUNT(*), MIN(v2-1) AS vmin, MAX(v2-1) AS vmax FROM ENG6511 GROUP BY d1+1, d2*2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511expLR: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511C ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(v1) AS vmin, MAX(v1) AS vmax FROM ENG6511 WHERE v1 > 4 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511C: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511TwoIndexes ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(v2) AS vmax FROM ENG6511 WHERE v1 > 4 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511TwoIndexes: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511NoGroup ORDER BY 1, 2, 3;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT COUNT(*), MIN(v1) AS vmin, MAX(v2) AS vmax FROM ENG6511 ORDER BY 1, 2, 3;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511NoGroup: ", tresult, vresult, EPSILON);
    }

    private void runAndVerifyENG6511(Client client, String query) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(1, results.length);
        verifyENG6511(client);
    }

    // Test the correctness of min/max when choosing an index on both group-by columns and aggregation column/exprs.
    private void subtestENG6511(boolean singlePartition) throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        int pid = singlePartition ? 1 : 2;

        insertRow(client, "ENG6511", 1, 1, 3, 70, 46);
        insertRow(client, "ENG6511", 1, 1, 3, 70, 46);
        insertRow(client, "ENG6511", 1, 1, 3, 12, 66);
        insertRow(client, "ENG6511", pid, 1, 3, 9, 70);
        insertRow(client, "ENG6511", pid, 1, 3, 256, 412);
        insertRow(client, "ENG6511", pid, 1, 3, 70, -46);

        insertRow(client, "ENG6511", 1, 1, 4, 17, 218);
        insertRow(client, "ENG6511", 1, 1, 4, 25, 28);
        insertRow(client, "ENG6511", pid, 1, 4, 48, 65);
        insertRow(client, "ENG6511", pid, 1, 4, -48, 70);

        insertRow(client, "ENG6511", 1, 2, 5, -71, 75);
        insertRow(client, "ENG6511", 1, 2, 5, -4, 5);
        insertRow(client, "ENG6511", pid, 2, 5, 64, 16);
        insertRow(client, "ENG6511", pid, 2, 5, null, 91);

        insertRow(client, "ENG6511", 1, 2, 6, -9, 85);
        insertRow(client, "ENG6511", 1, 2, 6, 38, 43);
        insertRow(client, "ENG6511", pid, 2, 6, 21, -51);
        insertRow(client, "ENG6511", pid, 2, 6, null, 17);
        verifyENG6511(client);

        runAndVerifyENG6511(client, "UPDATE ENG6511 SET v2=120 WHERE v2=17;");
        runAndVerifyENG6511(client, "DELETE FROM ENG6511 WHERE v2=-51;");
        runAndVerifyENG6511(client, "DELETE FROM ENG6511 WHERE v1=-71;");
        runAndVerifyENG6511(client, "DELETE FROM ENG6511 WHERE v1=48;");
        runAndVerifyENG6511(client, "UPDATE ENG6511 SET v1=NULL WHERE v1=256;");
        runAndVerifyENG6511(client, "DELETE FROM ENG6511 WHERE pid=1 AND v1=70 ORDER BY pid, d1, d2, v1, v2 LIMIT 2;");
        runAndVerifyENG6511(client, "DELETE FROM ENG6511 WHERE d1=2 AND d2=5 AND v1 IS NOT NULL;");
    }

    public void testSinglePartition() throws IOException, ProcCallException
    {
        subtestInsertSinglePartition();
        subtestDeleteSinglePartition();
        subtestUpdateSinglePartition();
        subtestSinglePartitionWithPredicates();
        subtestMinMaxSinglePartition();
        subtestMinMaxSinglePartitionWithPredicate();
        subtestIndexMinMaxSinglePartition();
        subtestIndexMinMaxSinglePartitionWithPredicate();
        subtestNullMinMaxSinglePartition();
        subtestENG7872SinglePartition();
        subtestENG6511(false);
    }


    private void subtestInsertSinglePartition() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 27500.20, 7, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 28920.99, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 32L, 63250.01, -1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(2, results[0].getRowCount());
        assert(results != null);

        // HSQL backend does not support multi-statement transactionality.
        if ( ! isHSQL()) {
            // Make a doomed attempt to insert that should have no effect.
            try {
                results = client.callProcedure("AddPerson", 1, 4L, 44L, 44444.44, 4, SABOTAGE).getResults();
                fail("intentional ProcCallException failed");
            } catch (ProcCallException pce) {
                // Expected the throw.
            }
        }
        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(2, results[0].getRowCount());
    }

    private void subtestDeleteSinglePartition() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 27500.20, 7, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 28920.99, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        results[0].advanceRow();
        assertEquals(31L, results[0].getLong(0));
        assertEquals(2L, results[0].getLong(2));
        assertEquals(27500.20 + 28920.99, results[0].getDouble("SALARIES"), 0.001);

        // HSQL backend does not support multi-statement transactionality.
        if ( ! isHSQL()) {
            // Make a doomed attempt to delete that should have no effect.
            try {
                results = client.callProcedure("DeletePerson", 1, 1L, SABOTAGE).getResults();
                fail("intentional ProcCallException failed");
            } catch (ProcCallException pce) {
                // Expected the throw.
            }
        }
        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        results[0].advanceRow();
        assertEquals(31L, results[0].getLong(0));
        assertEquals(2L, results[0].getLong(2));
        assertEquals(27500.20 + 28920.99, results[0].getDouble("SALARIES"), 0.001);

        results = client.callProcedure("DeletePerson", 1, 1L, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        while (results[0].advanceRow()) {
            assertEquals(31L, results[0].getLong(0));
            assertEquals(1L, results[0].getLong(2));
            assertEquals(28920.99, results[0].getDouble(3), 0.01);
            assertEquals(3L, results[0].getLong(4));
        }
        assert(results != null);

        results = client.callProcedure("DeletePerson", 1, 2L, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);
    }

    private void subtestUpdateSinglePartition() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 27500.20, 7, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 28920.99, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 33L, 28920.99, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("UpdatePerson", 1, 2L, 31L, 15000.00, 3).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("UpdatePerson", 1, 1L, 31L, 15000.00, 5).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(2, results[0].getRowCount());
        System.out.println(results[0].toString());
        VoltTableRow r1 = results[0].fetchRow(0);
        VoltTableRow r2 = results[0].fetchRow(1);
        assertEquals(31L, r1.getLong(0));
        assertEquals(2L, r1.getLong(2));
        assertTrue(Math.abs(r1.getDouble(3) - 30000.0) < .01);
        assertEquals(8L, r1.getLong(4));

        assertEquals(33L, r2.getLong(0));
        assertEquals(1L, r2.getLong(2));
        assertTrue(Math.abs(r2.getDouble(3) - 28920.99) < .01);
        assertEquals(3L, r2.getLong(4));
    }

    private void subtestSinglePartitionWithPredicates() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);

        // expecting the 2yr old won't make it
        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 7, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 2L, 2000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("UpdatePerson", 1, 1L, 3L, 1000.0, 6).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("UpdatePerson", 1, 2L, 50L, 4000.0, 4).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("DeletePerson", 1, 1L, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        assert(results != null);
    }

    private void subtestMinMaxSinglePartition() throws IOException, ProcCallException {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;
        VoltTable t;

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 4L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(4, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("DeletePerson", 1, 2L, NORMALLY).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("UpdatePerson", 1, 3L, 31L, 200, 9).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(200, (int)(t.getDouble(3)));
        assertEquals(9, t.getLong(4));

        results = client.callProcedure("UpdatePerson", 1, 4L, 31L, 0, 10).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));

        results = client.callProcedure("DeletePerson", 1, 1L, NORMALLY).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));

    }

    private void subtestMinMaxSinglePartitionWithPredicate() throws IOException, ProcCallException {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;
        VoltTable t;

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE3").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 4L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE3").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(5, t.getLong(3));

        results = client.callProcedure("DeletePerson", 1, 4L, NORMALLY).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE3").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(1, t.getLong(2));
        assertEquals(3, t.getLong(3));

        results = client.callProcedure("UpdatePerson", 1, 1L, 31L, 2000, 9).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE3").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(1, t.getLong(2));
        assertEquals(9, t.getLong(3));

    }

    private void subtestIndexMinMaxSinglePartition() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_MATVIEW;").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        insertRow(client, "DEPT_PEOPLE", 1, 1L, 31L, 1000.00, 3);
        insertRow(client, "DEPT_PEOPLE", 2, 1L, 31L, 900.00, 5);
        insertRow(client, "DEPT_PEOPLE", 3, 1L, 31L, 900.00, 1);
        insertRow(client, "DEPT_PEOPLE", 4, 1L, 31L, 2500.00, 5);

        VoltTable t;
        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(4, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("DEPT_PEOPLE.delete", 2L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("DEPT_PEOPLE.update", 3L, 1, 31L, 200, 9, 3L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(200, (int)(t.getDouble(3)));
        assertEquals(9, t.getLong(4));

        results = client.callProcedure("DEPT_PEOPLE.update", 4L, 1, 31L, 0, 10, 4L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));

        results = client.callProcedure("DEPT_PEOPLE.delete", 1L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));
    }

    private void subtestIndexMinMaxSinglePartitionWithPredicate() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_FILTER_MATVIEW;").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        insertRow(client, "DEPT_PEOPLE", 1, 1L, 31L, 1000.00, 3);
        insertRow(client, "DEPT_PEOPLE", 2, 1L, 31L, 900.00, 5);
        insertRow(client, "DEPT_PEOPLE", 3, 1L, 31L, 900.00, 1);
        insertRow(client, "DEPT_PEOPLE", 4, 1L, 31L, 2500.00, 5);

        VoltTable t;
        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_FILTER_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(5, t.getLong(3));

        results = client.callProcedure("DEPT_PEOPLE.delete", 2L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_FILTER_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(5, t.getLong(3));

        results = client.callProcedure("DEPT_PEOPLE.update", 4L, 1, 31L, 200, 9, 4L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_PEOPLE;").getResults();
        System.out.println(results[0].toString());

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_FILTER_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(1, t.getLong(2));
        assertEquals(3, t.getLong(3));

        results = client.callProcedure("DEPT_PEOPLE.update", 4L, 1, 31L, 2000, 9, 4L).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM DEPT_AGE_FILTER_MATVIEW").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(9, t.getLong(3));

    }

    private void subtestNullMinMaxSinglePartition() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;
        VoltTable t;

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 4L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 5L, 31L, null, null, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(5, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("DeletePerson", 1, 2L, NORMALLY).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(4, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("UpdatePerson", 1, 3L, 31L, 200, 9).getResults();

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(1, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(4, t.getLong(2));
        assertEquals(200, (int)(t.getDouble(3)));
        assertEquals(9, t.getLong(4));
    }

    private void subtestENG7872MP() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "0");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "0");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "0", "null");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "0", "null", "null");

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 4L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 5L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 6L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 7L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 8L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "8");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "4", "8");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "6", "900.0", "5");

        results = client.callProcedure("DeletePerson", 1, 2L, NORMALLY).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "7");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "3");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "4", "8");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "5", "900.0", "5");

        results = client.callProcedure("DeletePerson", 2, 6L, NORMALLY).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "6");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "2");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "4", "8");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "4", "1000.0", "5");

        results = client.callProcedure("UpdatePerson", 1, 3L, 31L, 200, 9).getResults();
        results = client.callProcedure("UpdatePerson", 2, 7L, 31L, 200, 9).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "6");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "2", "6");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "6", "200.0", "9");

        results = client.callProcedure("UpdatePerson", 1, 4L, 31L, 0, 10).getResults();
        results = client.callProcedure("UpdatePerson", 2, 8L, 31L, 0, 10).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "6");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "2", "6");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "6", "0.0", "10");

        results = client.callProcedure("DeletePerson", 1, 1L, NORMALLY).getResults();
        results = client.callProcedure("DeletePerson", 2, 5L, NORMALLY).getResults();

        assertAggNoGroupBy(client, "MATPEOPLE_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT", "4");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_SUM", "0", "null");
        assertAggNoGroupBy(client, "MATPEOPLE_CONDITIONAL_COUNT_MIN_MAX", "4", "0.0", "10");
    }

    public void testMPAndRegressions() throws IOException, ProcCallException
    {
        subtestMultiPartitionSimple();
        subtestInsertReplicated();
        subtestInsertAndOverflowSum();
        subtestENG798();
        subtestIndexed();
        subtestMinMaxMultiPartition();
        subtestENG7872MP();
        subtestENG6511(true);
    }

    private void subtestMultiPartitionSimple() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 2L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 3L, 23L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 4L, 23L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 5L, 35L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 6L, 35L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("UpdatePerson", 1, 2L, 32L, 1000.0, 3).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("DeletePerson", 2, 6L, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggAges", 1).getResults();
        assert(results != null);
        assertEquals(1, results.length);

        VoltTable results2[] = client.callProcedure("AggAges", 2).getResults();
        assert(results != null);
        assertEquals(1, results2.length);

        int totalRows = results[0].getRowCount() + results2[0].getRowCount();
        // unfortunately they're both 4 in the hsql case, the fact that partitioning
        // can change behavior between backends if not used smartly should be corrected
        assertTrue((4 == totalRows) ||
                   (results[0].getRowCount() == 4) || (results2[0].getRowCount() == 4));
    }

    private void subtestInsertReplicated() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggThings").getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("AddThing", 1L, 10L).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddThing", 2L, 12L).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddThing", 3L, 10L).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("AggThings").getResults();
        assertEquals(1, results.length);
        assertEquals(2, results[0].getRowCount());
        assert(results != null);
    }

    private void subtestInsertAndOverflowSum() throws IOException, ProcCallException
    {
        if (isHSQL()) {
            return;
        }
        Client client = getClient();
        truncateBeforeTest(client);
        int invocationIndex = 0;
        VoltTable[] results = client.callProcedure("OverflowTest", 0, 0, invocationIndex++).getResults();
        results = client.callProcedure("OverflowTest", 2, 0, invocationIndex++).getResults();
        results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
        results[0].advanceRow();
        long preRollbackValue = results[0].getLong(3);
        boolean threwException = false;
        try {
            results = client.callProcedure("OverflowTest", 0, 0, invocationIndex++).getResults();
        } catch (Exception e) {
           threwException = true;
        }
        assertTrue(threwException);
        results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
        results[0].advanceRow();
        assertEquals(preRollbackValue, results[0].getLong(3));
        preRollbackValue = 0;
        threwException = false;
        while (!threwException) {
            try {
                results = client.callProcedure("OverflowTest", 2, 0, invocationIndex++).getResults();
                results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
                results[0].advanceRow();
                preRollbackValue = results[0].getLong(2);
            } catch (Exception e) {
                threwException = true;
                break;
            }
        }
        results = client.callProcedure("OverflowTest", 1, 0, 0).getResults();
        results[0].advanceRow();
        assertEquals(preRollbackValue, results[0].getLong(2));
    }

    /** Test a view that re-orders the source table's columns */
    private void subtestENG798() throws IOException, ProcCallException
    {
        if (isHSQL()) {
            return;
        }

        // this would throw on a bad cast in the broken case.
        Client client = getClient();
        truncateBeforeTest(client);
        ClientResponse callProcedure = client.callProcedure("Eng798Insert", "clientname");
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        assertEquals(1, callProcedure.getResults().length);
        assertEquals(1, callProcedure.getResults()[0].asScalarLong());
    }


    private void subtestIndexed() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;

        results = client.callProcedure("AggAges", 1).getResults();
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());
        assert(results != null);

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 33L, 28920.99, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 4L, 23L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 5L, 35L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 6L, 35L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 7L, 23L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 8L, 31L, 2222.22, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("UpdatePerson", 1, 2L, 32L, 1000.0, 3).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("DeletePerson", 2, 6L, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        int totalRows;
        // INDEXED_FIRST_GROUP   AS SELECT AGE, SALARIES LIMIT 1;
        results = client.callProcedure("INDEXED_FIRST_GROUP").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        totalRows = results[0].getRowCount();
        assertEquals(1, totalRows);
        results[0].advanceRow();
        assertEquals(33L, results[0].getLong(0));
        assertEquals(28920.99, results[0].getDouble(1), 0.001);

        // INDEXED_MAX_GROUP     AS SELECT MAX(SALARIES);
        results = client.callProcedure("INDEXED_MAX_GROUP").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        totalRows = results[0].getRowCount();
        assertEquals(1, totalRows);
        results[0].advanceRow();
        assertEquals(28920.99, results[0].getDouble(0), 0.001);

        // INDEXED_MAX_IN_GROUPS AS SELECT MAX(SALARIES) WHERE AGE = ?;
        results = client.callProcedure("INDEXED_MAX_IN_GROUPS", 31L).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        totalRows = results[0].getRowCount();
        assertEquals(1, totalRows);
        results[0].advanceRow();
        assertEquals(2222.22, results[0].getDouble(0), 0.001);

        // INDEXED_GROUPS: AGE, SALARIES, PARTITION, NUM, KIDS ORDER BY AGE, SALARIES */
        results = client.callProcedure("INDEXED_GROUPS").getResults();
        assert(results != null);
        totalRows = results[0].getRowCount();
        assertEquals(6, totalRows);
        results[0].advanceRow();
        assertEquals(23L, results[0].getLong(0));
        assertEquals(2000.0, results[0].getDouble(1), 0.001);
        results[0].advanceRow();
        assertEquals(31L, results[0].getLong(0));
        assertEquals(1000.0, results[0].getDouble(1), 0.001);
        results[0].advanceRow();
        assertEquals(31L, results[0].getLong(0));
        assertEquals(2222.22, results[0].getDouble(1), 0.001);
        results[0].advanceRow();
        assertEquals(32L, results[0].getLong(0));
        assertEquals(1000.00, results[0].getDouble(1), 0.001);

        long timestampInitializer;
        int ii;

        int delay = 0; // keeps the clock moving forward.
        // +1 V_TEAM_MEMBERSHIP, +1 V_TEAM_TIMES
        timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
        insertRow(client, "CONTEST", "Senior", timestampInitializer, "Boston", "Jack");

        // +1 V_TEAM_MEMBERSHIP, +4 V_TEAM_TIMES
        for (ii = 0; ii < 4; ++ii) {
            timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
            insertRow(client, "CONTEST", "Senior", timestampInitializer, "Cambridge", "anonymous " + ii);
        }

        // +0 V_TEAM_MEMBERSHIP, +1 V_TEAM_TIMES
        timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
        for (ii = 0; ii < 3; ++ii) {
            insertRow(client, "CONTEST", "Senior", timestampInitializer, "Boston",  "not Jack " + ii);
        }

        // +1 V_TEAM_MEMBERSHIP, +1 V_TEAM_TIMES
        timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
        for (ii = 0; ii < 3; ++ii) {
            insertRow(client, "CONTEST", "Senior", timestampInitializer, "Concord", "Emerson " + ii);
        }

        // +1 V_TEAM_MEMBERSHIP, +2 V_TEAM_TIMES
        for (ii = 0; ii < 2; ++ii) {
            timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
            insertRow(client, "CONTEST", "Senior", timestampInitializer, "Lexington", "Luis " + ii);
        }

        if ( ! isHSQL()) {
            results = client.callProcedure("@AdHoc",
                "SELECT team, total, finish FROM V_TEAM_TIMES " +
                "ORDER BY total DESC, 0-SINCE_EPOCH(MILLISECOND, finish) DESC").getResults();
            assertEquals(1, results.length);
            System.out.println(results[0]);
            assertEquals(9, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals("Boston", results[0].getString(0));
            assertEquals(3, results[0].getLong(1));
            results[0].advanceRow();
            assertEquals("Concord", results[0].getString(0));
            assertEquals(3, results[0].getLong(1));

            results[0].advanceToRow(8);
            assertEquals("Lexington", results[0].getString(0));
            assertEquals(1, results[0].getLong(1));
        }

        /**
         * Current data in MV table: V_TEAM_MEMBERSHIP.
         *  header size: 39
         *   status code: -128 column count: 3
         *   cols (RUNNER_CLASS:STRING), (TEAM:STRING), (TOTAL:INTEGER),
         *   rows -
         *    Senior,Boston,4
         *    Senior,Cambridge,4
         *    Senior,Concord,3
         *    Senior,Lexington,2
         */
        results = client.callProcedure("@AdHoc",
                "SELECT count(*) FROM V_TEAM_MEMBERSHIP where team > 'Cambridge' order by total").getResults();
        assertEquals(1, results.length);
        System.out.println(results[0]);
        assertEquals(2L, results[0].asScalarLong());

        results = client.callProcedure("@AdHoc",
                "SELECT count(*) FROM V_TEAM_MEMBERSHIP where total > 3 ").getResults();
        assertEquals(1, results.length);
        System.out.println(results[0]);
        assertEquals(2L, results[0].asScalarLong());



        results = client.callProcedure("@AdHoc",
                "SELECT team, finish FROM V_TEAM_TIMES ORDER BY finish DESC limit 3").getResults();
        assertEquals(1, results.length);
        System.out.println(results[0]);
        assertEquals(3, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals("Lexington", results[0].getString(0));
        results[0].advanceRow();
        assertEquals("Lexington", results[0].getString(0));
        results[0].advanceRow();
        assertEquals("Concord", results[0].getString(0));
    }

    private void subtestMinMaxMultiPartition() throws IOException, ProcCallException {
        Client client = getClient();
        truncateBeforeTest(client);
        VoltTable[] results = null;
        VoltTable t;

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(0, results[0].getRowCount());

        results = client.callProcedure("AddPerson", 1, 1L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 2L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 3L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 1, 4L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 5L, 31L, 1000.0, 3, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 6L, 31L, 900.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 7L, 31L, 900.0, 1, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
        results = client.callProcedure("AddPerson", 2, 8L, 31L, 2500.0, 5, NORMALLY).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(2, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(4, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));
        t.advanceRow();
        assertEquals(4, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("DeletePerson", 1, 2L, NORMALLY).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results = client.callProcedure("DeletePerson", 2, 6L, NORMALLY).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(2, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(900, (int)(t.getDouble(3)));
        assertEquals(5, t.getLong(4));

        results = client.callProcedure("UpdatePerson", 1, 3L, 31L, 200, 9).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results = client.callProcedure("UpdatePerson", 2, 7L, 31L, 200, 9).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(2, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(200, (int)(t.getDouble(3)));
        assertEquals(9, t.getLong(4));
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(200, (int)(t.getDouble(3)));
        assertEquals(9, t.getLong(4));

        results = client.callProcedure("UpdatePerson", 1, 4L, 31L, 0, 10).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results = client.callProcedure("UpdatePerson", 2, 8L, 31L, 0, 10).getResults();
        assert(results != null);
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(2, t.getRowCount());
        System.out.println(t.toString());
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));
        t.advanceRow();
        assertEquals(3, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));

        results = client.callProcedure("DeletePerson", 1, 1L, NORMALLY).getResults();
        results = client.callProcedure("DeletePerson", 2, 5L, NORMALLY).getResults();

        validateHardCodedStatusQuo(client);

        System.out.println("Testing single-source-table truncates");

        // Make sure the stored proc behaves correctly even if we
        // purposely abort it.
        // forceAbort = 1 for "yes" is the more interesting case.
        for (int forceAbort : yesAndNo) {
            // Try one or more truncates on the same view source table
            // within the stored proc.
            for (int repeats = 1; repeats <= 3; ++repeats) {
                // For a given number of truncates, vary the number of
                // times we repopulate the data before the next truncate.
                // We always repopulate after the last truncate so the
                // final state of the database does not depend on a commit.

                // Different views (with a group by) MAY depend on
                // re-population to repro the issue.
                // Views without a GROUP BY maintain
                // an empty result row with count = 0,
                // which seems to be enough to trigger the problem.
                for (int restores = 1; restores < repeats; ++restores) {
                    try {
                        try {
                            results = client.callProcedure("TruncatePeople",
                                    forceAbort, repeats, restores).getResults();
                            assertEquals("TruncatePeople was expected to roll back", 0, forceAbort);
                        }
                        catch (ProcCallException vae) {
                            if ( ! vae.getMessage().contains("Rolling back as requested")) {
                                throw vae;
                            }
                            assertEquals("TruncatePeople was not requested to roll back", 1, forceAbort);
                        }
                    }
                    catch (Exception other) {
                        fail("The call to TruncatePeople unexpectedly threw: " + other);
                    }
                    //* enable to debug */  System.out.println("SURVIVED TruncatePeople." + repeats + "." + restores);
                    validateHardCodedStatusQuo(client);
                }
            }
        }
    }

    /**
     * @param client
     * @throws ProcCallException
     * @throws IOException
     * @throws NoConnectionsException
     */
    private void validateHardCodedStatusQuo(Client client)
            throws NoConnectionsException, IOException, ProcCallException {
        VoltTable[] results = client.callProcedure("@AdHoc",
                "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        VoltTable t;
        t = results[0];
        assertEquals(2, t.getRowCount());
        //* enable to debug */  System.out.println(t);
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));
        t.advanceRow();
        assertEquals(2, t.getLong(2));
        assertEquals(0, (int)(t.getDouble(3)));
        assertEquals(10, t.getLong(4));
    }

    private void insertRow(Client client, Object... parameters) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        results = client.callProcedure(parameters[0].toString() + ".insert", Arrays.copyOfRange(parameters, 1, parameters.length)).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
    }

    private void insertRowAdHoc(Client client, String stmt) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        results = client.callProcedure("@AdHoc", stmt).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
    }

    private void deleteRow(Client client, Object... parameters) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        String tableName = parameters[0].toString();
        if (tableName.equalsIgnoreCase("ORDERITEMS")) {
            results = client.callProcedure("DELETEORDERITEMS", parameters[1], parameters[2]).getResults();
        }
        else {
            results = client.callProcedure(tableName + ".delete", parameters[1]).getResults();
        }
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
    }

    private void updateRow(Client client, Object[] oldRow, Object[] newRow) throws IOException, ProcCallException
    {
        VoltTable[] results = null;
        String tableName1 = oldRow[0].toString();
        String tableName2 = newRow[0].toString();
        assertEquals("Trying to update table " + tableName1 + " with " + tableName2 + " data.", tableName1, tableName2);
        results = client.callProcedure("UPDATE" + tableName1, newRow[2], newRow[3],
                                                              oldRow[1], oldRow[2], oldRow[3]).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());
    }

    private void verifyViewOnJoinQueryResult(Client client) throws IOException, ProcCallException
    {
        VoltTable vresult = null;
        VoltTable tresult = null;
        String prefix = "Assertion failed comparing the view content and the AdHoc query result ";

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_COUNT_NOPCOL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_COUNT_NOPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_COUNT_NOPCOL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_COUNT_GLOBAL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_COUNT_GLOBAL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_COUNT_GLOBAL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_DETAIL_NOPCOL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_DETAIL_NOPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_DETAIL_NOPCOL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_DETAIL_WITHPCOL ORDER BY 1, 2;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_DETAIL_WITHPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_DETAIL_WITHPCOL: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER2016 ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER2016").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER2016: ", tresult, vresult, EPSILON);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM QTYPERPRODUCT ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_QTYPERPRODUCT").getResults()[0];
        assertTablesAreEqual(prefix + "QTYPERPRODUCT: ", tresult, vresult, EPSILON);
    }

    public void testViewOnJoinQuery() throws IOException, ProcCallException
    {
        Client client = getClient();
        truncateBeforeTest(client);
        ArrayList<Object[]> dataList1 = Lists.newArrayList(
            new Object[][] {
                {"CUSTOMERS", 1, "Tom", "VoltDB"},
                {"CUSTOMERS", 2, "Jerry", "Bedford"},
                {"CUSTOMERS", 3, "Rachael", "USA"},
                {"CUSTOMERS", 4, "Ross", "Massachusetts"},
                {"CUSTOMERS", 5, "Stephen", "Houston TX"},
                {"ORDERS", 1, 2, "2016-04-23 13:24:57.671000"},
                {"ORDERS", 2, 7, "2015-04-12 10:24:10.671400"},
                {"ORDERS", 3, 5, "2016-01-20 09:24:15.943000"},
                {"ORDERS", 4, 1, "2015-10-30 19:24:00.644000"},
                {"PRODUCTS", 1, "H MART", 20.97},
                {"PRODUCTS", 2, "COSTCO WHOLESALE", 62.66},
                {"PRODUCTS", 3, "CENTRAL ROCK GYM", 22.00},
                {"PRODUCTS", 4, "ATT*BILL PAYMENT", 48.90},
                {"PRODUCTS", 5, "APL* ITUNES", 16.23},
                {"PRODUCTS", 6, "GOOGLE *YouTube", 10.81},
                {"PRODUCTS", 7, "UNIV OF HOUSTON SYSTEM", 218.35},
                {"PRODUCTS", 8, "THE UPS STORE 2287", 36.31},
                {"PRODUCTS", 9, "NNU*XFINITYWIFI", 7.95},
                {"PRODUCTS", 10, "IKEA STOUGHTON", 61.03},
                {"PRODUCTS", 11, "WM SUPERCENTER #5752", 9.74},
                {"PRODUCTS", 12, "STOP & SHOP 0831", 12.28},
                {"PRODUCTS", 13, "VERANDA NOODLE HOUSE", 29.81},
                {"PRODUCTS", 14, "AMC 34TH ST 14 #2120", 38.98},
                {"PRODUCTS", 15, "STARBUCKS STORE 19384", 5.51},
                {"ORDERITEMS", 1, 2, 1},
                {"ORDERITEMS", 1, 7, 1},
                {"ORDERITEMS", 2, 5, 2},
                {"ORDERITEMS", 3, 1, 3},
                {"ORDERITEMS", 3, 15, 1},
                {"ORDERITEMS", 3, 20, 1},
                {"ORDERITEMS", 3, 4, 2},
                {"ORDERITEMS", 3, 26, 5},
                {"ORDERITEMS", 4, 30, 1},
                {"ORDERITEMS", 5, 8, 1},
            }
        );
        ArrayList<Object[]> dataList2 = Lists.newArrayList(
            new Object[][] {
                {"CUSTOMERS", 6, "Mike", "WPI"},
                {"CUSTOMERS", 7, "Max", "New York"},
                {"CUSTOMERS", 8, "Ethan", "Beijing China"},
                {"CUSTOMERS", 9, "Selina", "France"},
                {"CUSTOMERS", 10, "Harry Potter", "Hogwarts"},
                {"ORDERS", 5, 3, "2015-04-23 00:24:45.768000"},
                {"ORDERS", 6, 2, "2016-07-05 16:24:31.384000"},
                {"ORDERS", 7, 4, "2015-03-09 21:24:15.768000"},
                {"ORDERS", 8, 2, "2015-09-01 16:24:42.279300"},
                {"PRODUCTS", 16, "SAN SOO KAP SAN SHUSHI", 10.69},
                {"PRODUCTS", 17, "PLASTC INC.", 155.00},
                {"PRODUCTS", 18, "MANDARIN MALDEN", 34.70},
                {"PRODUCTS", 19, "MCDONALDS F16461", 7.25},
                {"PRODUCTS", 20, "UBER US JUL20 M2E3D", 31.33},
                {"PRODUCTS", 21, "TOUS LES JOURS", 13.25},
                {"PRODUCTS", 22, "GINGER JAPANESE RESTAU", 69.20},
                {"PRODUCTS", 23, "WOO JEON II", 9.58},
                {"PRODUCTS", 24, "INFLIGHT WI-FI - LTV", 7.99},
                {"PRODUCTS", 25, "EXPEDIA INC", 116.70},
                {"PRODUCTS", 26, "THE ICE CREAM STORE", 5.23},
                {"PRODUCTS", 27, "WEGMANS BURLINGTON #59", 22.13},
                {"PRODUCTS", 28, "ACADEMY EXPRESS", 46.80},
                {"PRODUCTS", 29, "TUCKS CANDY FACTORY INC", 7.00},
                {"PRODUCTS", 30, "SICHUAN GOURMET", 37.12},
                {"ORDERITEMS", 5, 12, 6},
                {"ORDERITEMS", 5, 1, 0},
                {"ORDERITEMS", 5, 27, 1},
                {"ORDERITEMS", 6, 0, 1},
                {"ORDERITEMS", 6, 21, 1},
                {"ORDERITEMS", 7, 8, 1},
                {"ORDERITEMS", 7, 19, 1},
                {"ORDERITEMS", 7, 30, 4},
                {"ORDERITEMS", 7, 1, 1},
                {"ORDERITEMS", 8, 25, 2}
            }
        );
        assertEquals(dataList1.size(), dataList2.size());

        // -- 1 -- Test updating the data in the source tables.
        // There are two lists of data, we first insert the data in the first list
        // into the corresponding source tables, then update each row with the data
        // from the second data list.
        System.out.println("Now testing updating the join query view source table.");
        for (int i=0; i<dataList1.size(); i++) {
            insertRow(client, dataList1.get(i));
            verifyViewOnJoinQueryResult(client);
        }
        for (int i=0; i<dataList2.size(); i++) {
            updateRow(client, dataList1.get(i), dataList2.get(i));
            verifyViewOnJoinQueryResult(client);
        }

        // -- 2 -- Test inserting the data into the source tables.
        // We do a shuffle here and in the delete test. But I do believe we still
        // have the full coverage of all the cases because we are inserting and deleting
        // all the rows. The cases updating values of all kinds of aggregations will be
        // tested in one row or another.
        truncateBeforeTest(client);
        // Merge two sub-lists for the following tests.
        dataList1.addAll(dataList2);
        // For more deterministic debugging, consider this instead of shuffle:
        // Collections.reverse(dataList1);
        Collections.shuffle(dataList1);
        System.out.println("Now testing inserting data to the join query view source table.");
        for (int i=0; i<dataList1.size(); i++) {
            insertRow(client, dataList1.get(i));
            verifyViewOnJoinQueryResult(client);
        }

        // -- 3 -- Test altering the source table
        // This alter table test will alter the source table schema first, then test if the view still
        // has the correct content. Columns referenced by the views are not altered (we don't allow it).
        // Our HSQL backend testing code does not support AdHoc DDL, disable this on HSQLBackend.
        // This is fine because we don't use HSQL as reference in this test anyway.
        if (! isHSQL()) {
            System.out.println("Now testing altering the source table of a view.");
            // 3.1 add column
            try {
                client.callProcedure("@AdHoc", "ALTER TABLE ORDERITEMS ADD COLUMN x FLOAT;" +
                        "ALTER TABLE WAS_ORDERITEMS ADD COLUMN x FLOAT;");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to add column to a view source table.");
            }
            verifyViewOnJoinQueryResult(client);
            // 3.2 drop column
            try {
                client.callProcedure("@AdHoc", "ALTER TABLE ORDERITEMS DROP COLUMN x;" +
                        "ALTER TABLE WAS_ORDERITEMS DROP COLUMN x;");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop column on a view source table.");
            }
            verifyViewOnJoinQueryResult(client);
            // 3.3 alter column
            try {
                client.callProcedure("@AdHoc", "ALTER TABLE CUSTOMERS ALTER COLUMN ADDRESS VARCHAR(100);" +
                        "ALTER TABLE WAS_CUSTOMERS ALTER COLUMN ADDRESS VARCHAR(100);");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to alter column in a view source table.");
            }
            verifyViewOnJoinQueryResult(client);
        }

        // -- 4 -- Test defining view after the data is loaded.
        //         The test is crafted to include only safe operations.
        if (! isHSQL()) {
            System.out.println("Now testing view data catching-up.");
            try {
                client.callProcedure("@AdHoc", "DROP VIEW ORDER_DETAIL_WITHPCOL;");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop a view.");
            }
            try {
                client.callProcedure("@AdHoc",
                    "CREATE VIEW ORDER_DETAIL_WITHPCOL (NAME, ORDER_ID, CNT, MINUNIT, MAXUNIT) AS " +
                    "SELECT " +
                        "CUSTOMERS.NAME, " +
                        "ORDERS.ORDER_ID, " +
                        "COUNT(*), " +
                        "MIN(PRODUCTS.PRICE), " +
                        "MAX(PRODUCTS.PRICE) " +
                    "FROM CUSTOMERS JOIN ORDERS ON CUSTOMERS.CUSTOMER_ID = ORDERS.CUSTOMER_ID " +
                                   "JOIN ORDERITEMS ON ORDERS.ORDER_ID = ORDERITEMS.ORDER_ID " +
                                   "JOIN PRODUCTS ON ORDERITEMS.PID = PRODUCTS.PID " +
                    "GROUP BY CUSTOMERS.NAME, ORDERS.ORDER_ID;");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create a view.");
            }
            verifyViewOnJoinQueryResult(client);
        }

        // -- 5 -- Test truncating one or more tables,
        // then explicitly restoring their content.
        System.out.println("Now testing truncating the join query view source table.");
        // Temporarily substitute never for yesAndNo on the next line if you
        // want to bypass testing of rollback after truncate.
        for (int forceRollback : /*default:*/ yesAndNo) { //alt:*/ never) {
            for (int truncateTable1 : yesAndNo) {
                // Each use of 'never' reduces by half the tried
                // combinations of truncate operations.
                for (int truncateTable2 : /*default:*/ yesAndNo) { //alt:*/ never) {
                    // Substitute yesAndNo below for test overkill
                    for (int truncateTable3 : /**/ never) { //*/ yesAndNo) {
                        for (int truncateTable4 : /**/ never) { //*/ yesAndNo) {
                            // truncateSourceTable verifies the short-term effects
                            // of truncation and restoration within the transaction.
                            truncateSourceTables(client, forceRollback,
                                    truncateTable1,
                                    truncateTable2,
                                    truncateTable3,
                                    truncateTable4);
                            // Verify the correctness outside the transaction.
                            verifyViewOnJoinQueryResult(client);
                        }
                    }
                }
            }
        }

        // -- 6 -- Test deleting the data from the source tables.
        // For more deterministic debugging, consider this instead of shuffle:
        // Collections.reverse(dataList1);
        Collections.shuffle(dataList1);
        System.out.println("Now testing deleting data from the join query view source table.");
        for (int i = 0; i < dataList1.size(); i++) {
            deleteRow(client, dataList1.get(i));
            verifyViewOnJoinQueryResult(client);
        }
    }

    private void truncateSourceTables(Client client, int rollback,
            int truncateTable1, int truncateTable2, int truncateTable3,
            int truncateTable4)
    {
        try {
            try {
                VoltTable vt = client.callProcedure("TruncateTables", rollback,
                        truncateTable1,
                        truncateTable2,
                        truncateTable3,
                        truncateTable4).getResults()[0];
                assertEquals("TruncateTables was expected to roll back", 0, rollback);
                String result = " UNEXPECTED EMPTY RETURN FROM TruncateTables ";
                if (vt.advanceRow()) {
                    result = vt.getString(0);
                }
                if ( ! "".equals(result)) {
                    fail("TruncateTables detected an unexpected difference: " + result);
                }
            }
            catch (ProcCallException vae) {
                if ( ! vae.getMessage().contains("Rolling back as requested")) {
                    throw vae;
                }
                assertEquals("TruncateTables was not requested to roll back", 1, rollback);
            }
        }
        catch (Exception other) {
            fail("The call to TruncateTables unexpectedly threw: " + other);
        }
    }

    public void testEng11024() throws Exception {
        // Regression test for ENG-11024, found by sqlcoverage
        Client client = getClient();

        // This is an edge case where a view with no group by keys
        // is having its output tuples counted with count(*).  Of course,
        // the result of this query will always be one.  This query was
        // causing anissue because intermediate temp tables had zero columns.
        VoltTable vt;
        vt = client.callProcedure("@AdHoc", "select count(*) from v3_eng_11024_join").getResults()[0];
        assertEquals(vt.asScalarLong(), 1);

        vt = client.callProcedure("@AdHoc", "select count(*) from v3_eng_11024_1tbl").getResults()[0];
        assertEquals(vt.asScalarLong(), 1);

        vt = client.callProcedure("@AdHoc",
                "select count(*) "
                        + "from v3_eng_11024_1tbl inner join r1_eng_11024 using (ratio)").getResults()[0];
        assertEquals(0, vt.asScalarLong());

        vt = client.callProcedure("@AdHoc",
                "select count(*) "
                        + "from v3_eng_11024_1tbl left outer join r1_eng_11024 using (ratio)").getResults()[0];
        assertEquals(1, vt.asScalarLong());
    }

    public void testUpdateAndMinMax() throws Exception {
        Client client = getClient();

        //        CREATE TABLE P2_ENG_11024 (
        //                ID INTEGER NOT NULL,
        //                VCHAR VARCHAR(300),
        //                NUM INTEGER,
        //                RATIO FLOAT,
        //                PRIMARY KEY (ID)
        //        );
        //        PARTITION TABLE P2_ENG_11024 ON COLUMN ID;
        //
        //        CREATE TABLE P1_ENG_11024 (
        //                ID INTEGER NOT NULL,
        //                VCHAR VARCHAR(300),
        //                NUM INTEGER,
        //                RATIO FLOAT,
        //                PRIMARY KEY (ID)
        //        );
        //
        //        CREATE VIEW V16_ENG_11042 (ID, COUNT_STAR, NUM) AS
        //            SELECT T2.NUM, COUNT(*), MAX(T1.NUM)
        //            FROM R1_ENG_11024 T1 JOIN R2_ENG_11024 T2 ON T1.ID = T2.ID
        //            GROUP BY T2.NUM;

        client.callProcedure("R1_ENG_11024.Insert", 1, "", 20, 0.0);
        client.callProcedure("R2_ENG_11024.Insert", 1, "", 100, 0.0);

        client.callProcedure("R1_ENG_11024.Insert", 2, "", 1000, 0.0);
        client.callProcedure("R2_ENG_11024.Insert", 2, "", 100, 0.0);

        VoltTable vt;
        vt = client.callProcedure("@AdHoc",
                "select * from V16_ENG_11042").getResults()[0];
        assertContentOfTable(new Object[][] {
            {100, 2, 1000}}, vt);

        vt = client.callProcedure("@AdHoc",
                "update R1_ENG_11024 set num = 15 where id = 2;")
                .getResults()[0];
        assertContentOfTable(new Object[][] {{1}}, vt);
        vt = client.callProcedure("@AdHoc",
                "select * from V16_ENG_11042").getResults()[0];
        assertContentOfTable(new Object[][] {
            {100, 2, 20}}, vt);

        // A second way of reproducing, slightly different
        client.callProcedure("@AdHoc", "DELETE FROM R1_ENG_11024");
        client.callProcedure("@AdHoc", "DELETE FROM R2_ENG_11024");

        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES(-13, 'mmm', -6, -13.0);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES(-13, 'mmm', -4, -13.0);");

        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES(-12, 'mmm', -12, -12.0);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES(-12, 'mmm', -4, -12.0);");

        vt = client.callProcedure("@AdHoc",
                "select * from V16_ENG_11042").getResults()[0];
        assertContentOfTable(new Object[][] {
            {-4, 2, -6}}, vt);

        client.callProcedure("@AdHoc", "UPDATE R1_ENG_11024 A SET NUM = ID WHERE ID=-13;");

        vt = client.callProcedure("@AdHoc",
                "select * from V16_ENG_11042").getResults()[0];
        assertContentOfTable(new Object[][] {
            {-4, 2, -12}}, vt);
    }

    public void testEng11043() throws Exception {
        Client client = getClient();

        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-1, null, null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-1, null, null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-1, null, null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-2, null, null, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-2, null, null, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-2, null, null, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-3, null, -333, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-3, null, -333, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-3, null, -333, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-4, null, -333, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-4, null, -333, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-4, null, -333, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-5, 'eee', null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-5, 'eee', null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-5, 'eee', null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-6, 'eee', null, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-6, 'eee', null, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-6, 'eee', null, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-7, 'eee', -777, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-7, 'eee', -777, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-7, 'eee', -777, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-8, 'eee', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-8, 'eee', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-8, 'eee', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-9, 'jjj', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-9, 'jjj', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-9, 'jjj', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-10, 'jjj', -10, -10);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-10, 'jjj', -10, -10);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-10, 'jjj', -10, -10);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-11, 'jjj', -11, -11);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-11, 'jjj', -11, -11);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-11, 'jjj', -11, -11);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-12, 'mmm', -12, -12);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-12, 'mmm', -12, -12);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-12, 'mmm', -12, -12);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-13, 'mmm', -13, -13);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-13, 'mmm', -13, -13);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-13, 'mmm', -13, -13);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-14, 'bouSWVaJwQHtrp', -16078, 5.88087039394022959016e-02);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-14, 'FOO', -16079, 9.88087039394022959016e-02);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-14, 'BAR', -16077, 7.88087039394022959016e-02);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_11024 VALUES (-15, 'NhFmPDULXEFLGI', 29960, 3.59831007623149345953e-01);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-15, 'FOOFOO', 29969, 9.59831007623149345953e-01);");
        client.callProcedure("@AdHoc", "INSERT INTO R1_ENG_11024 VALUES (-15, 'BARBAR', 29967, 7.59831007623149345953e-01);");

        client.callProcedure("@AdHoc", "UPDATE P2_ENG_11024 SET NUM = 18;");

        VoltTable vtExpected = client.callProcedure("@AdHoc",
                "SELECT T1.NUM, COUNT(*), MAX(T2.RATIO), MIN(T3.VCHAR) "
                + "FROM P1_ENG_11024 T1 JOIN P2_ENG_11024 T2 ON T1.ID = T2.ID JOIN R1_ENG_11024 T3 ON T2.ID = T3.ID "
                + "GROUP BY T1.NUM "
                + "ORDER BY T1.NUM").getResults()[0];
        VoltTable vtActual= client.callProcedure("@AdHoc",
                "select * from v27 order by num").getResults()[0];

        String prefix = "Assertion failed comparing the view content and the AdHoc query result of ";
        assertTablesAreEqual(prefix + "v27", vtExpected, vtActual, EPSILON);
    }

    public void testEng11047() throws Exception {
        Client client = getClient();

        ClientResponse cr;
        VoltTable      vt;

        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-1, null, null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-1, null, null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-2, null, null, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-2, null, null, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-3, null, -333, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-3, null, -333, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-4, null, -333, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-4, null, -333, -22.22);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-5, 'eee', null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-5, 'eee', null, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-6, 'eee', null, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-6, 'eee', null, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-7, 'eee', -777, null);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-7, 'eee', -777, null);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-8, 'eee', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-8, 'eee', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-9, 'jjj', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-9, 'jjj', -777, -66.66);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-10, 'jjj', -10, -10);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-10, 'jjj', -10, -10);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-11, 'jjj', -11, -11);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-11, 'jjj', -11, -11);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-12, 'mmm', -12, -12);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-12, 'mmm', -12, -12);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-13, 'mmm', -13, -13);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-13, 'mmm', -13, -13);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-14, 'bouSWVaJwQHtrp', -16078, 5.88087039394022959016e-02);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-14, 'FOO', -16079, 9.88087039394022959016e-02);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (-15, 'NhFmPDULXEFLGI', 29960, 3.59831007623149345953e-01);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (-15, 'BAR', 29967, 7.59831007623149345953e-01);");

        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (1, 'aaa', 1, 0);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (1, 'yyy', 1, 0);");
        client.callProcedure("@AdHoc", "INSERT INTO P2_ENG_11024 VALUES (2, 'xxx', 2, 0);");
        client.callProcedure("@AdHoc", "INSERT INTO R2_ENG_11024 VALUES (2, 'zzz', 2, 0);");

        // The answers here and in the next query were determined by
        // a judicious mix of testing and clever insertion.  The last four
        // insert statements above give the values in the second test.
        cr = client.callProcedure("@AdHoc", "SELECT (A.NUM) AS Q5 FROM V21 A WHERE (A.NUM) = (A.ID - 14) ORDER BY 1 LIMIT 82;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals(-13, vt.getLong(0));

        cr = client.callProcedure("@AdHoc", "SELECT (A.NUM) AS Q5 FROM V21 A WHERE (A.NUM) = (A.ID) ORDER BY 1 LIMIT 82;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals(1, vt.getLong(0));

    }

    // Regression test for ENG-11074
    public void testEng11074() throws Exception {
        Client client = getClient();
        VoltTable      vt;

        client.callProcedure("P1_ENG_11074.Insert", 0, "foo", "bar", "baz", 6.66);
        client.callProcedure("P1_ENG_11074.Insert", 1, "baz", "foo", "bar", 6.66);
        client.callProcedure("P2_ENG_11074.Insert", 0, "alpha", "beta", "gamma", 6.66);
        client.callProcedure("P2_ENG_11074.Insert", 1, "aleph", "beth", "gimel", 6.66);

        vt = client.callProcedure("@AdHoc", "select * from vjoin_eng_11074").getResults()[0];
        assertContentOfTable(new Object[][] {{2, "gamma"}}, vt);

        vt = client.callProcedure("@AdHoc", "select * from v1_eng_11074").getResults()[0];
        assertContentOfTable(new Object[][] {{2, "bar"}}, vt);
    }

    // Repro for ENG-11080
    public void testEng11080() throws Exception {
        Client client = getClient();

        // ID, VCHAR, NUM, RATIO
        String[] stmts = {
                "INSERT INTO R1_ENG_11024 VALUES (100, null, 50,  null)",
                "INSERT INTO R1_ENG_11024 VALUES (1000, null, 50,  null)",
                "INSERT INTO R2_ENG_11024 VALUES (100, null, 50,  null)"
        };

        for (String stmt : stmts) {
            client.callProcedure("@AdHoc", stmt);
        }

        VoltTable vt;

        // SELECT list of both views is:
        //   VCHAR, COUNT(*), MAX(ID)
        // At this point both views have just one group, where the GB key is NULL.
        // Both views should have the same content because the view with the join
        // is just joining to one row.

        Object[][] expectedBeforeDelete = new Object[][] {{null, 2, 1000}};
        Object[][] expectedAfterDelete = new Object[][] {{null, 1, 100}};

        vt = client.callProcedure("@AdHoc", "select * from v_eng_11080 order by 1, 2, 3").getResults()[0];
        assertContentOfTable(expectedBeforeDelete, vt);

        vt = client.callProcedure("@AdHoc", "select * from vjoin_eng_11080 order by 1, 2, 3").getResults()[0];
        assertContentOfTable(expectedBeforeDelete, vt);

        // This deletes the current MAX value for both views
        client.callProcedure("@AdHoc", "delete from r1_eng_11024 where id = 1000");

        // In this bug we had trouble finding the new MAX or MIN for groups with
        // NULL GB keys.  Ensure that the views are still correct.

        vt = client.callProcedure("@AdHoc", "select * from v_eng_11080 order by 1, 2, 3").getResults()[0];
        assertContentOfTable(expectedAfterDelete, vt);

        vt = client.callProcedure("@AdHoc", "select * from vjoin_eng_11080 order by 1, 2, 3").getResults()[0];
        assertContentOfTable(expectedAfterDelete, vt);
    }


    public void testEng11100() throws Exception {
        Client client = getClient();
        String[] stmts = {
                "INSERT INTO P1_ENG_11074 VALUES (-2,   null,  null, 'bbb', -22.22);",
                "INSERT INTO R1_ENG_11074 VALUES (-2,   null,  null, 'bbb', -22.22);",
                // Note: following two statements violate NOT NULL constraints on rightmost column.
                "INSERT INTO P1_ENG_11074 VALUES (-3,   null, 'ccc',  null,  null);",
                "INSERT INTO R1_ENG_11074 VALUES (-3,   null, 'ccc',  null,  null);",
                "INSERT INTO P1_ENG_11074 VALUES (-4,   null, 'ccc', 'bbb', -22.22);",
                "INSERT INTO R1_ENG_11074 VALUES (-4,   null, 'ccc', 'bbb', -22.22);",
                "INSERT INTO P1_ENG_11074 VALUES (-6,  'eee',  null, 'fff', -66.66);",
                "INSERT INTO R1_ENG_11074 VALUES (-6,  'eee',  null, 'fff', -66.66);",
                "INSERT INTO P1_ENG_11074 VALUES (-8,  'eee', 'ggg', 'fff', -66.66);",
                "INSERT INTO R1_ENG_11074 VALUES (-8,  'eee', 'ggg', 'fff', -66.66);",
                "INSERT INTO P1_ENG_11074 VALUES (-9,  'jjj', 'ggg', 'fff', -66.66);",
                "INSERT INTO R1_ENG_11074 VALUES (-9,  'jjj', 'ggg', 'fff', -66.66);",
                "INSERT INTO P1_ENG_11074 VALUES (-10, 'jjj', 'jjj', 'jjj', -10);",
                "INSERT INTO R1_ENG_11074 VALUES (-10, 'jjj', 'jjj', 'jjj', -10);",
                "INSERT INTO P1_ENG_11074 VALUES (-11, 'klm', 'klm', 'klm', -11);",
                "INSERT INTO R1_ENG_11074 VALUES (-11, 'klm', 'klm', 'klm', -11);",
                "INSERT INTO P1_ENG_11074 VALUES (-12, 'lll', 'lll', 'lll', -12);",
                "INSERT INTO R1_ENG_11074 VALUES (-12, 'lll', 'lll', 'lll', -12);",
                "INSERT INTO P1_ENG_11074 VALUES (-13, 'mmm', 'mmm', 'mmm', -13);",
                "INSERT INTO R1_ENG_11074 VALUES (-13, 'mmm', 'mmm', 'mmm', -13);",
            };

        int numExc = 0;
        for (String stmt : stmts) {
            try {
                client.callProcedure("@AdHoc", stmt);
            }
            catch (ProcCallException pce) {
                String expectedMessage;
                if (isHSQL()) {
                    expectedMessage = "integrity constraint violation";
                }
                else {
                    expectedMessage = "Constraint Type NOT_NULL";
                }

                assertTrue("Unexpected message: " + pce.getMessage(),
                        pce.getMessage().contains(expectedMessage));
                ++numExc;
            }

            VoltTable expected = client.callProcedure("@AdHoc",
                    "SELECT   T1.VCHAR_INLINE_MAX,   COUNT(*),   MIN(T2.VCHAR_INLINE_MAX)   "
                    + "FROM P1_ENG_11074 T1 JOIN R1_ENG_11074 T2 ON T1.ID  <  T2.ID "
                    + "GROUP BY T1.VCHAR_INLINE_MAX "
                    + "order by 1, 2, 3").getResults()[0];
            VoltTable actual = client.callProcedure("@AdHoc",
                    "select * "
                    + "from v_eng_11100 "
                    + "order by 1, 2, 3;").getResults()[0];
            assertTablesAreEqual("Query and view after stmt: " + stmt, expected, actual);
        }

        assertEquals(numExc, 2);
    }

    public void testCreateViewWithParams() throws Exception {
        Client client = getClient();

        String expectedMsg = "Materialized view \"V\" contains placeholders \\(\\?\\), "
                + "which are not allowed in the SELECT query for a view.";
        verifyStmtFails(client,
                "create view v as "
                + "select t3.f5, count(*) "
                + "FROM t3_eng_11119 as t3 INNER JOIN T1_eng_11119 as t1 "
                + "ON T1.f1 = T3.f4 "
                + "WHERE T3.f4 = ? "
                + "group by t3.f5;",
                expectedMsg);

        verifyStmtFails(client,
                "create view v as "
                + "select t3.f5, count(*) "
                + "FROM t3_eng_11119 as t3 "
                + "WHERE T3.f4 = ? "
                + "group by t3.f5;",
                expectedMsg);
    }

    public void testEng11203() throws Exception {
        // This test case has AdHoc DDL, so it cannot be ran in the HSQL backend.
        if (! isHSQL()) {
            Client client = getClient();
            Object[][] initialRows = {{"ENG_11203_A", 1, 2, 4}, {"ENG_11203_B", 1, 2, 4}};
            Object[][] secondRows = {{"ENG_11203_A", 6, 2, 4}, {"ENG_11203_B", 6, 2, 4}};
            // This test case tests ENG-11203, verifying that on single table views,
            // if a new index was created on the view target table, this new index
            // will be properly tracked by the MaterializedViewTriggerForInsert.

            // - 1 - Insert the initial data into the view source table.
            insertRow(client, initialRows[0]);
            insertRow(client, initialRows[1]);
            VoltTable vt = client.callProcedure("@AdHoc",
                    "SELECT * FROM V_ENG_11203_SINGLE").getResults()[0];
            assertContentOfTable(new Object[][] {{2, 1, 1}}, vt);
            vt = client.callProcedure("@AdHoc",
                    "SELECT * FROM V_ENG_11203_JOIN").getResults()[0];
            assertContentOfTable(new Object[][] {{2, 1, 1}}, vt);

            // - 2 - Now add a new index on the view target table.
            try {
                client.callProcedure("@AdHoc",
                    "CREATE INDEX I_ENG_11203_SINGLE ON V_ENG_11203_SINGLE(a, b);");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on the single table view V_ENG_11203_SINGLE.");
            }
            try {
                client.callProcedure("@AdHoc",
                    "CREATE INDEX I_ENG_11203_JOIN ON V_ENG_11203_JOIN(a, b);");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to create an index on the joined table view V_ENG_11203_JOIN.");
            }

            // - 3 - Insert another row of data.
            insertRow(client, secondRows[0]);
            insertRow(client, secondRows[1]);
            vt = client.callProcedure("@AdHoc",
                "SELECT * FROM V_ENG_11203_SINGLE").getResults()[0];
            assertContentOfTable(new Object[][] {{2, 2, 6}}, vt);
            vt = client.callProcedure("@AdHoc",
                "SELECT * FROM V_ENG_11203_JOIN").getResults()[0];
            assertContentOfTable(new Object[][] {{2, 4, 6}}, vt);

            // - 4 - Start to delete rows.
            // If the new index was not tracked properly, the server will start to crash
            // because the newly-inserted row was not inserted into the index.
            deleteRow(client, initialRows[0]);
            deleteRow(client, initialRows[1]);
            deleteRow(client, secondRows[0]);
            deleteRow(client, secondRows[1]);
            vt = client.callProcedure("@AdHoc",
                "SELECT * FROM V_ENG_11203_SINGLE").getResults()[0];
            assertContentOfTable(new Object[][] {}, vt);
            vt = client.callProcedure("@AdHoc",
                "SELECT * FROM V_ENG_11203_JOIN").getResults()[0];
            assertContentOfTable(new Object[][] {}, vt);
        }
    }

    public void testEng11314() throws Exception {
        Client client = getClient();
        String[] insertT1 = {
            "INSERT INTO T1_ENG_11314 (G1, C2, C3) VALUES (1, 1024, 64);",
            "INSERT INTO T1_ENG_11314 (G1, C2, C3) VALUES (2, 2048, 32);"
        };
        String insertT2 = "INSERT INTO T2_ENG_11314 (G0) VALUES (0);";
        String bugTrigger = "UPDATE T1_ENG_11314 SET C2=64, C3=1024 WHERE G1=2;";
        Object[][] viewContent = { {0, 2, 0, 0, 1024, 64, 0, 0, 0, 0, 0, 0, "abc", "def"} };
        // -1- Insert data
        insertRowAdHoc(client, insertT1[0]);
        insertRowAdHoc(client, insertT1[1]);
        insertRowAdHoc(client, insertT2);
        // -2- Test if the UPDATE statement will trigger an error on single table view V1:
        client.callProcedure("@AdHoc", bugTrigger);
        // -3- Verify view contents
        VoltTable vt = client.callProcedure("@AdHoc",
                "SELECT * FROM V1_ENG_11314").getResults()[0];
            assertContentOfTable(viewContent, vt);
        vt = client.callProcedure("@AdHoc",
                "SELECT * FROM V2_ENG_11314").getResults()[0];
            assertContentOfTable(viewContent, vt);
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaterializedViewSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        URL url = AddPerson.class.getResource("matviewsuite-ddl.sql");
        String schemaPath = url.getPath();
        project.addSchema(schemaPath);

        project.addProcedures(PROCEDURES);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Sites/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        LocalCluster config = new LocalCluster("matview-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        //* enable for simplified config */ config = new LocalCluster("matview-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("matview-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 3-node k=1 cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("matview-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }
}
