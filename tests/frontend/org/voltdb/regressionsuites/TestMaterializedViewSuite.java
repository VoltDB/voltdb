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
import org.voltdb_testprocs.regressionsuites.matviewprocs.UpdatePerson;

import com.google_voltpatches.common.collect.Lists;

import junit.framework.Test;


public class TestMaterializedViewSuite extends RegressionSuite {

    // Constants to control whether to abort a procedure invocation with explicit sabotage
    // or to allow it to run normally.
    private static final int SABOTAGE = 2;
    private static final int NORMALLY = 0;

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        AddPerson.class, DeletePerson.class, UpdatePerson.class, AggAges.class,
        SelectAllPeople.class, AggThings.class, AddThing.class, OverflowTest.class,
        Eng798Insert.class, TruncateMatViewDataMP.class
    };

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
            long count = countTable.asScalarLong();
            assertEquals("COUNT statement " + nStatement + "/" +
            results.length + " should have found no undeleted rows.", 0, count);
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
        assertTablesAreEqual(prefix + "VENG6511: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511expL ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1+1, d2*2, COUNT(*), MIN(v2) AS vmin, MAX(v2) AS vmax FROM ENG6511 GROUP BY d1+1, d2*2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511expL: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511expR ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(abs(v1)) AS vmax FROM ENG6511 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511expR: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511expLR ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1+1, d2*2, COUNT(*), MIN(v2-1) AS vmin, MAX(v2-1) AS vmax FROM ENG6511 GROUP BY d1+1, d2*2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511expLR: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511C ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(v1) AS vmin, MAX(v1) AS vmax FROM ENG6511 WHERE v1 > 4 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511C: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511TwoIndexes ORDER BY d1, d2;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT d1, d2, COUNT(*), MIN(abs(v1)) AS vmin, MAX(v2) AS vmax FROM ENG6511 WHERE v1 > 4 GROUP BY d1, d2 ORDER BY 1, 2;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511TwoIndexes: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM VENG6511NoGroup ORDER BY 1, 2, 3;").getResults()[0];
        tresult = client.callProcedure("@AdHoc", "SELECT COUNT(*), MIN(v1) AS vmin, MAX(v2) AS vmax FROM ENG6511 ORDER BY 1, 2, 3;").getResults()[0];
        assertTablesAreEqual(prefix + "VENG6511NoGroup: ", tresult, vresult);
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

        results = client.callProcedure("@AdHoc", "SELECT * FROM MATPEOPLE2").getResults();
        assert(results != null);
        assertEquals(1, results.length);
        t = results[0];
        assertEquals(2, t.getRowCount());
        System.out.println(t.toString());
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
        assertTablesAreEqual(prefix + "ORDER_COUNT_NOPCOL: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_COUNT_GLOBAL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_COUNT_GLOBAL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_COUNT_GLOBAL: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_DETAIL_NOPCOL ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_DETAIL_NOPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_DETAIL_NOPCOL: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER_DETAIL_WITHPCOL ORDER BY 1, 2;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER_DETAIL_WITHPCOL").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER_DETAIL_WITHPCOL: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM ORDER2016 ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_ORDER2016").getResults()[0];
        assertTablesAreEqual(prefix + "ORDER2016: ", tresult, vresult);

        vresult = client.callProcedure("@AdHoc", "SELECT * FROM QTYPERPRODUCT ORDER BY 1;").getResults()[0];
        tresult = client.callProcedure("PROC_QTYPERPRODUCT").getResults()[0];
        assertTablesAreEqual(prefix + "QTYPERPRODUCT: ", tresult, vresult);
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
        // to the corresponding source tables, then update each row with the data
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
        truncateBeforeTest(client);

        // Merge two sub-lists for the following tests.
        dataList1.addAll(dataList2);

        // -- 2 -- Test inserting the data into the source tables.
        // We do a shuffle here and in the delete test. But I do believe we still
        // have the full coverage of all the cases because we are inserting and deleting
        // all the rows. The cases updating values of all kinds of aggregations will be
        // tested in one row or another.
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
                client.callProcedure("@AdHoc", "ALTER TABLE ORDERITEMS ADD COLUMN x FLOAT;");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to add column to a view source table.");
            }
            verifyViewOnJoinQueryResult(client);
            // 3.2 drop column
            try {
                client.callProcedure("@AdHoc", "ALTER TABLE ORDERITEMS DROP COLUMN x;");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to drop column on a view source table.");
            }
            verifyViewOnJoinQueryResult(client);
            // 3.3 alter column
            try {
                client.callProcedure("@AdHoc", "ALTER TABLE CUSTOMERS ALTER COLUMN ADDRESS VARCHAR(100);");
            } catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Should be able to alter column in a view source table.");
            }
            verifyViewOnJoinQueryResult(client);
        }

        // -- 4 -- Test defining view after the data is loaded.
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
                    "CREATE VIEW ORDER_DETAIL_WITHPCOL (NAME, ORDER_ID, CNT, SUMAMT, MINUNIT, MAXUNIT, ITEMCOUNT) AS " +
                    "SELECT " +
                        "CUSTOMERS.NAME, " +
                        "ORDERS.ORDER_ID, " +
                        "COUNT(*), " +
                        "SUM(PRODUCTS.PRICE * ORDERITEMS.QTY), " +
                        "MIN(PRODUCTS.PRICE), " +
                        "MAX(PRODUCTS.PRICE), " +
                        "COUNT(ORDERITEMS.PID) " +
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

        // -- 5 -- Test deleting the data from the source tables.
        Collections.shuffle(dataList1);
        System.out.println("Now testing deleting data from the join query view source table.");
        for (int i=0; i<dataList1.size(); i++) {
            deleteRow(client, dataList1.get(i));
            verifyViewOnJoinQueryResult(client);
        }
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
        URL url = AddPerson.class.getResource("matviewsuite-ddl.sql");
        url.getPath();

        String schemaPath = url.getPath();

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaterializedViewSuite.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        //VoltServerConfig config = new LocalSingleProcessServer("matview-onesite.jar", 1, BackendTarget.NATIVE_EE_IPC);
        VoltServerConfig config = new LocalCluster("matview-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
        project.addSchema(schemaPath);

        project.addProcedures(PROCEDURES);
        // build the jarfile
        boolean success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("matview-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 3-node k=1 cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("matview-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
