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
import java.net.URL;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
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
            // System.out.println(countTable);
            ++nStatement;
            long count = countTable.asScalarLong();
            assertEquals("COUNT statement " + nStatement + "/" +
            results.length + " should have found no undeleted rows.", 0, count);
        }
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

        results = client.callProcedure("DEPT_PEOPLE.insert", 1, 1L, 31L, 1000.00, 3).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());
        results = client.callProcedure("DEPT_PEOPLE.insert", 2, 1L, 31L, 900.00, 5).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());
        results = client.callProcedure("DEPT_PEOPLE.insert", 3, 1L, 31L, 900.00, 1).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());
        results = client.callProcedure("DEPT_PEOPLE.insert", 4, 1L, 31L, 2500.00, 5).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());

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

        results = client.callProcedure("DEPT_PEOPLE.insert", 1, 1L, 31L, 1000.00, 3).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());
        results = client.callProcedure("DEPT_PEOPLE.insert", 2, 1L, 31L, 900.00, 5).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());
        results = client.callProcedure("DEPT_PEOPLE.insert", 3, 1L, 31L, 900.00, 1).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());
        results = client.callProcedure("DEPT_PEOPLE.insert", 4, 1L, 31L, 2500.00, 5).getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].getRowCount());

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


    public void testMPAndRegressions() throws IOException, ProcCallException
    {
        subtestMultiPartitionSimple();
        subtestInsertReplicated();
        subtestInsertAndOverflowSum();
        subtestENG798();
        subtestIndexed();
        subtestMinMaxMultiPartition();
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
        results = client.callProcedure("CONTEST.insert",
            "Senior", timestampInitializer, "Boston", "Jack").getResults();
        assertEquals(1, results.length);
        assertEquals(1L, results[0].asScalarLong());

        // +1 V_TEAM_MEMBERSHIP, +4 V_TEAM_TIMES
        for (ii = 0; ii < 4; ++ii) {
            timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
            results = client.callProcedure("CONTEST.insert",
                "Senior", timestampInitializer, "Cambridge", "anonymous " + ii).getResults();
            assertEquals(1, results.length);
            assertEquals(1L, results[0].asScalarLong());
        }

        // +0 V_TEAM_MEMBERSHIP, +1 V_TEAM_TIMES
        timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
        for (ii = 0; ii < 3; ++ii) {
            results = client.callProcedure("CONTEST.insert",
                "Senior", timestampInitializer, "Boston",  "not Jack " + ii).getResults();
            assertEquals(1, results.length);
            assertEquals(1L, results[0].asScalarLong());
        }

        // +1 V_TEAM_MEMBERSHIP, +1 V_TEAM_TIMES
        timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
        for (ii = 0; ii < 3; ++ii) {
            results = client.callProcedure("CONTEST.insert",
                "Senior", timestampInitializer, "Concord", "Emerson " + ii).getResults();
            assertEquals(1, results.length);
            assertEquals(1L, results[0].asScalarLong());
        }

        // +1 V_TEAM_MEMBERSHIP, +2 V_TEAM_TIMES
        for (ii = 0; ii < 2; ++ii) {
            timestampInitializer = (System.currentTimeMillis() + (++delay))*1000;
            results = client.callProcedure("CONTEST.insert",
                "Senior", timestampInitializer, "Lexington", "Luis " + ii).getResults();
            assertEquals(1, results.length);
            assertEquals(1L, results[0].asScalarLong());
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
             status code: -128 column count: 3
             cols (RUNNER_CLASS:STRING), (TEAM:STRING), (TOTAL:INTEGER),
             rows -
              Senior,Boston,4
              Senior,Cambridge,4
              Senior,Concord,3
              Senior,Lexington,2
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
