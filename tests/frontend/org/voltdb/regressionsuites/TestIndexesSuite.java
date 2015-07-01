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
import java.util.HashSet;
import java.util.TreeSet;

import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.indexes.CheckMultiMultiIntGTEFailure;
import org.voltdb_testprocs.regressionsuites.indexes.CompiledInLists;
import org.voltdb_testprocs.regressionsuites.indexes.Insert;

/**
 * Actual regression tests for SQL that I found that was broken and
 * have fixed.  Didn't like any of the other potential homes that already
 * existed for this for one reason or another.
 */

public class TestIndexesSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class,
        CheckMultiMultiIntGTEFailure.class, CompiledInLists.class};

    // Index stuff to test:
    // scans against tree
    // - < <= = > >=, range with > and <
    // - single column
    // - multi-column
    // - multi-map

    //
    // Multimap multi column, indexing only on prefix key
    // @throws IOException
    // @throws ProcCallException
    //
    public void testOrderedMultiMultiPrefixOnly()
    throws IOException, ProcCallException
    {
        String[] tables = {"P3", "R3"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s T where T.NUM > 100", table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());

            String queryEq = String.format("select * from %s T where T.NUM = 200", table);
            VoltTable[] resultsEq = client.callProcedure("@AdHoc", queryEq).getResults();
            assertEquals(2, resultsEq[0].getRowCount());
        }
    }

    public void testParameterizedLimitOnIndexScan()
    throws IOException, ProcCallException {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);

            VoltTable[] results = client.callProcedure("Eng397LimitIndex" + table, new Integer(2)).getResults();
            assertEquals(2, results[0].getRowCount());
        }
    }

    public void testPushDownAggregateWithLimit() throws Exception {
        String[] tables = {"R1", "P1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            client.callProcedure("Insert", table, 9, "h", 300, 8, 19.5);

            String sql = String.format("select T.ID, MIN(T.ID) from %s T group by T.ID order by T.ID limit 4",
                    table);
            VoltTable results = client.callProcedure("@AdHoc", sql).getResults()[0];
            System.out.println(results);
        }
    }

    public void testNaNInIndexes() throws Exception {
        // current hsql seems to fail on null handling
        if (isHSQL()) return;

        Client client = getClient();

        int i = 0;
        for (int j = 0; j < 20; j++) {
            client.callProcedure("R1IX.insert", i++, "a", 100 * i, 0.0 / 0.0);
            client.callProcedure("R1IX.insert", i++, "b", 100 * i, 16.5);
            client.callProcedure("R1IX.insert", i++, "c", 100 * i, 119.5);
            client.callProcedure("R1IX.insert", i++, "d", 100 * i, 9.5);
            client.callProcedure("R1IX.insert", i++, "e", 100 * i, 1.0 / 0.0);
            client.callProcedure("R1IX.insert", i++, "f", 100 * i, -14.5);
            client.callProcedure("R1IX.insert", i++, "g", 100 * i, 0.0 / 0.0);
            client.callProcedure("R1IX.insert", i++, "h", 100 * i, 14.5);
            client.callProcedure("R1IX.insert", i++, "i", 100 * i, 14.5);
            client.callProcedure("R1IX.insert", i++, "j", 100 * i, 1.0 / 0.0);
            client.callProcedure("R1IX.insert", i++, "k", 100 * i, 14.5);
            client.callProcedure("R1IX.insert", i++, "l", 100 * i, 0.0 / 0.0);
            client.callProcedure("R1IX.insert", i++, "m", 100 * i, 11.5);
            client.callProcedure("R1IX.insert", i++, "n", 100 * i, 10.5);
        }

        VoltTable results = client.callProcedure("@AdHoc", "delete from R1IX;").getResults()[0];
        System.out.println(results);
    }

    public void testOrderedUniqueOneColumnIntIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s T where T.ID > 1",
                                         table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(5, results[0].getRowCount());
            // make sure that we work if the value we want isn't present
            query = String.format("select * from %s T where T.ID > 4",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID > 8",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID >= 1",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID >= 4",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID >= 9",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID > 1 and T.ID < 6",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID > 1 and T.ID <= 6",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID > 1 and T.ID <= 5",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s T where T.ID >= 1 and T.ID < 7",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            // Check that >= work in conjunction with <
            // run over the end of the index to catch the keyIterate bug
            // in the first >= index fix
            query = String.format("select * from %s T where T.ID >= 1 and T.ID < 10",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            // XXX THIS CASE CURRENTLY FAILS
            // SEE TICKET 194
//            query = String.format("select * from %s T where T.ID >= 2.9",
//                                  table);
//            results = client.callProcedure("@AdHoc", query);
//            assertEquals(4, results[0].getRowCount());
        }
    }

    //
    // Multimap single column
    // @throws IOException
    // @throws ProcCallException
    //
    public void testOrderedMultiOneColumnIntIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s T where T.NUM > 100",
                                         table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM > 150",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM > 300",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM >= 100",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM >= 150",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM >= 301",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM > 100 and T.NUM < 300",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            // Check that >= work in conjunction with <
            // run over the end of the index to catch the keyIterate bug
            // in the first >= index fix
            query = String.format("select * from %s T where T.NUM >= 100 and T.NUM < 400",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM = 100",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM > 100 and T.NUM <= 300",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM > 100 and T.NUM <= 250",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s T where T.NUM > 100 and T.NUM <= 250",
                                  table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
        }
    }

    /**
     * Multimap one column less than.
     */
    public void testOrderedMultiOneColumnIndexLessThan()
    throws IOException, ProcCallException
    {
        Client client = getClient();
        client.callProcedure("Insert", "P3", 0, "a", 1, 2, 1.0);
        client.callProcedure("Insert", "P3", 1, "b", 1, 2, 2.0);
        client.callProcedure("Insert", "P3", 2, "c", 2, 3, 3.0);
        client.callProcedure("Insert", "P3", 3, "d", 3, 4, 4.0);
        client.callProcedure("Insert", "P3", 4, "e", 4, 5, 5.0);
        client.callProcedure("Insert", "P3", 5, "f", 5, 6, 6.0);
        client.callProcedure("Insert", "P3", 6, "g", 5, 6, 7.0);

        VoltTable result = client.callProcedure("@AdHoc", "select * from P3 where NUM < 5 order by num desc")
                                 .getResults()[0];
        assertEquals(5, result.getRowCount());
        TreeSet<Integer> ids = new TreeSet<Integer>();
        while (result.advanceRow()) {
            assertFalse(ids.contains((int) result.getLong("ID")));
            ids.add((int) result.getLong("ID"));
        }

        int i = 0;
        for (int id : ids) {
            assertEquals(i++, id);
        }

        result = client.callProcedure("@AdHoc", "select * from P3 where NUM < 1 order by num desc")
                       .getResults()[0];
        assertEquals(0, result.getRowCount());

        result = client.callProcedure("@AdHoc", "select * from P3 where NUM < 4 order by num desc")
                       .getResults()[0];
        assertEquals(4, result.getRowCount());
    }

    private static void compareTable(VoltTable vt, Object [][] expected) {
        int len = expected.length;
        assertEquals(len, vt.getRowCount());
        for (int i=0; i < len; i++) {
            compareRow(vt, expected[i]);
        }
    }

    private static void compareRow(VoltTable vt, Object [] expected) {
        assertTrue(vt.advanceRow());
        assertEquals( ((Integer)expected[0]).intValue(), vt.getLong(0));
        assertEquals( ((String)expected[1]), vt.getString(1));
        assertEquals( ((Integer)expected[2]).intValue(), vt.getLong(2));
        assertEquals( ((Integer)expected[3]).intValue(), vt.getLong(3));
        assertEquals( ((Double)expected[4]).doubleValue(), vt.getDouble(4), 0.001);
    }

    public void testInList()
            throws IOException, ProcCallException
    {
        String[] tables = {"P3", "R3"};
        Object [] line1 = new Object[] {1, "a", 100, 1, 14.5};
        Object [] line2 = new Object[] {2, "b", 100, 2, 15.5};
        Object [] line3 = new Object[] {3, "c", 200, 3, 16.5};
        Object [] line6 = new Object[] {6, "f", 200, 6, 17.5};
        Object [] line7 = new Object[] {7, "g", 300, 7, 18.5};
        Object [] line8 = new Object[] {8, "h", 300, 8, 19.5};

        Client client = getClient();
        String query;
        VoltTable[] results;

        // Try to repro ENG-5537, the error found by a user query.
        // We dropped an IN LIST filter when it was not a candidate for
        // NestLoopIndexJoin optimization.
        results = client.callProcedure("@AdHoc", "INSERT INTO tableX VALUES (1, 10, 1, 'one', 31, 41);").getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "INSERT INTO tableX VALUES (2, 20, 1, 'two', 32, 42);").getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "INSERT INTO tableX VALUES (3, 30, 1, 'three', 32, 42);").getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "INSERT INTO tableX VALUES (5, 50, 5, 'one', 35, 45);").getResults();
        assertEquals(1, results[0].asScalarLong());

        results = client.callProcedure("@AdHoc", "INSERT INTO tableY VALUES (1, 10, 1000, 10000);").getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "INSERT INTO tableY VALUES (2, 20, 2000, 20000);").getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "INSERT INTO tableY VALUES (3, 30, 3000, 30000);").getResults();
        assertEquals(1, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "INSERT INTO tableY VALUES (5, 50, 1000, 10000);").getResults();
        assertEquals(1, results[0].asScalarLong());

        query = "SELECT amps.keyB " +
                "   FROM tableX amps INNER JOIN tableY ohms " +
                "     ON amps.keyA = ohms.keyA " +
                "    AND amps.keyB = ohms.keyB " +
                " WHERE " +
                "     amps.keyC = 1 AND" +
                "     amps.keyD IN ('one','two') AND" +
                "     ohms.keyH IN (1000,3000) " +
                " ORDER BY amps.sort1 DESC; " +
                "";
//DEBUG        results = client.callProcedure("@Explain", query).getResults();
//DEBUG        System.out.println(results[0]);

        results = client.callProcedure("@AdHoc", query).getResults();
        System.out.println(results[0]);
        try {
            assertEquals(10, results[0].asScalarLong());
        } catch (IllegalStateException not_one) {
            fail("IN LIST test query rerurned wrong number of rows: " + not_one);
        }
/* TODO: enable and investigate:
 queries like this were causing column index resolution errors.
 @AdHoc (vs. just @Explain) may be required to repro?
        query = "select * from R3, P3 where R3.NUM2 = P3.NUM2 " +
            " and R3.NUM IN (200, 300)" +
            " and P3.NUM IN (200, 300)" +
            "";
            results = client.callProcedure("@Explain", query).getResults();
            System.out.println(results[0]);

        query = "select * from R3, P3 where R3.NUM2 = P3.NUM2 " +
            " and P3.NUM IN (200, 300)" +
            "";
            results = client.callProcedure("@Explain", query).getResults();
            System.out.println(results[0]);
*/
        for (String table : tables) {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);

            query = String.format("select * from %s T where T.NUM IN (200, 300) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.NUM IN (10, 200, 300, -1) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.NUM IN (10, 200, 300, -1, 200) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.NUM IN (200) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6});

            query = String.format("select * from %s T where T.NUM IN (10)", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());

            //query = String.format("select * from %s T where T.NUM IN ()", table);
            //results = client.callProcedure("@AdHoc", query).getResults();
            //assertEquals(0, results[0].getRowCount());

            query = String.format("select * from %s T where T.DESC IN ('c', 'f', 'g', 'h') ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.DESC IN ('', 'c', 'f', 'g', 'h', " +
                "'a value with some length to it in case there are object allocation issues'" +
                ") ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});


            query = String.format("select * from %s T where T.DESC " +
                    "IN ('', 'c', 'f', 'g', 'h', 'f') ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.DESC IN ('a')", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            compareRow(results[0], line1);

            query = String.format("select * from %s T where T.DESC IN ('b')", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            compareRow(results[0], line2);

            query = String.format("select * from %s T where T.DESC IN ('')", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());


            query = String.format("select * from %s T where T.DESC IN ('c', 'f', 'g', 'h')" +
                " and T.NUM IN (200, 300) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.DESC IN ('', 'c', 'f', 'g', 'h', " +
                "'a value with some length to it in case there are object allocation issues'" +
                ")" +
                " and T.NUM IN (10, 200, 300, -1) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.DESC IN ('', 'c', 'f', 'g', 'h', 'f')" +
                " and T.NUM IN (10, 200, 300, -1, 200) ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line3,line6,line7,line8});

            query = String.format("select * from %s T where T.DESC IN ('a')" +
                    " and T.NUM IN (100)", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            compareRow(results[0], line1);

            query = String.format("select * from %s T where T.DESC IN ('b')" +
                " and T.NUM IN (100)", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            compareRow(results[0], line2);

            query = String.format("select * from %s T where T.DESC IN ('')" +
                 " and T.NUM IN (10)", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());

//            Current table is P3, results:
//                header size: 46
//                status code: -128 column count: 5
//                cols (ID:INTEGER), (DESC:STRING), (NUM:INTEGER), (NUM2:INTEGER), (RATIO:FLOAT),
//                rows -
//                 3,c,200,3,16.5
//                 6,f,200,6,17.5
//                 1,a,100,1,14.5
//                 7,g,300,7,18.5
//                 2,b,100,2,15.5
//                 8,h,300,8,19.5

            // try some DML -- but try not to actually update values except to themselves
            // -- that just makes it harder to profile expected results down the line
            query = String.format("delete from %s where DESC IN ('')" +
                    " and NUM IN (111,112)", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            System.out.println("Delete results:" + results[0]);
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(0, results[0].getLong(0));

            query = String.format("select * from %s T ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line1,line2,line3,line6,line7,line8});

            // Try delete with in
            query = String.format("delete from %s where DESC IN ('x','y', 'b','z')" +
                    " and NUM IN (119,100)", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(1, results[0].getLong(0));

            query = String.format("select * from %s T ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(5, results[0].getRowCount());
            compareTable(results[0], new Object [][] {line1,line3,line6,line7,line8});

            results = client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(1, results[0].getLong(0));

            // Test update with IN
            query = String.format("update %s set num2 = 10 where DESC IN ('x', 'y', 'z', 'c')", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(1, results[0].getLong(0));

            query = String.format("select id, desc from %s where num2 = 10 ", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(3, results[0].getLong(0));
            assertEquals("c", results[0].getString(1));

            query = String.format("update %s set num2 = 3 where DESC = 'c'", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(1, results[0].getLong(0));

            query = String.format("select * from %s T ORDER BY T.ID", table);
            results = client.callProcedure("@AdHoc", query).getResults();
            compareTable(results[0], new Object [][] {line1,line2,line3,line6,line7,line8});

        }

        // Flag whether CompiledInLists needs to tiptoe around lack of "col IN ?" support
        // in the HSQL backend that doesn't support it.
        int hsql = isHSQL() ? 1 : 0;
        String[] fewdescs = new String[] { "", "b", "no match", "this either",
        "and last but not least the obligatory longish value to test object allocation" };
        int[] fewnums = new int[] { 10, 100, 100, 100, -1 };
        results = client.callProcedure("CompiledInLists", fewdescs, fewnums, hsql).getResults();
        assertEquals(6, results.length);
        assertEquals(1, results[0].getRowCount());
        assertEquals(1, results[1].getRowCount());
        assertEquals(1, results[2].getRowCount());
        assertEquals(2, results[3].getRowCount());
        assertEquals(2, results[4].getRowCount());
        assertEquals(2, results[5].getRowCount());

        String[] manydescs = new String[] { "b", "c", "f", "g", "h" };
        int[] manynums = new int[] { 100, 200, 300, 200, 100 };
        results = client.callProcedure("CompiledInLists", manydescs, manynums, hsql).getResults();
        assertEquals(6, results.length);
        assertEquals(5, results[0].getRowCount());
        assertEquals(5, results[1].getRowCount());
        assertEquals(5, results[2].getRowCount());
        assertEquals(4, results[3].getRowCount());
        assertEquals(4, results[4].getRowCount());
        assertEquals(4, results[5].getRowCount());

        Integer fewObjNums[] = new Integer[fewnums.length];
        for (int ii = 0; ii < fewnums.length; ++ii) {
            fewObjNums[ii] = new Integer(fewnums[ii]);
        }

        Integer manyObjNums[] = new Integer[manynums.length];
        for (int ii = 0; ii < manynums.length; ++ii) {
            manyObjNums[ii] = new Integer(manynums[ii]);
        }

        results = client.callProcedure("InlinedInListP3with5DESCs", (Object[])fewdescs).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results = client.callProcedure("InlinedInListR3with5DESCs", (Object[])fewdescs).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        if ( ! isHSQL()) {
            results = client.callProcedure("InlinedInListP3withDESCs", (Object)fewdescs).getResults();
            assertEquals(1, results.length);
            assertEquals(1, results[0].getRowCount());
        }
        results = client.callProcedure("InlinedInListP3with5NUMs",  (Object[])fewObjNums).getResults();
        assertEquals(1, results.length);
        assertEquals(2, results[0].getRowCount());
        results = client.callProcedure("InlinedInListR3with5NUMs",  (Object[])fewObjNums).getResults();
        assertEquals(1, results.length);
        assertEquals(2, results[0].getRowCount());
        // Passing Object vectors as single parameters is not allowed.
        // if ( ! isHSQL()) {
        //     results = client.callProcedure("InlinedInListP3withNUMs", (Object)fewObjNums).getResults();
        //     assertEquals(1, results.length);
        //     assertEquals(2, results[0].getRowCount());
        // }
        //TODO: test as type failure:
        //results = client.callProcedure("InlinedInListP3with5NUMs", fewnums).getResults();
        //TODO: test as type failure:
        //results = client.callProcedure("InlinedInListR3with5NUMs", fewnums).getResults();
        if ( ! isHSQL()) {
            results = client.callProcedure("InlinedInListP3withNUMs", fewnums).getResults();
            assertEquals(1, results.length);
            assertEquals(2, results[0].getRowCount());
        }

        results = client.callProcedure("InlinedInListP3with5DESCs", (Object[])manydescs).getResults();
        assertEquals(1, results.length);
        assertEquals(5, results[0].getRowCount());
        results = client.callProcedure("InlinedInListR3with5DESCs", (Object[])manydescs).getResults();
        assertEquals(1, results.length);
        assertEquals(5, results[0].getRowCount());
        if ( ! isHSQL()) {
            results = client.callProcedure("InlinedInListP3withDESCs", (Object)manydescs).getResults();
            assertEquals(1, results.length);
            assertEquals(5, results[0].getRowCount());
        }
        results = client.callProcedure("InlinedInListP3with5NUMs",  (Object[])manyObjNums).getResults();
        assertEquals(1, results.length);
        assertEquals(4, results[0].getRowCount());
        results = client.callProcedure("InlinedInListR3with5NUMs",  (Object[])manyObjNums).getResults();
        assertEquals(1, results.length);
        assertEquals(4, results[0].getRowCount());
        // Passing Object vectors as single parameters is not allowed.
        // if ( ! isHSQL()) {
        //     results = client.callProcedure("InlinedInListP3withNUMs", (Object)manyObjNums).getResults();
        //     assertEquals(1, results.length);
        //     assertEquals(4, results[0].getRowCount());
        //        }
        //TODO: test as type failure:
        //results = client.callProcedure("InlinedInListP3with5NUMs", manynums).getResults();
        //TODO: test as type failure:
        //results = client.callProcedure("InlinedInListR3with5NUMs", manynums).getResults();
        if ( ! isHSQL()) {
            results = client.callProcedure("InlinedInListP3withNUMs", manynums).getResults();
            assertEquals(1, results.length);
            assertEquals(4, results[0].getRowCount());
        }

        // Confirm that filters get the expected number of rows before trying the DML that uses them.
        results = client.callProcedure("@AdHoc", "select count(*) from R3 where DESC IN ('x', 'y', 'z', 'a')" +
                                                 " and NUM IN (1010, 1020, 1030, -1040, 100)").getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(1, results[0].getLong(0));

        results = client.callProcedure("@AdHoc", "select count(*) from P3 where DESC IN ('x', 'y', 'z', 'b')" +
                                                 " and NUM IN (1010, 1020, 1030, -1040, 100)").getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(1, results[0].getLong(0));

        // Test IN LIST DML interaction ENG-4909 -- this is a plan correctness test --
        results = client.callProcedure("@AdHoc", "update R3 set NUM = (1000) where DESC IN ('x', 'y', 'z', 'a')" +
                                                 " and NUM IN (1010, 1020, 1030, -1040, 100)").getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(1, results[0].getLong(0));

        results = client.callProcedure("@AdHoc", "delete from P3 where DESC IN ('x', 'y', 'z', 'b')" +
                                                 " and NUM IN (1010, 1020, 1030, -1040, 100)").getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(1, results[0].getLong(0));

    }

    public void testTicket195()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s T where T.NUM >= 100 and T.NUM <= 400",
                                  table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
        }
    }

    //
    // Multimap multi column
    // @throws IOException
    // @throws ProcCallException
    //
    public void testOrderedMultiMultiColumnIntIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P3", "R3"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "a", 100, 1, 14.5);
            client.callProcedure("Insert", table, 2, "b", 100, 2, 15.5);
            client.callProcedure("Insert", table, 3, "c", 200, 3, 16.5);
            client.callProcedure("Insert", table, 6, "f", 200, 6, 17.5);
            client.callProcedure("Insert", table, 7, "g", 300, 7, 18.5);
            client.callProcedure("Insert", table, 8, "h", 300, 8, 19.5);
            String query = String.format("select * from %s T where T.NUM > 100 AND T.NUM2 > 1",
                                         table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
        }
    }

    public void testOrderedMultiMultiIntGTEFailure()
    throws IOException, ProcCallException
    {
        final Client client = getClient();
        final VoltTable results[] = client.callProcedure("CheckMultiMultiIntGTEFailure").getResults();
        if (results == null) {
            fail();
        }
        //
        // Must pass 10 tests
        //
        assertEquals(10, results.length);

        // Start off easy, with COUNT(*)s
        // Actually, these exercise a different (counted index) code path which has experienced its own regressions.
        // Test 1 -- count EQ first component of compound key
        int tableI = 0;
        final VoltTableRow countEQ = results[tableI].fetchRow(0);
        assertEquals( 2, countEQ.getLong(0));

        // Test 2 -- count GTE first component of compound key
        tableI++;
        final VoltTableRow countGT = results[tableI].fetchRow(0);
        assertEquals( 3, countGT.getLong(0));

        // Test 3 -- count GT first component of compound key
        tableI++;
        final VoltTableRow countGTE = results[tableI].fetchRow(0);
        assertEquals( 1, countGTE.getLong(0));

        // Test 4 -- count LTE first component of compound key
        tableI++;
        final VoltTableRow countLTE = results[tableI].fetchRow(0);
        assertEquals( 3, countLTE.getLong(0));

        // Test 5 -- count LT first component of compound key
        tableI++;
        final VoltTableRow countLT = results[tableI].fetchRow(0);
        assertEquals( 1, countLT.getLong(0));

        // Test 6 -- EQ first component of compound key
        tableI++;
        int rowI = 0;
        assertEquals( 2, results[tableI].getRowCount());
        final VoltTableRow rowEQ0 = results[tableI].fetchRow(rowI++);
        assertEquals( 0, rowEQ0.getLong(0));
        assertEquals( 0, rowEQ0.getLong(1));

        final VoltTableRow rowEQ1 = results[tableI].fetchRow(rowI++);
        assertEquals( 0, rowEQ1.getLong(0));
        assertEquals( 1, rowEQ1.getLong(1));

        // Test 7 -- GTE first component of compound key
        tableI++;
        rowI = 0;
        assertEquals( 3, results[tableI].getRowCount());
        final VoltTableRow rowGTE0 = results[tableI].fetchRow(rowI++);
        assertEquals( 0, rowGTE0.getLong(0));
        assertEquals( 0, rowGTE0.getLong(1));

        final VoltTableRow rowGTE1 = results[tableI].fetchRow(rowI++);
        assertEquals( 0, rowGTE1.getLong(0));
        assertEquals( 1, rowGTE1.getLong(1));

        final VoltTableRow rowGTE2 = results[tableI].fetchRow(rowI++);
        assertEquals( 1, rowGTE2.getLong(0));
        assertEquals( 1, rowGTE2.getLong(1));

        // Test 8 -- GT first component of compound key
        tableI++;
        rowI = 0;
        assertEquals( 1, results[tableI].getRowCount());
        final VoltTableRow rowGT0 = results[tableI].fetchRow(rowI++);
        assertEquals( 1, rowGT0.getLong(0));
        assertEquals( 1, rowGT0.getLong(1));

        // Test 9 -- LTE first component of compound key
        tableI++;
        rowI = 0;
        assertEquals( 3, results[tableI].getRowCount());
        // after adding reserve scan, JNI and HSQL will report
        // tuples in different order
        // so, add them to a set and ignore the order instead
        final VoltTableRow rowLTE0 = results[tableI].fetchRow(rowI++);
        final VoltTableRow rowLTE1 = results[tableI].fetchRow(rowI++);
        final VoltTableRow rowLTE2 = results[tableI].fetchRow(rowI++);
        HashSet<Long> TID = new HashSet<Long>();

        HashSet<Long> BID = new HashSet<Long>();
        HashSet<Long> expectedTID = new HashSet<Long>();
        HashSet<Long> expectedBID = new HashSet<Long>();

        expectedTID.add(-1L);
        expectedTID.add(0L);
        expectedTID.add(0L);

        expectedBID.add(0L);
        expectedBID.add(0L);
        expectedBID.add(1L);

        TID.add(rowLTE0.getLong(0));
        TID.add(rowLTE1.getLong(0));
        TID.add(rowLTE2.getLong(0));
        BID.add(rowLTE0.getLong(1));
        BID.add(rowLTE1.getLong(1));
        BID.add(rowLTE2.getLong(1));

        assertTrue(TID.equals(expectedTID));
        assertTrue(BID.equals(expectedBID));

        // Test 10 -- LT first component of compound key
        tableI++;
        rowI = 0;
        assertEquals( 1, results[tableI].getRowCount());
        final VoltTableRow rowLT0 = results[tableI].fetchRow(rowI++);
        assertEquals( -1, rowLT0.getLong(0));
        assertEquals( 0, rowLT0.getLong(1));
}

    void callHelper(Client client, String procname, Object ...objects )
    throws InterruptedException, IOException
    {
        NullCallback nullCallback = new NullCallback();
        boolean done;
        do {
            done = client.callProcedure(nullCallback, procname, objects);
            if (!done) {
                client.backpressureBarrier();
            }
        } while(!done);
    }

    // Testing ENG-506 but this probably isn't enough to trust...
    public void testUpdateRange() throws IOException, ProcCallException, InterruptedException {
        final Client client = getClient();
        VoltTable[] results;

        callHelper(client, "InsertR1IX", 960, "ztgiZQdUtVJeaPLjN", 1643, 4.95657525992782899138e-01);
        callHelper(client, "InsertR1IX", 961, "ztgiZQdUtVJeaPLjN", 1643, 4.95657525992782899138e-01);
        callHelper(client, "InsertR1IX", 964, "ztgiZQdUtVJeaPLjN", 1643, 8.68352518423806229997e-01);
        callHelper(client, "InsertR1IX", 965, "ztgiZQdUtVJeaPLjN", 1643, 8.68352518423806229997e-01);
        callHelper(client, "InsertR1IX", 968, "ztgiZQdUtVJeaPLjN", -22250, 6.20549983245015868150e-01);
        callHelper(client, "InsertR1IX", 969, "ztgiZQdUtVJeaPLjN", -22250, 6.20549983245015868150e-01);
        callHelper(client, "InsertR1IX", 972, "ztgiZQdUtVJeaPLjN", -22250, 2.69767394221735901105e-01);
        callHelper(client, "InsertR1IX", 973, "ztgiZQdUtVJeaPLjN", -22250, 2.69767394221735901105e-01);
        callHelper(client, "InsertR1IX", 976, "XtQOuGWNzVKtrpnMj", 30861, 1.83913810933858279384e-01);
        callHelper(client, "InsertR1IX", 977, "XtQOuGWNzVKtrpnMj", 30861, 1.83913810933858279384e-01);
        callHelper(client, "InsertR1IX", 980, "XtQOuGWNzVKtrpnMj", 30861, 9.95833142789745329182e-01);
        callHelper(client, "InsertR1IX", 981, "XtQOuGWNzVKtrpnMj", 30861, 9.95833142789745329182e-01);
        callHelper(client, "InsertR1IX", 984, "XtQOuGWNzVKtrpnMj", 32677, 6.78465381526806687873e-01);
        callHelper(client, "InsertR1IX", 985, "XtQOuGWNzVKtrpnMj", 32677, 6.78465381526806687873e-01);
        callHelper(client, "InsertR1IX", 988, "XtQOuGWNzVKtrpnMj", 32677, 3.98623510723492113783e-01);
        callHelper(client, "InsertR1IX", 989, "XtQOuGWNzVKtrpnMj", 32677, 3.98623510723492113783e-01);

        // add NaN for fun
        if (!isHSQL()) {
            callHelper(client, "InsertR1IX", 974, "XtQOuGWNzVKtrpnMj", 32677, 0.0 / 0.0);
        }

        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 44 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<45)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 44 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<43)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 66 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<86)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 66 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<96)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 65 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<1)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 65 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<73)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<86)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<40)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 53 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>76)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 53 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>44)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>29)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>100)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 10 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>87)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 10 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>74)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 79 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>32)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 79 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>8)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 76 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID = 44)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 76 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID = 99)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 26 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID = 15)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 26 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID = 89)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 39 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID = 92)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 39 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID = 8)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID = 83)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID = 72)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 53 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<= 75)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 53 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<= 30)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 54 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<= 12)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 54 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<= 21)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 82 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<= 15)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 82 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<= 49)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 22 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<= 58)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 22 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<= 36)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 48 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>= 90)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 48 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>= 48)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 38 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>= 47)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 38 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID>= 98)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 75 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>= 33)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 75 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>= 33)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 54 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>= 43)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 54 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID>= 29)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 19 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID != 1)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 19 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID != 33)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 4 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID != 52)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 4 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID != 54)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 56 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID != 37)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 56 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID != 94)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 7 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID != 81)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 7 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID != 65)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 72 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<>67)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 72 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<>45)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 94 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<>5)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 94 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.ID<>63)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 57 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<>18)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 57 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<>18)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 78 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<>24)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 78 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.ID<>44)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 23 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<100)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 23 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<64)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 21 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<3)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 21 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<11)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 17 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<2)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 17 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<16)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 99 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<18)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 99 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<73)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 96 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>67)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 96 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>86)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 21 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>84)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 21 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>19)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 0 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>75)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 0 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>34)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 100 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>82)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 100 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>2)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 86 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM = 44)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 86 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM = 16)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 35 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM = 100)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 35 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM = 12)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 99 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM = 3)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 99 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM = 94)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 49 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM = 68)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 49 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM = 43)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 49 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<= 58)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 49 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<= 63)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 59 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<= 31)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 59 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<= 85)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 37 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<= 80)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 37 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<= 57)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<= 64)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<= 88)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 86 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>= 29)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 86 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>= 98)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 48 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>= 5)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 48 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM>= 46)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 14 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>= 83)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 14 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>= 60)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 91 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>= 71)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 91 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM>= 62)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 63 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM != 82)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 63 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM != 86)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM != 57)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM != 46)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM != 88)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM != 70)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 69 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM != 50)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 69 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM != 95)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 28 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<>71)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 28 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<>28)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 87 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<>4)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 87 WHERE (R1IX.ID<R1IX.NUM) AND (R1IX.NUM<>57)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 92 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<>21)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 92 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<>74)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 98 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<>31)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 98 WHERE (R1IX.ID<R1IX.NUM) OR (R1IX.NUM<>60)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 3 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<78)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 3 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<41)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 94 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<41)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 94 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<30)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 73 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<26)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 73 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<7)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 78 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<72)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 78 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<28)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 89 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>19)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 89 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>40)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 45 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>100)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 45 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>92)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 18 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>2)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 18 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>71)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 97 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>86)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 97 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>22)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 62 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID = 46)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 62 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID = 82)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 16 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID = 67)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 16 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID = 92)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 79 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID = 90)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 79 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID = 61)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 36 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID = 57)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 36 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID = 31)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 35 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<= 70)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 35 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<= 71)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 10 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<= 6)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 10 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<= 68)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<= 66)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<= 46)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 61 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<= 22)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 61 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<= 66)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 32 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>= 62)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 32 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>= 86)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>= 89)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 11 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID>= 88)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 51 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>= 28)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 51 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>= 4)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 76 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>= 13)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 76 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID>= 29)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 3 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID != 93)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 3 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID != 98)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 77 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID != 41)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 77 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID != 30)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 70 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID != 62)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 70 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID != 79)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 25 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID != 31)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 25 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID != 40)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 33 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<>4)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 33 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<>57)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 46 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<>21)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 46 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.ID<>19)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 72 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<>4)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 72 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<>45)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 99 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<>45)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 99 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.ID<>43)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 30 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM<55)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 30 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM<5)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 25 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM<46)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 25 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM<48)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.NUM<91)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 9 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.NUM<87)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 29 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.NUM<39)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 29 WHERE (R1IX.ID>R1IX.NUM) OR (R1IX.NUM<61)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 89 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM>37)");
        callHelper(client, "@AdHoc", "UPDATE R1IX SET NUM = 89 WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM>48)");

        client.drain();

        results = client.callProcedure("@AdHoc", "select * from R1IX").getResults();
        System.out.printf("Table has %d rows.\n", results[0].getRowCount());
        System.out.println(results[0]);

        results = client.callProcedure("Eng506UpdateRange", 51, 17).getResults();
        assertNotNull(results);
        assertEquals(1, results.length);
        VoltTable result = results[0];
        long modified = result.fetchRow(0).getLong(0);
        System.out.printf("Update statement modified %d rows.\n", modified);

        if (isHSQL()) {
            assertEquals(16, modified);
        }
        else {
            // extra NaN row got added if not HSQL
            // for now, this query includes the NaN value, but it shouldn't forever
            assertEquals(17, modified);
        }

        // check we can clear out with a NaN involved
        results = client.callProcedure("@AdHoc", "delete from R1IX").getResults();
    }

    public void testKeyCastingOverflow() throws NoConnectionsException, IOException, ProcCallException {
        Client client = getClient();

        ClientResponseImpl cr =
                (ClientResponseImpl) client.callProcedure("@AdHoc",
                                                          "select * from P1 where ID = ?;", 0);
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        try {
            cr = (ClientResponseImpl) client.callProcedure("@AdHoc",
                                                           "select * from P1 where ID = ?;", 6000000000L);
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("tryToMakeCompatible: The provided value: (6000000000) of type:"
                    + " java.lang.Long is not a match or is out of range for the target parameter type: int"));
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestIndexesSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestIndexesSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("indexes-ddl.sql"));
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("Eng397LimitIndexR1", "select * from R1 where R1.ID > 2 Limit ?");
        project.addStmtProcedure("Eng397LimitIndexP1", "select * from P1 where P1.ID > 2 Limit ?");
        project.addStmtProcedure("Eng397LimitIndexR2", "select * from R2 where R2.ID > 2 Limit ?");
        project.addStmtProcedure("Eng397LimitIndexP2", "select * from P2 where P2.ID > 2 Limit ?");
        project.addStmtProcedure("Eng2914BigKeyP1", "select * from P1 where ID < 600000000000");
        project.addStmtProcedure("Eng506UpdateRange",
                                 "UPDATE R1IX SET NUM = ? WHERE (R1IX.ID>R1IX.NUM) AND (R1IX.NUM>?)");
        project.addStmtProcedure("InsertR1IX", "insert into R1IX values (?, ?, ?, ?);");

        project.addStmtProcedure("InlinedInListP3with5DESCs",
                                 "select * from P3 T where T.DESC IN (?, ?, ?, ?, ?)" +
                                 " and T.NUM IN (100, 200, 300, 400, 500)");

        project.addStmtProcedure("InlinedInListR3with5DESCs",
                                 "select * from R3 T where T.DESC IN (?, ?, ?, ?, ?)" +
                                 " and T.NUM IN (100, 200, 300, 400, 500)");

        project.addStmtProcedure("InlinedInListP3withDESCs",
                                 "select * from P3 T where T.DESC IN ?" +
                                 " and T.NUM IN (100, 200, 300, 400, 500)");


        project.addStmtProcedure("InlinedInListP3with5NUMs",
                                 "select * from P3 T where T.DESC IN ('a', 'b', 'c', 'g', " +
                                 "'this here is a longish string to force a permanent object allocation'" +
                                 ")" +
                                 " and T.NUM IN (?, ?, ?, ?, ?)");

        project.addStmtProcedure("InlinedInListR3with5NUMs",
                                 "select * from R3 T where T.DESC IN ('a', 'b', 'c', 'g', " +
                                 "'this here is a longish string to force a permanent object allocation'" +
                                 ")" +
                                 " and T.NUM IN (?, ?, ?, ?, ?)");

        project.addStmtProcedure("InlinedInListP3withNUMs",
                                 "select * from P3 T where T.DESC IN ('a', 'b', 'c', 'g', " +
                                 "'this here is a longish string to force a permanent object allocation'" +
                                 ")" +
                                 " and T.NUM IN ?");

        //project.addStmtProcedure("InlinedUpdateInListP3with5NUMs",
        //        "update P3 set NUM = 0 where DESC IN ('a', 'b', 'c', 'g', " +
        //        "'this here is a longish string to force a permanent object allocation'" +
        //        ")" +
        //        " and NUM IN (111,222,333,444,555)");

        boolean success;

        //* CONFIG #1: HSQL -- keep this enabled by default with //
        config = new LocalCluster("testindexes-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // end of easy-to-disable code section */

        //* CONFIG #2: JNI -- keep this enabled by default with //
        config = new LocalCluster("testindexes-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // end of easy-to-disable code section */

        /*/ CONFIG #3: IPC -- keep this normally disabled with / * vs. //
        config = new LocalCluster("testindexes-threesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // end of normally disabled section */

        // no clustering tests for indexes

        return builder;
    }

}
