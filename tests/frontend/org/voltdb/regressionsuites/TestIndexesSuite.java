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
import java.util.TreeSet;

import org.voltdb.*;
import org.voltdb.client.*;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.indexes.CheckMultiMultiIntGTEFailure;
import org.voltdb_testprocs.regressionsuites.indexes.Insert;

/**
 * Actual regression tests for SQL that I found that was broken and
 * have fixed.  Didn't like any of the other potential homes that already
 * existed for this for one reason or another.
 */

public class TestIndexesSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class,
        CheckMultiMultiIntGTEFailure.class};

    // Index stuff to test:
    // scans against tree
    // - < <= = > >=, range with > and <
    // - single column
    // - multi-column
    // - multi-map

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
            VoltTable results =
                client.callProcedure(
                        "@AdHoc",
                        "SELECT R1.ID, MIN(R1.ID) from R1 group by R1.ID order by R1.ID limit 4").getResults()[0];
            System.out.println(results);
        }
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
            String query = String.format("select * from %s where %s.ID > 1",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(5, results[0].getRowCount());
            // make sure that we work if the value we want isn't present
            query = String.format("select * from %s where %s.ID > 4",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 8",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 1",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 4",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 9",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 1 and %s.ID < 6",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 1 and %s.ID <= 6",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID > 1 and %s.ID <= 5",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 1 and %s.ID < 7",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            // Check that >= work in conjunction with <
            // run over the end of the index to catch the keyIterate bug
            // in the first >= index fix
            query = String.format("select * from %s where %s.ID >= 1 and %s.ID < 10",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            // XXX THIS CASE CURRENTLY FAILS
            // SEE TICKET 194
//            query = String.format("select * from %s where %s.ID >= 2.9",
//                                  table, table);
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
            String query = String.format("select * from %s where %s.NUM > 100",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 150",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 300",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM >= 100",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM >= 150",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM >= 301",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 100 and %s.NUM < 300",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            // Check that >= work in conjunction with <
            // run over the end of the index to catch the keyIterate bug
            // in the first >= index fix
            query = String.format("select * from %s where %s.NUM >= 100 and %s.NUM < 400",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM = 100",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(2, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 100 and %s.NUM <= 300",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.NUM > 100 and %s.NUM <= 250",
                                  table, table, table);
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
            String query = String.format("select * from %s where %s.NUM >= 100 and %s.NUM <= 400",
                                  table, table, table);
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
            String query = String.format("select * from %s where %s.NUM > 100 AND %s.NUM2 > 1",
                                         table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
        }
    }

    public void testOrderedMultiMultiIntGTEFailure()
    throws IOException, ProcCallException
    {
        final Client client = getClient();
        final VoltTable results[] = client.callProcedure("CheckMultiMultiIntGTEFailure").getResults();
        if (results == null || results.length == 0) {
            fail();
        }
        assertEquals( 2, results[0].getRowCount());
        final VoltTableRow row0 = results[0].fetchRow(0);
        assertEquals( 0, row0.getLong(0));
        assertEquals( 0, row0.getLong(1));

        final VoltTableRow row1 = results[0].fetchRow(1);
        assertEquals( 0, row1.getLong(0));
        assertEquals( 1, row1.getLong(1));
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

        callHelper(client, "InsertP1IX", 960, "ztgiZQdUtVJeaPLjN", 1643, 4.95657525992782899138e-01);
        callHelper(client, "InsertP1IX", 961, "ztgiZQdUtVJeaPLjN", 1643, 4.95657525992782899138e-01);
        callHelper(client, "InsertP1IX", 964, "ztgiZQdUtVJeaPLjN", 1643, 8.68352518423806229997e-01);
        callHelper(client, "InsertP1IX", 965, "ztgiZQdUtVJeaPLjN", 1643, 8.68352518423806229997e-01);
        callHelper(client, "InsertP1IX", 968, "ztgiZQdUtVJeaPLjN", -22250, 6.20549983245015868150e-01);
        callHelper(client, "InsertP1IX", 969, "ztgiZQdUtVJeaPLjN", -22250, 6.20549983245015868150e-01);
        callHelper(client, "InsertP1IX", 972, "ztgiZQdUtVJeaPLjN", -22250, 2.69767394221735901105e-01);
        callHelper(client, "InsertP1IX", 973, "ztgiZQdUtVJeaPLjN", -22250, 2.69767394221735901105e-01);
        callHelper(client, "InsertP1IX", 976, "XtQOuGWNzVKtrpnMj", 30861, 1.83913810933858279384e-01);
        callHelper(client, "InsertP1IX", 977, "XtQOuGWNzVKtrpnMj", 30861, 1.83913810933858279384e-01);
        callHelper(client, "InsertP1IX", 980, "XtQOuGWNzVKtrpnMj", 30861, 9.95833142789745329182e-01);
        callHelper(client, "InsertP1IX", 981, "XtQOuGWNzVKtrpnMj", 30861, 9.95833142789745329182e-01);
        callHelper(client, "InsertP1IX", 984, "XtQOuGWNzVKtrpnMj", 32677, 6.78465381526806687873e-01);
        callHelper(client, "InsertP1IX", 985, "XtQOuGWNzVKtrpnMj", 32677, 6.78465381526806687873e-01);
        callHelper(client, "InsertP1IX", 988, "XtQOuGWNzVKtrpnMj", 32677, 3.98623510723492113783e-01);
        callHelper(client, "InsertP1IX", 989, "XtQOuGWNzVKtrpnMj", 32677, 3.98623510723492113783e-01);
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 44 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<45)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 44 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<43)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 66 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<86)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 66 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<96)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 65 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<1)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 65 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<73)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<86)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<40)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 53 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>76)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 53 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>44)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>29)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>100)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 10 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>87)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 10 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>74)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 79 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>32)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 79 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>8)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 76 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID = 44)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 76 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID = 99)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 26 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID = 15)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 26 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID = 89)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 39 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID = 92)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 39 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID = 8)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID = 83)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID = 72)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 53 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<= 75)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 53 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<= 30)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 54 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<= 12)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 54 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<= 21)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 82 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<= 15)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 82 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<= 49)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 22 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<= 58)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 22 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<= 36)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 48 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>= 90)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 48 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>= 48)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 38 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>= 47)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 38 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID>= 98)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 75 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>= 33)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 75 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>= 33)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 54 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>= 43)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 54 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID>= 29)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 19 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID != 1)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 19 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID != 33)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 4 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID != 52)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 4 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID != 54)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 56 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID != 37)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 56 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID != 94)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 7 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID != 81)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 7 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID != 65)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 72 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<>67)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 72 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<>45)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 94 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<>5)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 94 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.ID<>63)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 57 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<>18)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 57 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<>18)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 78 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<>24)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 78 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.ID<>44)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 23 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<100)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 23 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<64)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 21 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<3)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 21 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<11)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 17 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<2)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 17 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<16)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 99 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<18)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 99 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<73)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 96 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>67)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 96 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>86)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 21 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>84)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 21 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>19)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 0 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>75)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 0 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>34)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 100 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>82)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 100 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>2)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 86 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM = 44)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 86 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM = 16)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 35 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM = 100)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 35 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM = 12)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 99 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM = 3)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 99 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM = 94)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 49 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM = 68)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 49 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM = 43)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 49 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<= 58)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 49 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<= 63)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 59 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<= 31)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 59 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<= 85)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 37 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<= 80)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 37 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<= 57)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<= 64)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<= 88)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 86 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>= 29)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 86 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>= 98)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 48 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>= 5)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 48 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM>= 46)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 14 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>= 83)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 14 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>= 60)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 91 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>= 71)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 91 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM>= 62)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 63 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM != 82)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 63 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM != 86)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM != 57)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM != 46)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM != 88)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM != 70)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 69 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM != 50)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 69 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM != 95)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 28 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<>71)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 28 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<>28)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 87 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<>4)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 87 WHERE (P1IX.ID<P1IX.NUM) AND (P1IX.NUM<>57)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 92 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<>21)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 92 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<>74)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 98 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<>31)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 98 WHERE (P1IX.ID<P1IX.NUM) OR (P1IX.NUM<>60)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 3 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<78)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 3 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<41)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 94 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<41)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 94 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<30)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 73 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<26)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 73 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<7)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 78 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<72)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 78 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<28)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 89 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>19)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 89 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>40)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 45 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>100)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 45 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>92)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 18 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>2)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 18 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>71)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 97 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>86)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 97 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>22)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 62 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID = 46)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 62 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID = 82)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 16 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID = 67)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 16 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID = 92)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 79 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID = 90)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 79 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID = 61)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 36 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID = 57)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 36 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID = 31)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 35 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<= 70)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 35 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<= 71)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 10 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<= 6)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 10 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<= 68)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<= 66)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<= 46)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 61 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<= 22)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 61 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<= 66)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 32 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>= 62)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 32 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>= 86)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>= 89)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 11 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID>= 88)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 51 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>= 28)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 51 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>= 4)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 76 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>= 13)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 76 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID>= 29)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 3 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID != 93)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 3 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID != 98)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 77 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID != 41)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 77 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID != 30)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 70 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID != 62)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 70 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID != 79)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 25 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID != 31)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 25 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID != 40)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 33 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<>4)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 33 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<>57)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 46 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<>21)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 46 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.ID<>19)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 72 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<>4)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 72 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<>45)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 99 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<>45)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 99 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.ID<>43)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 30 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM<55)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 30 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM<5)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 25 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM<46)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 25 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM<48)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.NUM<91)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 9 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.NUM<87)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 29 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.NUM<39)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 29 WHERE (P1IX.ID>P1IX.NUM) OR (P1IX.NUM<61)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 89 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM>37)");
        callHelper(client, "@AdHoc", "UPDATE P1IX SET NUM = 89 WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM>48)");

        client.drain();

        results = client.callProcedure("@AdHoc", "select * from P1IX").getResults();
        System.out.printf("Table has %d rows.\n", results[0].getRowCount());
        System.out.println(results[0]);

        results = client.callProcedure("Eng506UpdateRange", 51, 17).getResults();
        assertNotNull(results);
        assertEquals(1, results.length);
        VoltTable result = results[0];
        long modified = result.fetchRow(0).getLong(0);
        System.out.printf("Update statment modified %d rows.\n", modified);
        assertEquals(16, modified);
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
        project.addPartitionInfo("P1", "ID");
        project.addPartitionInfo("P2", "ID");
        project.addPartitionInfo("P3", "ID");
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("Eng397LimitIndexR1", "select * from R1 where R1.ID > 2 Limit ?");
        project.addStmtProcedure("Eng397LimitIndexP1", "select * from P1 where P1.ID > 2 Limit ?");
        project.addStmtProcedure("Eng397LimitIndexR2", "select * from R2 where R2.ID > 2 Limit ?");
        project.addStmtProcedure("Eng397LimitIndexP2", "select * from P2 where P2.ID > 2 Limit ?");
        project.addStmtProcedure("Eng506UpdateRange", "UPDATE P1IX SET NUM = ? WHERE (P1IX.ID>P1IX.NUM) AND (P1IX.NUM>?)");
        project.addStmtProcedure("InsertP1IX", "insert into P1IX values (?, ?, ?, ?);");

        boolean success;

    /*
        // CONFIG #1: Local Site/Partitions running on IPC backend
        config = new LocalSingleProcessServer("sqltypes-onesite.jar", 1, BackendTarget.NATIVE_EE_IPC);
        config.compile(project);
        builder.addServerConfig(config);*/
        // CONFIG #2: HSQL
        config = new LocalSingleProcessServer("testindexes-hsql.jar", 1, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);


        // JNI
        config = new LocalSingleProcessServer("testindexes-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CLUSTER?
        /*config = new LocalCluster("testindexes-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);*/

        return builder;
    }

}
