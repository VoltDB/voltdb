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
import java.util.ArrayList;
import java.util.Collections;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.orderbyprocs.InsertO1;
import org.voltdb_testprocs.regressionsuites.orderbyprocs.InsertO3;
import org.voltdb_testprocs.regressionsuites.orderbyprocs.OrderByCountStarAlias;
import org.voltdb_testprocs.regressionsuites.orderbyprocs.OrderByNonIndex;
import org.voltdb_testprocs.regressionsuites.orderbyprocs.OrderByOneIndex;

public class TestOrderBySuite extends RegressionSuite {

    /*
     * CREATE TABLE O1 ( PKEY INTEGER, A_INT INTEGER, A_INLINE_STR VARCHAR(10),
     * A_POOL_STR VARCHAR(1024), PRIMARY_KEY (PKEY) );
     */

    static final Class<?>[] PROCEDURES = {InsertO1.class,
                                          InsertO3.class,
                                          OrderByCountStarAlias.class,
                                          OrderByNonIndex.class,
                                          OrderByOneIndex.class };

    ArrayList<Integer> a_int = new ArrayList<Integer>();
    ArrayList<String> a_inline_str = new ArrayList<String>();
    ArrayList<String> a_pool_str = new ArrayList<String>();

    public final static String bigString = "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ" +
                                    "ABCDEFGHIJ";

    /** add 20 shuffled rows
     * @throws InterruptedException */
    private void load(Client client)
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException
    {
        client.callProcedure("Truncate01");
        int pkey = 0;
        a_int.clear();
        a_inline_str.clear();
        a_pool_str.clear();

        // if you want to test synchronous latency, this
        //  is a good variable to change
        boolean async = true;

        for (int i=0; i < 20; i++) {
            a_int.add(i);
            a_inline_str.add("a_" + i);
            a_pool_str.add(bigString + i);
        }

        Collections.shuffle(a_int);
        Collections.shuffle(a_inline_str);
        Collections.shuffle(a_pool_str);

        for (int i=0; i < 20; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb,
                    "InsertO1",
                    pkey++,
                    a_int.get(i),
                    a_inline_str.get(i),
                    a_pool_str.get(i));

            if (!async) {
                cb.waitForResponse();
                VoltTable vt = cb.getResponse().getResults()[0];
                assertTrue(vt.getRowCount() == 1);
            }
        }

        client.drain();
    }

    private void loadO3(Client client)
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException
    {
        client.callProcedure("Truncate03");
        int pkey = 0;
        a_int.clear();

        // if you want to test synchronous latency, this
        //  is a good variable to change
        boolean async = true;

        for (int i=0; i < 20; i++) {
            a_int.add(i);
            a_inline_str.add("a_" + i);
            a_pool_str.add(bigString + i);
        }

        Collections.shuffle(a_int);

        for (int i=0; i < 20; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb,
                    "InsertO3",
                    pkey,
                    a_int.get(i),
                    a_int.get(i),
                    a_int.get(i)
                    );

            if (!async) {
                cb.waitForResponse();
                VoltTable vt = cb.getResponse().getResults()[0];
                assertTrue(vt.getRowCount() == 1);
            }
        }

        client.drain();
    }

    private void loadInOrder(Client client)
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException
    {
        client.callProcedure("Truncate03");
        // if you want to test synchronous latency, this
        //  is a good variable to change
        boolean async = true;

        for (int i = 0; i < 100; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb, "InsertO3", 3, i, i, i);

            if (!async) {
                cb.waitForResponse();
                VoltTable vt = cb.getResponse().getResults()[0];
                assertTrue(vt.getRowCount() == 1);
            }
        }
    }

    private void loadWithDupes(Client client) throws Exception
    {
        client.callProcedure("Truncate01");

        client.callProcedure("InsertO1", 1, new Long(1), "Alice", "AlphaBitters");
        client.callProcedure("InsertO1", 2, new Long(2), "Alice", "CrunchTubers");
        client.callProcedure("InsertO1", 3, new Long(3), "Alice", "BetaBuildingBlocks");

        client.callProcedure("InsertO1", 4, new Long(1), "Betty", "CrunchTubers");
        client.callProcedure("InsertO1", 5, new Long(2), "Betty", "AlphaBitters");
        client.callProcedure("InsertO1", 6, new Long(3), "Betty", "BetaBuildingBlocks");

        client.callProcedure("InsertO1", 7, new Long(1), "Chris", "BetaBuildingBlocks");
        client.callProcedure("InsertO1", 8, new Long(2), "Chris", "CrunchTubers");
        client.callProcedure("InsertO1", 9, new Long(3), "Chris", "AlphaBitters");

        client.drain();
    }

    private void loadWithDifferingDupes(Client client)
    throws NoConnectionsException, IOException, ProcCallException, InterruptedException
    {
        client.callProcedure("Truncate01");

        client.callProcedure("InsertO1", 1, new Long(1), "Alice", "AlphaBitters");
        client.callProcedure("InsertO1", 2, new Long(2), "Alice", "CrunchTubers");
        client.callProcedure("InsertO1", 3, new Long(3), "Alice", "BetaBuildingBlocks");
        client.callProcedure("InsertO1", 4, new Long(4), "Betty", "CrunchTubers");
        client.callProcedure("InsertO1", 5, new Long(1), "Betty", "AlphaBitters");
        client.callProcedure("InsertO1", 6, new Long(2), "Betty", "BetaBuildingBlocks");
        client.callProcedure("InsertO1", 7, new Long(3), "Chris", "BetaBuildingBlocks");
        client.callProcedure("InsertO1", 8, new Long(1), "Chris", "CrunchTubers");
        client.callProcedure("InsertO1", 9, new Long(2), "Chris", "AlphaBitters");
        client.callProcedure("InsertO1", 10, new Long(1), "TheDude", "Caucasian");
        client.drain();
    }

    /** select * from T order by A ASC
     * @throws IOException
     * @throws ProcCallException
     * @throws NoConnectionsException
     * @throws InterruptedException */
    private void subtestOrderBySingleColumnAscending()
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException
    {
        VoltTable vt;
        Client client = this.getClient();
        load(client);

        // sort column of ints ascending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_INT ASC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        int it = 0;
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            int pos = a_int.indexOf(a);   // offset of this value in unsorted data

            assertEquals(it, a.intValue());     // a should be order 1, 2, 3..
            assertEquals(pos, key.intValue());  // side-effect of insertion method
            assertEquals(b, a_inline_str.get(pos));
            assertEquals(c, a_pool_str.get(pos));

            it++;
        }

        // sort column of inlined strings ascending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_INLINE_STR ASC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        String lastString = "a";
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            assertTrue(lastString.compareTo(b) < 0);  // always ascending
            lastString = b;

            int pos = a_inline_str.indexOf(b);   // offset of this value in unsorted data
            assertEquals(pos, key.intValue());   // side-effect of insertion method
            assertEquals(a, a_int.get(pos));     // retrieved value matches at index in unsorted data
            assertEquals(c, a_pool_str.get(pos));
        }


        // sort column of non-inlined strings ascending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_POOL_STR ASC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        lastString = "A";
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            assertTrue(lastString.compareTo(c) < 0);  // always ascending
            lastString = c;

            int pos = a_pool_str.indexOf(c);   // offset of this value in unsorted data
            assertEquals(pos, key.intValue());   // side-effect of insertion method
            assertEquals(a, a_int.get(pos));     // retrieved value matches at index in unsorted data
            assertEquals(b, a_inline_str.get(pos));
        }

        loadO3(client);
        Integer lastPk2 = -1;
        // sort indexed column ascending with equality filter on prefix indexed key
        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 = 0 ORDER BY PK2").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        while(vt.advanceRow()) {
            assertEquals(0, vt.getLong(0));
            Integer pk2 = (Integer) vt.get(1, VoltType.INTEGER);
            assertTrue(lastPk2.compareTo(pk2) < 0);
            lastPk2 = pk2;
        }
        lastPk2 = -1;
        // sort indexed column ascending with upper bound filter on prefix indexed key
        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 < 1 ORDER BY PK2").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        while(vt.advanceRow()) {
            assertEquals(0, vt.getLong(0));
            Integer pk2 = (Integer) vt.get(1, VoltType.INTEGER);
            assertTrue(lastPk2.compareTo(pk2) < 0);
            lastPk2 = pk2;
        }

    }

    private void subtestOrderBySingleColumnDescending()
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException
    {
        VoltTable vt;
        int it;
        Client client = this.getClient();
        load(client);

        // sort column of ints descending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_INT DESC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        it = 19;
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            int pos = a_int.indexOf(a);   // offset of this value in unsorted data

            assertEquals(it, a.intValue());     // a should be order 1, 2, 3..
            assertEquals(pos, key.intValue());  // side-effect of insertion method
            assertEquals(b, a_inline_str.get(pos));
            assertEquals(c, a_pool_str.get(pos));

            it--;
        }

        // try that again unperturbed by a silly extra duplicate order by column
        // -- something very similar used to fail as ENG-631
        vt = client.callProcedure("@AdHoc",      // order by A_INT DESC, A_INT is just silly
                                  "select * from O1 order by A_INT DESC, A_INT").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        it = 19;
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            int pos = a_int.indexOf(a);   // offset of this value in unsorted data

            assertEquals(it, a.intValue());     // a should be order 1, 2, 3..
            assertEquals(pos, key.intValue());  // side-effect of insertion method
            assertEquals(b, a_inline_str.get(pos));
            assertEquals(c, a_pool_str.get(pos));

            it--;
        }

        // sort column of inlined strings descending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_INLINE_STR DESC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        String lastString = "z";
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            assertTrue(lastString.compareTo(b) > 0);  // always descending
            lastString = b;

            int pos = a_inline_str.indexOf(b);   // offset of this value in unsorted data
            assertEquals(pos, key.intValue());   // side-effect of insertion method
            assertEquals(a, a_int.get(pos));     // retrieved value matches at index in unsorted data
            assertEquals(c, a_pool_str.get(pos));
        }

        // sort column of non-inlined strings descending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_POOL_STR DESC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        lastString = bigString + "99";
        while (vt.advanceRow()) {
            Integer key = (Integer) vt.get(0, VoltType.INTEGER);
            Integer a = (Integer) vt.get(1, VoltType.INTEGER);
            String b = (String) vt.get(2, VoltType.STRING);
            String c = (String) vt.get(3, VoltType.STRING);

            assertTrue(lastString.compareTo(c) > 0);  // always descending
            lastString = c;

            int pos = a_pool_str.indexOf(c);     // offset of this value in unsorted data
            assertEquals(pos, key.intValue());   // side-effect of insertion method
            assertEquals(a, a_int.get(pos));     // retrieved value matches at index in unsorted data
            assertEquals(b, a_inline_str.get(pos));
        }

        loadO3(client);
        Integer lastPk2 = 20;
        // sort indexed column descending with equality filter on prefix indexed key
        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 = 0 ORDER BY PK2 DESC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        System.out.println(vt.toString());
        while(vt.advanceRow()) {
            assertEquals(0, vt.getLong(0));
            Integer pk2 = (Integer) vt.get(1, VoltType.INTEGER);
            assertTrue(lastPk2.compareTo(pk2) > 0);
            lastPk2 = pk2;
        }
        lastPk2 = 20;
        // desc sort indexed column descending with upper bound filter on prefix indexed key
        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 < 1 ORDER BY PK2 DESC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        System.out.println(vt.toString());
        while(vt.advanceRow()) {
            assertEquals(0, vt.getLong(0));
            Integer pk2 = (Integer) vt.get(1, VoltType.INTEGER);
            assertTrue(lastPk2.compareTo(pk2) > 0);
            lastPk2 = pk2;
        }
    }


            /* create this fascinating survey result table:
         *   Key    Rank    User       Cereal
         *   1      1       Alice      AlphaBitters
         *   2      2       Alice      CrunchTubers
         *   3      3       Alice      BetaBuildingBlocks

         *   4      1       Betty      CrunchTubers
         *   5      2       Betty      AlphaBitters
         *   6      3       Betty      BetaBuildingBlocks

         *   7      1       Chris      BetaBuildingBlocks
         *   8      2       Chris      CrunchTubers
         *   9      3       Chris      AlphaBitters
         */

    private void subtestMultiColumnOrderBy() throws Exception {
        VoltTable vt;
        Client client = this.getClient();
        loadWithDupes(client);

        // order by reverse rank and ascending name ..
        vt =  client.callProcedure("@AdHoc", "select * from O1 order by A_INT DESC, A_INLINE_STR ASC" ).getResults()[0];
        assertTrue(vt.getRowCount() == 9);

        vt.advanceRow();
        Integer a = (Integer) vt.get(1, VoltType.INTEGER);
        String b = (String) vt.get(2, VoltType.STRING);
        String c = (String) vt.get(3, VoltType.STRING);
        assertEquals(3, a.intValue());
        assertEquals(b, "Alice");
        assertEquals(c, "BetaBuildingBlocks");

        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(3, a.intValue());
        assertEquals(b, "Betty");
        assertEquals(c, "BetaBuildingBlocks");

        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(3, a.intValue());
        assertEquals(b, "Chris");
        assertEquals(c, "AlphaBitters");

        // 2nd rank
        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(2, a.intValue());
        assertEquals(b, "Alice");
        assertEquals(c, "CrunchTubers");

        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(2, a.intValue());
        assertEquals(b, "Betty");
        assertEquals(c, "AlphaBitters");

        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(2, a.intValue());
        assertEquals(b, "Chris");
        assertEquals(c, "CrunchTubers");

        // 1st rank
        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(1, a.intValue());
        assertEquals(b, "Alice");
        assertEquals(c, "AlphaBitters");

        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(1, a.intValue());
        assertEquals(b, "Betty");
        assertEquals(c, "CrunchTubers");

        vt.advanceRow();
        a = (Integer) vt.get(1, VoltType.INTEGER); b = (String) vt.get(2, VoltType.STRING); c = (String) vt.get(3, VoltType.STRING);
        assertEquals(1, a.intValue());
        assertEquals(b, "Chris");
        assertEquals(c, "BetaBuildingBlocks");
    }

    private void subtestOrderByUseIndex()
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException
    {
        @SuppressWarnings("unused")
        long start, elapsed;
        //long base;
        VoltTable vt;
        Client client = this.getClient();
        if (this.isHSQL())
            return;

        loadInOrder(client);

        // the duration of doing sequential scan followed by a quicksort
        // start = System.currentTimeMillis();
        // vt = client.callProcedure("OrderByNonIndex")[0];
        // base = System.currentTimeMillis() - start;

        // sort one index column of ints descending.
        // When testSillyCase goes non-zero, test for non-effect of ENG-631
        // -- possible confusion caused by "order by I3 desc, I3".
        for (int testSillyCase = 0; testSillyCase < 2; ++testSillyCase) {
            start = System.currentTimeMillis();
            vt = client.callProcedure("OrderByOneIndex", testSillyCase).getResults()[0];
            elapsed = System.currentTimeMillis() - start;
            // at least 3 times faster
            // TODO (nshi): This should really belong to performance tests.
            // assertTrue(elapsed <= base / 3);
            assertTrue(vt.getRowCount() == 3);
            long it = Integer.MAX_VALUE;
            while (vt.advanceRow()) {
                int b = (Integer) vt.get(1, VoltType.INTEGER);
                int c = (Integer) vt.get(2, VoltType.INTEGER);
                int d = (Integer) vt.get(3, VoltType.INTEGER);

                assertTrue(b == c && c == d && b <= it);
                it = b;
            }
        }
    }

    private void subtestAggOrderByGroupBy() throws Exception
    {
        VoltTable vt;
        Client client = this.getClient();
        loadWithDupes(client);
        vt =  client.callProcedure("@AdHoc", "select sum(A_INT), A_INLINE_STR, sum(PKEY) from O1 group by A_INLINE_STR order by A_INLINE_STR" ).getResults()[0];
        System.out.println(vt.toString());
        vt.advanceRow();
        assertEquals(6, vt.get(0, VoltType.INTEGER));
        assertEquals("Alice", vt.get(1, VoltType.STRING));
        assertEquals(6, vt.get(2, VoltType.INTEGER));
        vt.advanceRow();
        assertEquals(6, vt.get(0, VoltType.INTEGER));
        assertEquals("Betty", vt.get(1, VoltType.STRING));
        assertEquals(15, vt.get(2, VoltType.INTEGER));
        vt.advanceRow();
        assertEquals(6, vt.get(0, VoltType.INTEGER));
        assertEquals("Chris", vt.get(1, VoltType.STRING));
        assertEquals(24, vt.get(2, VoltType.INTEGER));
    }

    private void subtestOrderByCountStarAlias()
    throws IOException, ProcCallException, InterruptedException
    {
        VoltTable vt;
        Client client = getClient();
        loadWithDifferingDupes(client);
        vt = client.callProcedure("OrderByCountStarAlias").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(4, vt.getRowCount());
        vt.advanceRow();
        assertEquals(4, vt.get("A_INT", VoltType.INTEGER));
        assertEquals(1L, vt.get("FOO", VoltType.BIGINT));
        vt.advanceRow();
        assertEquals(3, vt.get("A_INT", VoltType.INTEGER));
        assertEquals(2L, vt.get("FOO", VoltType.BIGINT));
        vt.advanceRow();
        assertEquals(2, vt.get("A_INT", VoltType.INTEGER));
        assertEquals(3L, vt.get("FOO", VoltType.BIGINT));
        vt.advanceRow();
        assertEquals(1, vt.get("A_INT", VoltType.INTEGER));
        assertEquals(4L, vt.get("FOO", VoltType.BIGINT));
    }

    private void subtestOrderByCountStarCardinal()
    throws IOException, ProcCallException, InterruptedException
    {
        VoltTable vt;
        Client client = getClient();
        loadWithDifferingDupes(client);
        vt = client.callProcedure("@AdHoc", "select A_INT, count(*) from O1 group by A_INT order by 2;").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(4, vt.getRowCount());
        vt.advanceRow();
        assertEquals(4, vt.get(0, VoltType.INTEGER));
        assertEquals(1L, vt.get(1, VoltType.BIGINT));
        vt.advanceRow();
        assertEquals(3, vt.get(0, VoltType.INTEGER));
        assertEquals(2L, vt.get(1, VoltType.BIGINT));
        vt.advanceRow();
        assertEquals(2, vt.get(0, VoltType.INTEGER));
        assertEquals(3L, vt.get(1, VoltType.BIGINT));
        vt.advanceRow();
        assertEquals(1, vt.get(0, VoltType.INTEGER));
        assertEquals(4L, vt.get(1, VoltType.BIGINT));
    }

    private void subtestOrderByCountStarWithLimit()
    throws IOException, ProcCallException, InterruptedException
    {
        VoltTable vt;
        Client client = getClient();
        loadWithDifferingDupes(client);
        vt = client.callProcedure("@AdHoc", "select A_INT, count(*) as FOO from O1 group by A_INT order by FOO limit 1;").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals(4, vt.get("A_INT", VoltType.INTEGER));
        assertEquals(1L, vt.get("FOO", VoltType.BIGINT));
    }

    private void subtestOrderByWithNewExpression() throws Exception
    {
        VoltTable vt;
        Client client = getClient();
        loadWithDupes(client);
        vt = client.callProcedure("@AdHoc", "select PKEY + A_INT from O1 order by PKEY + A_INT;").getResults()[0];
        System.out.println(vt.toString());
        ArrayList<Long> expected = new ArrayList<Long>();
        for (int i = 1; i < 10; i++)
        {
            expected.add((long) (i + ((i-1) % 3) + 1));
        }
        Collections.sort(expected);
        assertEquals(9, expected.size());
        for (int i = 0; i < expected.size(); i++)
        {
            vt.advanceRow();
            assertEquals(expected.get(i), vt.get(0, VoltType.BIGINT));
        }
    }

    private void subtestEng1133() throws Exception
    {
        Client client = getClient();
        client.callProcedure("TruncateA");
        client.callProcedure("TruncateB");

        for(int a=0; a < 5; a++)
        {
            client.callProcedure("A.insert", a);
            client.callProcedure("B.insert", a);
        }
        VoltTable vt = client.callProcedure("@AdHoc", "select a.a, b.a from a, b where b.a >= 3 order by a.a, b.a").
                              getResults()[0];
        System.out.println(vt.toString());
        for (int i = 0; i < 10; i++)
        {
            assertEquals(i/2, vt.fetchRow(i).getLong(0));
        }
    }

    private void subtestEng4676() throws Exception
    {
        Client client = getClient();
        client.callProcedure("Truncate01");
        client.callProcedure("Truncate03");
        /*
         * Column definition for O1 and O3:
         *   O1 (PKEY, A_INT, A_INLINE_STR, A_POOL_STR)
         *   O3 (PK1, PK2, I3, I4)
         */
        for (int i = 0; i < 10; i++)
        {
            client.callProcedure("InsertO1", i, i, "", "");
            client.callProcedure("InsertO3", i + 1, i + 1, i + 1, i + 1);
            client.callProcedure("InsertO3", i + 1, i + 2, i + 2, i + 2);
            client.callProcedure("InsertO3", i + 1, i + 3, i + 3, i + 3);
        }
        VoltTable vt;

        vt = client.callProcedure("@AdHoc", "SELECT O3.PK2 FROM O1, O3 WHERE O3.PK1 = O1.A_INT ORDER BY O1.PKEY LIMIT 1").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.getRowCount());
        assertEquals(1, vt.fetchRow(0).getLong(0));

        vt = client.callProcedure("@AdHoc", "SELECT O3.PK2 FROM O1, O3 WHERE O3.PK1 = O1.A_INT ORDER BY O1.PKEY DESC LIMIT 1").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.getRowCount());
        assertEquals(9, vt.fetchRow(0).getLong(0));

        vt = client.callProcedure("@AdHoc", "SELECT O1.A_INT FROM O1, O3 WHERE O3.PK2 = O1.A_INT AND O3.PK1 = 5 ORDER BY O1.PKEY LIMIT 1").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.getRowCount());
        assertEquals(5, vt.fetchRow(0).getLong(0));

        vt = client.callProcedure("@AdHoc", "SELECT O1.A_INT FROM O1, O3 WHERE O3.PK2 = O1.A_INT AND O3.PK1 = 5 ORDER BY O1.PKEY DESC LIMIT 1").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.getRowCount());
        assertEquals(7, vt.fetchRow(0).getLong(0));
    }

    private void subtestEng5021() throws Exception
    {
        Client client = getClient();
        client.callProcedure("Truncate03");
        client.callProcedure("InsertO3", 1,1,1,1);
        client.callProcedure("InsertO3", 1,2,1,1);
        client.callProcedure("InsertO3", 1,3,1,1);
        client.callProcedure("InsertO3", 2,1,2,2);
        client.callProcedure("InsertO3", 2,2,2,2);
        client.callProcedure("InsertO3", 2,3,2,2);
        client.callProcedure("InsertO3", 3,3,3,3);
        client.callProcedure("InsertO3", 4,4,4,4);
        client.callProcedure("InsertO3", 5,5,5,5);

        VoltTable vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 = 1 ORDER BY PK2 DESC LIMIT 1").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(3, vt.fetchRow(0).getLong(1));

        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 = 1 ORDER BY PK1, PK2 DESC").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(3, vt.fetchRow(0).getLong(1));

        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 = 1 ORDER BY PK1, PK2").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.fetchRow(0).getLong(1));

        vt = client.callProcedure("@AdHoc", "SELECT * FROM O3 WHERE PK1 = 1 ORDER BY PK2").getResults()[0];
        System.out.println(vt.toString());
        assertEquals(1, vt.fetchRow(0).getLong(1));

    }

    public void testAll()
    throws Exception
    {
        subtestOrderBySingleColumnAscending();
        subtestOrderBySingleColumnDescending();
        subtestMultiColumnOrderBy();
        subtestOrderByUseIndex();
        subtestAggOrderByGroupBy();
        subtestOrderByCountStarAlias();
        subtestOrderByCountStarCardinal();
        subtestOrderByCountStarWithLimit();
        subtestOrderByWithNewExpression();
        subtestEng1133();
        subtestEng4676();
        subtestEng5021();
    }

    //
    // Suite builder boilerplate
    //
    public TestOrderBySuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        LocalCluster config = null;
        boolean success;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestOrderBySuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestOrderBySuite.class.getResource("testorderby-ddl.sql"));
        project.addProcedures(PROCEDURES);

        //* Single-server configuration  -- please do not remove or corrupt this structured comment
        config = new LocalCluster("testorderby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // End single-server configuration  -- please do not remove or corrupt this structured comment */

        //* HSQL backend server configuration  -- please do not remove or corrupt this structured comment
        config = new LocalCluster("testorderby-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // End HSQL backend server configuration  -- please do not remove or corrupt this structured comment */

        //* Multi-server configuration  -- please do not remove or corrupt this structured comment
        config = new LocalCluster("testorderby-cluster.jar", 3, 2, 1, BackendTarget.NATIVE_EE_JNI);
        // Disable hasLocalServer -- with hasLocalServer enabled,
        // multi-server pro configs mysteriously hang at startup under eclipse.
        config.setHasLocalServer(false);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // End multi-server configuration  -- please do not remove or corrupt this structured comment */

        return builder;
    }
}
