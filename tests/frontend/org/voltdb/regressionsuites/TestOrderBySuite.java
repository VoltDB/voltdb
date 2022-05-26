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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
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

    static final Class<?>[] MP_PROCEDURES = {InsertO3.class,
                                          OrderByCountStarAlias.class,
                                          OrderByNonIndex.class,
                                          OrderByOneIndex.class };

    ArrayList<Integer> a_int = new ArrayList<>();
    ArrayList<String> a_inline_str = new ArrayList<>();
    ArrayList<String> a_pool_str = new ArrayList<>();

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

        vt =  client.callProcedure("@AdHoc", "select sum(A_INT), A_INLINE_STR, sum(PKEY) from O1 group by A_INLINE_STR order by A_INLINE_STR offset 2" ).getResults()[0];
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals(6, vt.get(0, VoltType.INTEGER));
        assertEquals("Chris", vt.get(1, VoltType.STRING));
        assertEquals(24, vt.get(2, VoltType.INTEGER));

        vt =  client.callProcedure("@AdHoc", "select sum(A_INT), A_INLINE_STR, sum(PKEY) from O1 group by A_INLINE_STR order by A_INLINE_STR limit 1" ).getResults()[0];
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals(6, vt.get(0, VoltType.INTEGER));
        assertEquals("Alice", vt.get(1, VoltType.STRING));
        assertEquals(6, vt.get(2, VoltType.INTEGER));

        vt =  client.callProcedure("@AdHoc", "select sum(A_INT), A_INLINE_STR, sum(PKEY) from O1 group by A_INLINE_STR order by A_INLINE_STR limit 1 offset 1" ).getResults()[0];
        assertEquals(1, vt.getRowCount());
        vt.advanceRow();
        assertEquals(6, vt.get(0, VoltType.INTEGER));
        assertEquals("Betty", vt.get(1, VoltType.STRING));
        assertEquals(15, vt.get(2, VoltType.INTEGER));

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
        ArrayList<Long> expected = new ArrayList<>();
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

    private void subtestPartialIndex() throws Exception
    {
        Client client = getClient();
        client.callProcedure("Truncate03");
        client.callProcedure("InsertO3", 2,3,2,2);
        client.callProcedure("InsertO3", 3,-3,3,3);
        client.callProcedure("InsertO3", 4,4,4,4);
        client.callProcedure("InsertO3", 5,-5,5,5);
        client.callProcedure("InsertO3", 1,1,1,1);
        client.callProcedure("InsertO3", 1,2,1,1);
        client.callProcedure("InsertO3", 1,3,1,1);
        client.callProcedure("InsertO3", 2,1,2,2);
        client.callProcedure("InsertO3", 2,2,2,2);

        VoltTable vt;
        String sql;
        // Partial index O3_PARTIAL_TREE (I4 WHERE PK2 > 0) is used for ordering
        sql = "SELECT * FROM O3 WHERE PK2 > 0 ORDER BY I4 DESC LIMIT 1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());
        assertEquals(4, vt.fetchRow(0).getLong(3));

        vt = client.callProcedure("@Explain", sql).getResults()[0];
        System.out.println(vt.toString());
        assertTrue(vt.toString().contains("O3_PARTIAL_TREE"));

        // Partial index O3_PARTIAL_TREE (I4 WHERE PK2 > 0) is used for ordering
        sql = "SELECT * FROM O3 WHERE PK2 > 0 ORDER BY PK2 DESC LIMIT 1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());
        assertEquals(4, vt.fetchRow(0).getLong(1));

        vt = client.callProcedure("@Explain", sql).getResults()[0];
        System.out.println(vt.toString());
        assertTrue(vt.toString().contains("O3_PARTIAL_TREE"));

        // Index O3_TREE (I3) is used for ordering
        sql = "SELECT * FROM O3 WHERE PK2 > 0 AND I3 > 2 ORDER BY PK2 LIMIT 1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());
        assertEquals(4, vt.fetchRow(0).getLong(1));

        vt = client.callProcedure("@Explain", sql).getResults()[0];
        System.out.println(vt.toString());
        assertTrue(vt.toString().contains("O3_TREE"));
    }

    private void subtestOrderByMP() throws Exception
    {
        Client client = getClient();
        client.callProcedure("Truncate01");
        client.callProcedure("Truncate03");
        client.callProcedure("InsertO1", 5, 5,"dummy","dummy");
        client.callProcedure("InsertO1", 1, 5,"dummy1","dummy");
        client.callProcedure("InsertO1", 4, 2,"dummy","dummy");
        client.callProcedure("InsertO1", 3, 1,"dummy","dummy");
        client.callProcedure("InsertO1", 2, 7,"dummy","dummy");
        client.callProcedure("InsertO1", 6, 7,"dummy1","dummy");
        client.callProcedure("InsertO1", 7, 8,"dummy","dummy");

        client.callProcedure("InsertO3", 1, 1, 7, 7);
        client.callProcedure("InsertO3", 2, 2, 7, 7);
        client.callProcedure("InsertO3", 3, 3, 8, 8);
        client.callProcedure("InsertO3", 4, 4, 1, 1);
        client.callProcedure("InsertO3", 10, 10, 10, 10);

        client.callProcedure("TruncateP");
        client.callProcedure("@AdHoc", "insert into P values (0, 1, 2, 10)");
        client.callProcedure("@AdHoc", "insert into P values (1, 1, 1, 10)");
        client.callProcedure("@AdHoc", "insert into P values (2, 1, 1, 20)");
        client.callProcedure("@AdHoc", "insert into P values (3, 1, 0, 30)");

        String sql;
        VoltTable vt;
        long[][] expected;
        // Partitions Result sets are ordered by index. No LIMIT/OFFEST
        sql = "SELECT PKEY FROM O1 ORDER BY PKEY DESC";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{7}, {6}, {5}, {4}, {3}, {2}, {1}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // Partitions Result sets are ordered by index with LIMIT/OFFSET
        sql = "SELECT PKEY FROM O1 ORDER BY PKEY LIMIT 3 OFFSET 3";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{4}, {5}, {6}};
        validateTableOfLongs(vt, expected);

        sql = "SELECT PKEY FROM O1 ORDER BY PKEY DESC LIMIT 3 OFFSET 3";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{4}, {3}, {2}};
        validateTableOfLongs(vt, expected);

        sql = "SELECT PKEY FROM O1 ORDER BY PKEY DESC LIMIT 3";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{7}, {6}, {5}};
        sql = "SELECT PKEY FROM O1 ORDER BY PKEY DESC OFFSET 5";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{2}, {1}};
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // IDX_O1_A_INT_PKEY index provides the right order for the coordinator. Merge Receive
        sql = "SELECT A_INT, PKEY FROM O1 ORDER BY A_INT DESC, PKEY DESC LIMIT 3";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{8,7}, {7,6}, {7,2}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // NLIJ with index outer table scan
        sql = "SELECT O1.A_INT FROM O1, O3 WHERE O1.A_INT = O3.I3 ORDER BY O1.A_INT";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {7}, {7}, {7}, {7}, {8}};
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // Index P_D32_10_IDX ON P (P_D3 / 10, P_D2) covers ORDER BY expressions (P_D3 / 10, P_D2)
        sql = "select P_D0 from P where P.P_D3 / 10 > 0 order by P_D3 / 10, P_D2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {0}, {2}, {3}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));
        assertTrue(vt.toString().contains("P_D32_10_IDX"));

        // Index P_D32_10_IDX ON P (P_D3 / 10, P_D2) does not cover ORDER BY expressions (P_D3 / 5, P_D2)
        // Trivial coordinator makes MERGE RECEIVE possible
        sql = "select P_D0 from P where P.P_D3 / 10 > 0 order by P_D3 / 5, P_D2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {0}, {2}, {3}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));
        assertTrue(vt.toString().contains("P_D32_10_IDX"));

        // P_D0 is a partition column for P. All rows are from a single partition.
        // Merge Receive
        client.callProcedure("TruncateP");
        client.callProcedure("@AdHoc", "insert into P values (1, 1, 2, 10)");
        client.callProcedure("@AdHoc", "insert into P values (3, 1, 1, 10)");
        client.callProcedure("@AdHoc", "insert into P values (5, 1, 1, 20)");
        client.callProcedure("@AdHoc", "insert into P values (7, 1, 0, 30)");
        sql = "select P_D0 from P where P.P_D3 / 10 > 0 order by P_D3 / 10, P_D2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{3}, {1}, {5}, {7}};
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

    }

    private void subtestOrderByMP_Agg() throws Exception
    {
        Client client = getClient();
        client.callProcedure("TruncateP");
        client.callProcedure("@AdHoc", "insert into P values(1, 11, 1, 1)");
        client.callProcedure("@AdHoc", "insert into P values(1, 1, 2, 1)");
        client.callProcedure("@AdHoc", "insert into P values(3, 6, 2, 1)");
        client.callProcedure("@AdHoc", "insert into P values(4, 6, 1, 1)");
        client.callProcedure("@AdHoc", "insert into P values(5, 11, 2, 1)");
        client.callProcedure("@AdHoc", "insert into P values(7, 1, 4, 1)");
        client.callProcedure("@AdHoc", "insert into P values(7, 6, 1, 1)");

        String sql;
        VoltTable vt;
        long[][] expected;
        // Merge Receive with Serial aggregation
        //            select indexed_non_partition_key, max(col)
        //            from partitioned
        //            group by indexed_non_partition_key
        //            order by indexed_non_partition_key;"
        sql = "select P_D1, max(P_D2) from P where P_D1 > 0 group by P_D1 order by P_D1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1, 4}, {6, 2}, {11, 2}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // Merge Receive with Partial aggregation
        //            select indexed_non_partition_key, col, max(col)
        //            from partitioned
        //            group by indexed_non_partition_key, col
        //            order by indexed_non_partition_key;
        sql = "select P_D1, P_D3, max(P_D2) from P group by P_D1, P_D3 order by P_D1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1, 1, 4}, {6, 1, 2}, {11, 1, 2}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // No aggregation at coordinator
        //          select indexed_partition_key, max(col)
        //          from partitioned
        //          group by indexed_partition_key
        //          order by indexed_partition_key;"
        sql = "select max(P_D2), P_D0 from P group by P_D0  order by P_D0";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{2, 1}, {2, 3}, {1, 4}, {2, 5}, {4, 7}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // No aggregation at coordinator
        //          select indexed_non_partition_key, max(col)
        //          from partitioned
        //          group by indexed_non_partition_key, indexed_partition_key
        //          order by indexed_partition_key;"
        sql = "select max(P_D2), P_D1, P_D0 from P group by P_D1, P_D0 order by P_D0  limit 3 offset 2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{2, 6, 3}, {1, 6, 4}, {2, 11,5}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        // Merge Receive with Serial aggregation
        //            select indexed_non_partition_key1, indexed_non_partition_key2, max(col)
        //            from partitioned
        //            group by indexed_non_partition_key1, indexed_non_partition_key2
        //            order by indexed_non_partition_key1, indexed_non_partition_key2;"
        sql = "select P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D2 order by P_D3, P_D2 limit 1 offset 1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1, 2, 5}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        sql = "select P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D2 order by P_D3, P_D2 offset 2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1, 4, 7}};
        validateTableOfLongs(vt, expected);

        sql = "select P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D2 order by P_D3, P_D2 limit 2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1, 1, 7}, {1, 2, 5}};
        validateTableOfLongs(vt, expected);

        // Merge Receive without aggregation at coordinator
        //            select indexed_non_partition_key1, indexed_non_partition_key2, col, max(col)
        //            from partitioned
        //            group by indexed_non_partition_key1, indexed_non_partition_key2, col
        //            order by indexed_non_partition_key1, indexed_non_partition_key2;"
        sql = "select P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D2, P_D0 order by P_D3, P_D2";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1, 1}, {1, 1}, {1, 1}, {1, 2}, {1, 2}, {1, 2}, {1, 4}};
        assertEquals(expected.length, vt.getRowCount());
        for(int i = 0; i < expected.length; ++i) {
            VoltTableRow row = vt.fetchRow(i);
            assertEquals(expected[i][0], row.getLong("P_D3"));
            assertEquals(expected[i][1], row.getLong("P_D2"));
        }
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        //  Merge Receive from view, ordering by its non-partition-key grouping columns
        sql = "SELECT V_P_D1 FROM V_P order by V_P_D1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {1}, {6}, {6}, {11}, {11}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));

        sql = "SELECT V_P_D1, V_P_D2 FROM V_P order by V_P_D1 DESC , V_P_D2 DESC";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{11, 2}, {11, 1}, {6, 2}, {6, 1}, {1, 4}, {1, 2}};
        validateTableOfLongs(vt, expected);
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().contains("MERGE RECEIVE"));
    }

    private void subtestOrderByMP_Subquery() throws Exception
    {
        Client client = getClient();
        client.callProcedure("TruncateP");
        client.callProcedure("@AdHoc", "insert into P values(11, 11, 1, 1)");
        client.callProcedure("@AdHoc", "insert into P values(1, 1, 2, 1)");
        client.callProcedure("@AdHoc", "insert into P values(3, 6, 2, 1)");
        client.callProcedure("@AdHoc", "insert into P values(8, 4, 1, 1)");
        client.callProcedure("@AdHoc", "insert into P values(5, 11, 2, 1)");
        client.callProcedure("@AdHoc", "insert into P values(4, 1, 4, 1)");
        client.callProcedure("@AdHoc", "insert into P values(7, 6, 1, 1)");

        String sql;
        VoltTable vt;
        long[][] expected;
        // Select from an ordered subquery. The subquery SeqScanPlanNode.isOutputOrdered
        // unconditionally returns FALSE even if the parent sort expressions and order matches
        // its own ones.
        sql = "select PT_D1 from (select P_D1 as PT_D1 from P where P.P_D1 > 0 order by P_D1) P_T order by PT_D1;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {1}, {4}, {6}, {6}, {11}, {11}};
        validateTableOfLongs(vt, expected);

        // Select from an ordered subquery with LIMIT. The subquery MERGERECEIVE node is preserved to guarantee
        // its determinism. Subquery LIMIT is required to disable the subquery optimization
        sql = "select PT_D1 from (select P_D1 as PT_D1 from P where P.P_D1 > 0 order by P_D1 limit 4) P_T limit 4;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {1}, {4}, {6}};
        validateTableOfLongs(vt, expected);

        // The subquery with non-partition GROUP BY column - The subquery MERGERECEIVE node is preserved
        sql = "select PT_D1, MP_D3 from (select P_D1 as PT_D1, max(P_D3) as MP_D3 from P group by P_D1 order by P_D1 limit 4) P_T order by PT_D1";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        expected = new long[][] {{1}, {4}, {6}, {11}};
        validateTableOfLongs(vt, expected);
    }

    /*
     * To disable a test, set these to false.  This is convenient
     * for testing.  For example, to test subtestOrderByMP, set
     * everything but do_subtestOrderByMP to false.  Then only
     * subtestOrderByMP will run.
     */
    private static boolean do_subtestOrderBySingleColumnAscending = true;
    private static boolean do_subtestOrderBySingleColumnDescending = true;
    private static boolean do_subtestMultiColumnOrderBy = true;
    private static boolean do_subtestOrderByUseIndex = true;
    private static boolean do_subtestAggOrderByGroupBy = true;
    private static boolean do_subtestOrderByCountStarAlias = true;
    private static boolean do_subtestOrderByCountStarCardinal = true;
    private static boolean do_subtestOrderByCountStarWithLimit = true;
    private static boolean do_subtestOrderByWithNewExpression = true;
    private static boolean do_subtestEng1133 = true;
    private static boolean do_subtestEng4676 = true;
    private static boolean do_subtestEng5021 = true;
    private static boolean do_subtestPartialIndex = true;
    private static boolean do_subtestOrderByMP = true;
    private static boolean do_subtestOrderByMP_Agg = true;
    private static boolean do_subtestOrderByMP_Subquery = true;

    public void testAll()
    throws Exception
    {
        if (do_subtestOrderBySingleColumnAscending) {
            subtestOrderBySingleColumnAscending();
        }
        if (do_subtestOrderBySingleColumnDescending) {
            subtestOrderBySingleColumnDescending();
        }
        if (do_subtestMultiColumnOrderBy) {
            subtestMultiColumnOrderBy();
        }
        if (do_subtestOrderByUseIndex) {
            subtestOrderByUseIndex();
        }
        if (do_subtestAggOrderByGroupBy) {
            subtestAggOrderByGroupBy();
        }
        if (do_subtestOrderByCountStarAlias) {
            subtestOrderByCountStarAlias();
        }
        if (do_subtestOrderByCountStarCardinal) {
            subtestOrderByCountStarCardinal();
        }
        if (do_subtestOrderByCountStarWithLimit) {
            subtestOrderByCountStarWithLimit();
        }
        if (do_subtestOrderByWithNewExpression) {
            subtestOrderByWithNewExpression();
        }
        if (do_subtestEng1133) {
            subtestEng1133();
        }
        if (do_subtestEng4676) {
            subtestEng4676();
        }
        if (do_subtestEng5021) {
            subtestEng5021();
        }
        if (do_subtestPartialIndex) {
            subtestPartialIndex();
        }
        if (do_subtestOrderByMP) {
            subtestOrderByMP();
        }
        if (do_subtestOrderByMP_Agg) {
            subtestOrderByMP_Agg();
        }
        if (do_subtestOrderByMP_Subquery) {
            subtestOrderByMP_Subquery();
        }
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
        project.addMultiPartitionProcedures(MP_PROCEDURES);
        project.addProcedure(InsertO1.class, "O1.PKEY: 0");

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
