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

    static final Class<?>[] PROCEDURES = { InsertO1.class,
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
    private void load(Client client) throws NoConnectionsException, ProcCallException, IOException, InterruptedException {
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

    private void loadInOrder(Client client) throws NoConnectionsException,
                                           ProcCallException,
                                           IOException, InterruptedException {
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

    private void loadWithDupes(Client client) throws Exception {
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
    public void testOrderBySingleColumnAscending() throws NoConnectionsException, ProcCallException, IOException, InterruptedException {
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
    }

    public void testOrderBySingleColumnDescending() throws NoConnectionsException, ProcCallException, IOException, InterruptedException {
        VoltTable vt;
        Client client = this.getClient();
        load(client);

        // sort column of ints descending
        vt = client.callProcedure("@AdHoc", "select * from O1 order by A_INT DESC").getResults()[0];
        assertTrue(vt.getRowCount() == 20);
        int it = 19;
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


        // sort column of non-inlined strings ascending
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

    public void testMultiColumnOrderBy() throws Exception {
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

    public void testOrderByUseIndex() throws NoConnectionsException,
                                     ProcCallException,
                                     IOException, InterruptedException {
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

        // sort one index column of ints ascending
        start = System.currentTimeMillis();
        vt = client.callProcedure("OrderByOneIndex").getResults()[0];
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

    public void testAggOrderByGroupBy() throws Exception
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

    public void testOrderByCountStarAlias()
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

    public void testOrderByCountStarCardinal()
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

    public void testOrderByCountStarWithLimit()
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

    public void testOrderByWithNewExpression() throws Exception
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

    public void testEng1133() throws Exception
    {
        Client client = getClient();
        for(int a=0; a < 5; a++)
        {
            client.callProcedure("InsertA", a);
            client.callProcedure("InsertB", a);
        }
        VoltTable vt = client.callProcedure("@AdHoc", "select a.a, b.a from a, b where b.a >= 3 order by a.a, b.a").
                              getResults()[0];
        System.out.println(vt.toString());
        for (int i = 0; i < 10; i++)
        {
            assertEquals(i/2, vt.fetchRow(i).getLong(0));
        }
    }

    //
    // Suite builder boilerplate
    //

    public TestOrderBySuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestOrderBySuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestOrderBySuite.class.getResource("testorderby-ddl.sql"));
        project.addPartitionInfo("O1", "PKEY");
        project.addPartitionInfo("a", "a");
        project.addStmtProcedure("InsertA", "INSERT INTO A VALUES(?);");
        project.addStmtProcedure("InsertB", "INSERT INTO B VALUES(?);");
        project.addProcedures(PROCEDURES);

        config = new LocalSingleProcessServer("testorderby-onesite.jar",
                1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        config = new LocalSingleProcessServer("testorderby-threesites.jar",
                    3, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        config = new LocalSingleProcessServer("testorderby-hsql.jar",
                1, BackendTarget.HSQLDB_BACKEND);
        config.compile(project);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testorderby-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }
}
