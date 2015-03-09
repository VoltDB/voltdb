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
import java.util.Comparator;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.planner.TestPlansGroupBy;

/*
 * Functional tests of the statements compiled in the test suite
 * org.voltdb.planner.TestPlansGroupBy.
 */

public class TestGroupBySuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.CountT1A1.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.InsertF.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.InsertDims.class,
        org.voltdb_testprocs.regressionsuites.plansgroupbyprocs.SumGroupSingleJoin.class };

    /** Load 1 1's, 2 2's, 3 3's .. 10 10's and 1 11 */
    private int loaderNxN(Client client, int pkey) throws ProcCallException,
    IOException, NoConnectionsException {
        VoltTable vt;
        //String qs;
        // Insert some known data. Insert {1, 2, 2, 3, 3, 3, ... }
        for (int i = 1; i <= 10; i++) {
            for (int j = 0; j < i; j++) {
                //qs = "INSERT INTO T1 VALUES (" + pkey++ + ", " + i + ");";
                vt = client.callProcedure("T1Insert", pkey++, i).getResults()[0];
                assertTrue(vt.getRowCount() == 1);
                // assertTrue(vt.asScalarLong() == 1);
            }
        }
        // also add a single "11" to make verification a bit saner
        // (so that the table results of "count" and "group by" can be
        // distinguished)
        vt = client.callProcedure("@AdHoc", "insert into t1 values (" + pkey++
                + ",11);").getResults()[0];
        assertTrue(vt.getRowCount() == 1);
        // assertTrue(vt.asScalarLong() == 1);
        return pkey;
    }

    /** Load 1 1's, 2 2's, 3 3's .. 10 10's and 1 11 */
    private int loaderNxNb(Client client, int pkey) throws ProcCallException,
    IOException, NoConnectionsException {
        VoltTable vt;
        //String qs;
        // Insert some known data. Insert {1, 2, 2, 3, 3, 3, ... }
        for (byte i = 1; i <= 10; i++) {
            for (byte j = 0; j < i; j++) {
                // "INSERT INTO B VALUES (" + pkey++ + ", " + [i,i,0,0,i,i] + ");";
                byte b[] = { i, i, 0, 0, i, i };
                vt = client.callProcedure("BInsert", pkey++, b).getResults()[0];
                assertTrue(vt.getRowCount() == 1);
            }
        }
        // also add a single "11" to make verification a bit saner
        // (so that the table results of "count" and "group by" can be
        // distinguished)
        vt = client.callProcedure("@AdHoc", "insert into B values (" + pkey++
                + ",'0B0B00000B0B');").getResults()[0];
        assertTrue(vt.getRowCount() == 1);
        return pkey;
    }

    /** load known data to F without loading the Dimension tables
     * @throws InterruptedException */
    private int loadF(Client client, int pkey) throws NoConnectionsException,
    ProcCallException, IOException, InterruptedException {
        VoltTable vt;

        // if you want to test synchronous latency, this
        //  is a good variable to change
        boolean async = true;

        // val1 = constant value 2
        // val2 = i * 10
        // val3 = 0 for even i, 1 for odd i

        for (int i = 0; i < 1000; i++) {
            int f_d1 = i % 10; // 10 unique dim1s
            int f_d2 = i % 50; // 50 unique dim2s
            int f_d3 = i % 100; // 100 unique dim3s

            boolean done;
            SyncCallback cb = new SyncCallback();
            do {
                done = client.callProcedure(cb, "InsertF", pkey++, f_d1, f_d2, f_d3,
                                            2, (i * 10), (i % 2));
                if (!done) {
                    client.backpressureBarrier();
                }
            } while (!done);


            if (!async) {
                cb.waitForResponse();
                vt = cb.getResponse().getResults()[0];
                assertTrue(vt.getRowCount() == 1);
                // assertTrue(vt.asScalarLong() == 1);
            }
        }

        client.drain();

        return pkey;
    }

    /** load the dimension tables */
    private void loadDims(Client client) throws NoConnectionsException,
    ProcCallException, IOException {
        client.callProcedure("InsertDims");
    }

    /** select A1 from T1 group by A1 */
    public void testSelectAGroupbyA() throws IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        loaderNxN(client, 0);

        vt = client.callProcedure("@AdHoc", "Select * from T1").getResults()[0];
        System.out.println("T1-*:" + vt);

        // execute the query
        vt = client.callProcedure("@AdHoc", "SELECT A1 from T1 group by A1").getResults()[0];

        // one row per unique value of A1
        System.out.println("testSelectAGroubyA: " + vt);
        assertTrue(vt.getRowCount() == 11);

        // Selecting A1 - should get values 1 through 11
        // once each. These results aren't necessarily ordered.
        int found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            Integer A1 = (Integer) vt.get(0, VoltType.INTEGER);
            assertTrue(A1 <= 11);
            assertTrue(A1 > 0);
            found[A1.intValue()] += 1;
        }
        assertEquals(0, found[0]);
        for (int i = 1; i < 12; i++) {
            assertEquals(1, found[i]);
        }
    }

    /** select B_VAL1 from B group by B_VAL1 */
    public void testSelectGroupbyVarbinary() throws IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        loaderNxNb(client, 0);

        vt = client.callProcedure("@AdHoc", "Select * from B").getResults()[0];
        System.out.println("B-*:" + vt);

        // execute the query
        vt = client.callProcedure("@AdHoc", "SELECT B_VAL1 from B group by B_VAL1").getResults()[0];

        // one row per unique value of A1
        System.out.println("testSelectGroubyVarbinary: " + vt);
        assertTrue(vt.getRowCount() == 11);

        // Selecting B_VAL1 - should get byte values "1,1,1,1,1,1" through "11,11,11,11,11,11"
        // once each. These results aren't necessarily ordered.
        byte found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            byte[] b_val1 = vt.getVarbinary(0);
            assertTrue(b_val1.length == 6);
            assertTrue(b_val1[0] <= 11);
            assertTrue(b_val1[5] <= 11);
            assertTrue(b_val1[0] > 0);
            assertTrue(b_val1[5] > 0);
            found[b_val1[0]] += 1;
        }
        assertEquals(0, found[0]);
        for (int i = 1; i < 12; i++) {
            assertEquals(1, found[i]);
        }
    }

    /** select count(A1) from T1 group by A1 */
    public void testSelectCountAGroupbyA() throws IOException,
    ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        loaderNxN(client, 0);

        vt = client.callProcedure("@AdHoc",
        "select count(A1), A1 from T1 group by A1").getResults()[0];
        System.out.println("testSelectCountAGroupbyA result: " + vt);
        assertTrue(vt.getRowCount() == 11);

        // Selecting count(A1) - should get two counts of 1 and one count each
        // of 2-10: (1, 1, 2, 3, 4, .. 10).
        // These results aren't necessarily ordered
        int found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            Integer A1 = (Integer) vt.get(0, VoltType.INTEGER);
            assertTrue(A1 <= 10);
            assertTrue(A1 > 0);
            found[A1.intValue()] += 1;
        }
        assertEquals(0, found[0]);
        assertEquals(2, found[1]);
        for (int i = 2; i < 11; i++) {
            assertEquals(1, found[i]);
        }
    }

    /** select A1, sum(A1) from T1 group by A1 */
    public void testSelectSumAGroupbyA() throws IOException, ProcCallException {
        VoltTable vt;
        Client client = this.getClient();
        loaderNxN(client, 0);

        String qs = "select A1, sum(A1) from T1 group by A1";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        System.out.println("testSelectSumAGroupbyA result: " + vt);
        assertEquals(11, vt.getRowCount());

        int found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            Integer a1 = (Integer) vt.get(0, VoltType.INTEGER);
            Integer sum = (Integer) vt.get(1, VoltType.INTEGER);
            found[a1.intValue()] += 1;
            // A1 = 11 is a special case
            if (a1.intValue() == 11)
                assertEquals(11, sum.intValue());
            // every other n appears n times. The sum is therefore n x n.
            else
                assertEquals(a1.intValue() * a1.intValue(), sum.intValue());
        }
        assertEquals(0, found[0]);
        for (int i = 1; i < 12; i++)
            assertEquals(found[i], 1);  // one result for each unique A1
    }

    /** select count(distinct A1) from T1 */
    public void testSelectCountDistinct() throws IOException, ProcCallException {
        VoltTable vt;
        Client client = getClient();
        loaderNxN(client, 0);
        vt = client
        .callProcedure("@AdHoc", "select count(distinct A1) from T1").getResults()[0];
        assertTrue(vt.getRowCount() == 1);

        // there are 11 distinct values for A1
        while (vt.advanceRow()) {
            Integer A1 = (Integer) vt.get(0, VoltType.INTEGER);
            assertEquals(11, A1.intValue());
        }
    }

    /** select count(A1) from T1 */
    public void testSelectCount() throws IOException, ProcCallException {
        VoltTable vt;
        Client client = getClient();
        loaderNxN(client, 0);
        vt = client.callProcedure("@AdHoc", "select count(A1) from T1").getResults()[0];
        assertTrue(vt.getRowCount() == 1);

        // there are 56 rows in the table 1 + 2 + 3 + .. + 10 + 1
        while (vt.advanceRow()) {
            Integer A1 = (Integer) vt.get(0, VoltType.INTEGER);
            System.out.println("select count = " + A1.intValue());
            assertEquals(56, A1.intValue());
        }
    }

    /** select distinct a1 from t1 */
    public void testSelectDistinctA() throws IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        loaderNxN(client, 0);

        vt = client.callProcedure("@AdHoc", "select distinct a1 from t1").getResults()[0];
        System.out.println("testSelectDistinctA result row("
                + vt.getColumnName(0) + ") " + vt);

        // valid result is the set {1,2,...,11}
        int found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            Integer A1 = (Integer) vt.get(0, VoltType.INTEGER);
            System.out.println("\tdistinct value: " + A1.intValue());
            assertEquals("A1", vt.getColumnName(0));
            assertTrue(A1 <= 11);
            assertTrue(A1 > 0);
            found[A1.intValue()] += 1;
        }
        assertEquals(0, found[0]);
        for (int i = 1; i < 12; i++) {
            assertEquals(1, found[i]);
        }
    }

    /**
     * distributed sums of a partitioned table
     * select sum(F_VAL1), sum(F_VAL2), sum(F_VAL3) from F
     * @throws InterruptedException
     */
    public void testDistributedSum() throws IOException, ProcCallException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);

        String qs = "select sum(F_VAL1), sum(F_VAL2), sum(F_VAL3) from F";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        System.out.println("testDistributedSum result: " + vt);
        assertTrue(vt.getRowCount() == 1);
        vt.advanceRow();
        Integer sum1 = (Integer) vt.get(0, VoltType.INTEGER);
        assertEquals(2000, sum1.intValue());
        Integer sum2 = (Integer) vt.get(1, VoltType.INTEGER);
        assertEquals(4995000, sum2.intValue());
        Integer sum3 = (Integer) vt.get(2, VoltType.INTEGER);
        assertEquals(500, sum3.intValue());

        // Also, regression test ENG-199 -- duplicate aggregation column.
        vt = client.callProcedure("@AdHoc", "select sum(F_VAL1), sum(F_VAL1) from F").getResults()[0];
        System.out.println("testDistributedSum result: " + vt);
        assertTrue(vt.getRowCount() == 1);
        vt.advanceRow();
        sum1 = (Integer) vt.get(0, VoltType.INTEGER);
        assertEquals(2000, sum1.intValue());
        try {
            sum2 = (Integer) vt.get(1, VoltType.INTEGER);
            assertEquals(2000, sum2.intValue());
        } catch ( Exception exc ) {
            fail("Apparently failing like ENG-199 with: " + exc);
        }
    }

    /**
     * distributed sums of a view
     * select sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) from V
     * @throws InterruptedException
     */
    public void testDistributedSum_View() throws IOException, ProcCallException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);

        String qs = "select sum(V.SUM_v1), sum(V.SUM_V2), sum(V.SUM_V3) from V";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        System.out.println("testDistributedSum_View result: " + vt);
        assertTrue(vt.getRowCount() == 1);
        while (vt.advanceRow()) {
            Integer sum1 = (Integer) vt.get(0, VoltType.INTEGER);
            assertEquals(2000, sum1.intValue());
            Integer sum2 = (Integer) vt.get(1, VoltType.INTEGER);
            assertEquals(4995000, sum2.intValue());
            Integer sum3 = (Integer) vt.get(2, VoltType.INTEGER);
            assertEquals(500, sum3.intValue());
        }
    }

    /**
     * distributed sums of a view (REDUNDANT GROUP BY)
     * select V.D1_PKEY, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3)
     * from V group by V.V_D1_PKEY
     * @throws InterruptedException
     */
    public void testDistributedSumAndGroup() throws NoConnectionsException,
    ProcCallException, IOException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);

        String qs = "select V.V_D1_PKEY, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) "
            + "from V group by V.V_D1_PKEY";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        System.out.println("testDistributedSumAndJoin result: " + vt);
        assert (vt.getRowCount() == 10); // 10 unique values for dim1 which is
        // the grouping col

        int found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            Integer d1 = (Integer) vt.get(0, VoltType.INTEGER);
            Integer s1 = (Integer) vt.get(1, VoltType.INTEGER);
            Integer s2 = (Integer) vt.get(2, VoltType.INTEGER);
            Integer s3 = (Integer) vt.get(3, VoltType.INTEGER);

            // track that 10 dim1s are in the final group
            found[d1.intValue()] += 1;
            // sum1 is const 2. 100 dim1 instances / group
            assertEquals(200, s1.intValue());
            // sum of every 10th i * 10 in this range
            assertTrue(495000 <= s2.intValue() && 504000 >= s2.intValue());
            // sum3 alternates 0|1. There are 100 dim1 instances / group
            if ((d1.intValue() % 2) == 0)
                assertEquals(s3.intValue(), 0);
            else
                assertEquals(s3.intValue(), 100);

        }
        for (int i = 0; i < 10; i++)
            assertEquals(1, found[i]);

    }

    /**
     * distributed sum of a view with a group by and join on a replicated table.
     * (REDUNDANT GROUP BY)
     * select D1.D1_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3)
     * from D1, V where D1.D1_PKEY = V.V_D1_PKEY group by D1.D1_NAME
     * @throws InterruptedException
     */
    public void testDistributedSumGroupSingleJoin()
    throws NoConnectionsException, ProcCallException, IOException, InterruptedException {
        VoltTable vt;
        Client client = getClient();

        loadF(client, 0);
        loadDims(client);

        vt = client.callProcedure("SumGroupSingleJoin").getResults()[0];
        assertTrue(vt.getRowCount() == 10);

        int found[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        while (vt.advanceRow()) {
            String d1 = (String) vt.get(0, VoltType.STRING);
            Integer s1 = (Integer) vt.get(1, VoltType.INTEGER);
            Integer s2 = (Integer) vt.get(2, VoltType.INTEGER);
            Integer s3 = (Integer) vt.get(3, VoltType.INTEGER);
            // sum1 is const 2; 100 dim1 instances per group.
            assertEquals(200, s1.intValue());
            assertTrue(495000 <= s2.intValue() && 504000 >= s2.intValue());
            assertTrue(s3.intValue() == 0 || s3.intValue() == 100);

            Integer di = Integer.valueOf(d1.substring(3));
            found[di.intValue()] += 1;
        }
        for (int i = 0; i < 10; i++)
            assertEquals(1, found[i]);
    }

    /**
     * distributed sum of a view with a join on a replicated table for one dim value
     * (REDUNDANT GROUP BY)
     * select D1.D1_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3)
     * from D1, V where D1.D1_PKEY = V.V_D1_PKEY and D1.D1_PKEY = ?
     * group by D1_NAME
     * @throws InterruptedException
     */
    public void testDistributedSumGroupSingleJoinOneDim() throws IOException,
    ProcCallException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);
        loadDims(client);

        String qs = "select D1.D1_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) "
            + " from D1, V where D1.D1_PKEY = V.V_D1_PKEY and D1.D1_PKEY = 5"
            + " group by D1.D1_NAME";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        assertTrue(vt.getRowCount() == 1);
        System.out.println("testDistributedSumGroupSingleJoinOneDim: " + vt);
        while (vt.advanceRow()) {
            String d1 = (String) vt.get(0, VoltType.STRING);
            Integer s1 = (Integer) vt.get(1, VoltType.INTEGER);
            Integer s2 = (Integer) vt.get(2, VoltType.INTEGER);
            Integer s3 = (Integer) vt.get(3, VoltType.INTEGER);

            assertEquals(d1, "D1_5");             // name is D1_%d where %d is pkey
            assertEquals(200, s1.intValue());     // dim1 present 100 times. s1 == 2.
            assertEquals(500000, s2.intValue());  // verified in hsql.
            assertEquals(100, s3.intValue());     // odd dim1 == 1 in s3 ( x100 )
        }
    }

    /**
     * distributed sum of a view with 3-way join on replicated tables
     * (REDUNDANT GROUP BY)
     * select D1.D1_NAME, D2.D2_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3)
     * from D1, D2, V where V.V_D1_PKEY = D1.D1_PKEY and V.V_D2_PKEY = D2.D2_PKEY
     * group by D1_NAME, D2_NAME
     * @throws InterruptedException
     */
    public void testDistributedSumGroupMultiJoin() throws IOException,
    ProcCallException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);
        loadDims(client);

        String qs = "select D1.D1_NAME, D2.D2_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) "
            + "from V, D1, D2 "
            + "where V.V_D1_PKEY = D1.D1_PKEY and V.V_D2_PKEY = D2.D2_PKEY "
            + "group by D1.D1_NAME, D2.D2_NAME";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        System.out.println("DistributedSumGroupMultiJoin: " + vt);

        // sort the output by d2's value
        ArrayList<VoltTableRow> sorted = new ArrayList<VoltTableRow>();
        while (vt.advanceRow()) {
            String d1 = (String) vt.get(0, VoltType.STRING);
            String d2 = (String) vt.get(1, VoltType.STRING);
            System.out.println("Adding Row: " + d1 + ", " + d2);
            // this will add the active row of vt
            sorted.add(vt.cloneRow());
        }
        System.out.println("DSGMJonedim"); debug(sorted);
        Collections.sort(sorted, new VRowComparator<VoltTableRow>());
        System.out.println("DSGMJonedim: "); debug(sorted);

        // 5 unique d2's for each of 10 d1's (so 10 * 5 rows)
        assertEquals(50, vt.getRowCount());
        Integer i = 0, j = 0;
        for (VoltTableRow row : sorted) {
            String d1_name = "D1_" + i;
            String d2_name = "D2_" + (i + (j * 10));
            int v3 = (i % 2) * 20;   // 20 unique combinations of d1, d2, d3

            String d1 = (String)   row.get(0, VoltType.STRING);
            String d2 = (String)   row.get(1, VoltType.STRING);
            Integer s1 = (Integer) row.get(2, VoltType.INTEGER);
            Integer s3 = (Integer) row.get(4, VoltType.INTEGER);

            assertEquals(d1, d1_name);
            assertEquals(d2, d2_name);
            assertEquals(s1.intValue(), 40);
            assertEquals(s3.intValue(), v3);
            j++;  if (j == 5) { i++; j = 0; }
        }
    }

    /**
     * distributed sum of a view with 3-way join on replicated table for
     * specific dim1 (REDUNDANT GROUP BY)
     * select D1.D1_NAME, D2.D2_NAME, sum(V.SUM_V1),
     * sum(V.SUM_V2), sum(V.SUM_V3) from D1, D2, V where V.V_D1_PKEY =
     * D1.D1_PKEY and V.V_D2_PKEY = D2.D2_PKEY and D1.D1_PKEY = ?
     * group by D1_NAME, D2_NAME
     * @throws InterruptedException
     */
    public void testDistributedSumGroupMultiJoinOneDim() throws IOException,
    ProcCallException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);
        loadDims(client);

        String qs = "select D1.D1_NAME, D2.D2_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) "
            + "from D1, D2, V "
            + "where V.V_D1_PKEY = D1.D1_PKEY and V.V_D2_PKEY = D2.D2_PKEY and D1.D1_PKEY = 6 "
            + "group by D1.D1_NAME, D2.D2_NAME;";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        // 5 unique values of d2 for each value of d1 (and a single d1 value is selected above)
        assertEquals(vt.getRowCount(), 5);

        // sort the output by d2's value
        ArrayList<VoltTableRow> sorted = new ArrayList<VoltTableRow>();
        while (vt.advanceRow()) {
            // this will add the active row of vt
            sorted.add(vt.cloneRow());
        }
        System.out.println("DSGMJonedim"); debug(sorted);
        Collections.sort(sorted, new VRowComparator<VoltTableRow>());
        System.out.println("DSGMJonedim: "); debug(sorted);

        int i = 0;
        for (VoltTableRow row : sorted) {
            String d2_name = "D2_" + ((i * 10) + 6);

            String d1 = (String)   row.get(0, VoltType.STRING);
            String d2 = (String)   row.get(1, VoltType.STRING);
            Integer s1 = (Integer) row.get(2, VoltType.INTEGER);
            Integer s3 = (Integer) row.get(4, VoltType.INTEGER);

            System.out.println("D2 expected: " + d2_name + " actual: " + d2);

            assertEquals(d1, "D1_6");
            assertEquals(d2, d2_name);
            assertEquals(s1.intValue(), 40);  // 20 unique combinations * 2.
            assertEquals(s3.intValue(), 0);   // all even d1's are 0 in s3
            i++;
        }
    }

    /**
     * distributed sum of a view with 4-way join on replicated tables for
     * specific dim1, dim2 (REDUNDANT GROUP BY)
     * select D1.D1_NAME, D2.D2_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3)
     * from D1, D2, V where V.V_D1_PKEY = D1.D1_PKEY and V.V_D2_PKEY = D2.D2_PKEY
     * and D1.D1_PKEY = ? and D2.D2_PKEY = ? group by D1_NAME, D2_NAME
     * @throws InterruptedException
     */
    public void testDistributedSumGroupMultiJoinTwoDims() throws IOException, ProcCallException, InterruptedException {
        VoltTable vt;
        Client client = getClient();
        loadF(client, 0);
        loadDims(client);

        String qs = "select D1.D1_NAME, D2.D2_NAME, D3.D3_NAME, sum(V.SUM_V1), sum(V.SUM_V2), sum(V.SUM_V3) "
            + "from D1, D2, D3, V "
            + "where V.V_D1_PKEY = D1.D1_PKEY and V.V_D2_PKEY = D2.D2_PKEY and V.V_D3_PKEY = D3.D3_PKEY "
            +        "and D1.D1_PKEY = 6 and D2.D2_PKEY = 26 "
            + "group by D1.D1_NAME, D2.D2_NAME, D3.D3_NAME;";

        vt = client.callProcedure("@AdHoc", qs).getResults()[0];
        System.out.println("MultiJoin3Dims: " + vt);

        // output looks like this - in either ordering
        // D1_6, D2_26, D3_76, 20, 52600, 0,
        // D1_6, D2_26, D3_26, 20, 47600, 0,
        while (vt.advanceRow()) {
            String d1 = (String)   vt.get(0, VoltType.STRING);
            String d2 = (String)   vt.get(1, VoltType.STRING);
            String d3 = (String)   vt.get(2, VoltType.STRING);
            Integer s1 = (Integer) vt.get(3, VoltType.INTEGER);
            Integer s3 = (Integer) vt.get(4, VoltType.INTEGER);

            assertEquals("D1_6", d1);
            assertEquals("D2_26", d2);
            assertEquals(20, s1.intValue());
            if (d3.equals("D3_26"))
                assertEquals(47600, s3.intValue());
            else if (d3.equals("D3_76"))
                assertEquals(52600, s3.intValue());
            else
                fail();
        }
    }

    // Fix bug: serial grouping by an inline varchar field only has one group
    public void testENG6732_serialAggInlineVarchar() throws IOException, ProcCallException, InterruptedException {
        System.out.println("STARTING serial/parital aggregate test.....");
        String sql;
        VoltTable vt;

        Client client = this.getClient();

        String[] tbNames = {"VOTES", "VOTESBYTES"};

        for (String tbName : tbNames) {
            String proc = tbName + ".insert";
            client.callProcedure(proc, 1, "MA", 1);
            client.callProcedure(proc, 2, "RI", 2);
            client.callProcedure(proc, 3, "CA", 1);
            client.callProcedure(proc, 4, "MA", 2);
            client.callProcedure(proc, 5, "CA", 1);


            sql = "select state, count(*) from " + tbName + " group by state order by 1, 2";
            vt = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(vt.toString().toLowerCase().contains("serial"));

            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            assertEquals(3, vt.getRowCount());
            vt.advanceRow(); assertEquals("CA", vt.getString(0)); assertEquals(2, vt.getLong(1));
            vt.advanceRow(); assertEquals("MA", vt.getString(0)); assertEquals(2, vt.getLong(1));
            vt.advanceRow(); assertEquals("RI", vt.getString(0)); assertEquals(1, vt.getLong(1));

            // test partial serial aggregate
            sql = " select state, contestant_number, count(*) from  " + tbName +
                  " group by state, contestant_number order by 1, 2";
            vt = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(vt.toString().toLowerCase().contains("partial"));

            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            assertEquals(4, vt.getRowCount());
            vt.advanceRow(); assertEquals("CA", vt.getString(0)); assertEquals(1, vt.getLong(1)); assertEquals(2, vt.getLong(2));
            vt.advanceRow(); assertEquals("MA", vt.getString(0)); assertEquals(1, vt.getLong(1)); assertEquals(1, vt.getLong(2));
            vt.advanceRow(); assertEquals("MA", vt.getString(0)); assertEquals(2, vt.getLong(1)); assertEquals(1, vt.getLong(2));
            vt.advanceRow(); assertEquals("RI", vt.getString(0)); assertEquals(2, vt.getLong(1)); assertEquals(1, vt.getLong(2));
        }
    }


    public void testPartialAggregate() throws IOException, ProcCallException, InterruptedException {
        System.out.println("STARTING partial aggregate test.....");
        String sql;
        VoltTable vt;

        Client client = this.getClient();
        loadF(client, 0);

        // Have an index on column F_D1,
        // index keep F_D1 ordered but not enough ordering for serial aggregate for whole query.
        sql = "SELECT F_D1, F_D2, SUM(F_D3) FROM F GROUP BY F_D1, F_D2 ORDER BY 1, 2 LIMIT 5 OFFSET 3";
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().toLowerCase().contains("partial"));
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, new long[][] {{0,30,1100}, {0,40,1300},
            {1,1,520}, {1,11,720},{1,21,920} });

        // Have an index on expression ABS(F_D1)
        // index keep F_D1 ordered but not enough ordering for serial aggregate for whole query.
        sql = "SELECT ABS(F_D1), F_D3, COUNT(*) FROM F GROUP BY ABS(F_D1), F_D3 ORDER BY 1, 2 LIMIT 5 OFFSET 8";
        vt = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(vt.toString().toLowerCase().contains("partial"));
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][] {{0,80,10}, {0,90,10},
                {1,1,10}, {1,11,10},{1,21,10} });

        // Joined with aggregation is tested in SQL Coverage tests.
    }


    //
    // Suite builder boilerplate
    //

    public TestGroupBySuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestGroupBySuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestPlansGroupBy.class
                .getResource("testplans-groupby-ddl.sql"));
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("T1Insert", "INSERT INTO T1 VALUES (?, ?);");
        project.addStmtProcedure("BInsert", "INSERT INTO B VALUES (?, ?);");

        // config = new LocalSingleProcessServer("plansgroupby-ipc.jar", 1, BackendTarget.NATIVE_EE_IPC);
        // config.compile(project);
        // builder.addServerConfig(config);

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        config = new LocalCluster("plansgroupby-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("plansgroupby-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);

        return builder;
    }

    public class VRowComparator<T> implements Comparator<VoltTableRow>
    {
        @Override
        public int compare(VoltTableRow r1, VoltTableRow r2) {
            String r1d1 = (String) r1.get(0, VoltType.STRING);
            String r1d2 = (String) r1.get(1, VoltType.STRING);
            String r2d1 = (String) r2.get(0, VoltType.STRING);
            String r2d2 = (String) r2.get(1, VoltType.STRING);

            int r1d1_pos = Integer.valueOf(r1d1.substring(3));
            int r1d2_pos = Integer.valueOf(r1d2.substring(3));
            int r2d1_pos = Integer.valueOf(r2d1.substring(3));
            int r2d2_pos = Integer.valueOf(r2d2.substring(3));

            System.out.printf("comparing (%s, %s) to (%s, %s)\n",
                    r1d1, r1d2, r2d1, r2d2);

            if (r1d1_pos != r2d1_pos)
                return r1d1_pos - r2d1_pos;

            if (r1d2_pos != r2d2_pos)
                return r1d2_pos - r2d2_pos;

            return 0;
        }
    }

    private void debug(ArrayList<VoltTableRow> sorted) {
        for (VoltTableRow row : sorted) {
            String d1 = (String) row.get(0, VoltType.STRING);
            String d2 = (String) row.get(1, VoltType.STRING);
            System.out.println("Row: " + d1 + ", " + d2);
        }
    }
}
