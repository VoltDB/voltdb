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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.sqlfeatureprocs.BatchedMultiPartitionTest;

import junit.framework.Test;

public class TestIndexScanCountSuite extends RegressionSuite {
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestIndexScanCountSuite(String name) {
        super(name);
    }

    void callWithExpectedCount(Client client,int count, String procName, Object... params)
            throws NoConnectionsException, IOException, ProcCallException {

        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(count, result.getLong(0));
    }

    void callAdHocFilterWithExpectedCount(Client client, String tbName, String filters, int expectedCounts) throws Exception {
        VoltTable vt;
        String sql = "SELECT COUNT(ID) FROM " + tbName + " WHERE " + filters;
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];

        assertTrue(vt.getRowCount() == 1);
        assertTrue(vt.advanceRow());
        assertEquals(expectedCounts, vt.getLong(0));
    }

    public void testOverflow() throws Exception {
        Client client = getClient();
        // Unique Map, Single column index
        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, 3);
        client.callProcedure("TU1.insert", 6, 6);
        client.callProcedure("TU1.insert", 8, 8);
        client.callProcedure("TU1.insert", 10, null);

        callWithExpectedCount(client, 5, "TU1_LT", 6000000000L);
        callWithExpectedCount(client, 5, "TU1_LET", 6000000000L);
        callWithExpectedCount(client, 0, "TU1_GT", 6000000000L);
        callWithExpectedCount(client, 0, "TU1_GET", 6000000000L);

        callWithExpectedCount(client, 0, "TU1_LT", -6000000000L);
        callWithExpectedCount(client, 0, "TU1_LET", -6000000000L);
        callWithExpectedCount(client, 5, "TU1_GT", -6000000000L);
        callWithExpectedCount(client, 5, "TU1_GET", -6000000000L);

        // Unique Map, two column index
        client.callProcedure("TU3.insert", 1, 1, 123);
        client.callProcedure("TU3.insert", 2, 2, 123);
        client.callProcedure("TU3.insert", 3, 3, 123);
        client.callProcedure("TU3.insert", 4, 6, 123);
        client.callProcedure("TU3.insert", 5, 8, 123);
        client.callProcedure("TU3.insert", 6, 1, 456);
        client.callProcedure("TU3.insert", 7, 2, 456);
        client.callProcedure("TU3.insert", 8, 3, 456);
        client.callProcedure("TU3.insert", 9, 6, 456);
        client.callProcedure("TU3.insert", 10, 8, 456);

        client.callProcedure("TU3.insert", 11, null, 123);
        client.callProcedure("TU3.insert", 12, null, 456);

        callWithExpectedCount(client, 5, "TU3_LT", 123, 6000000000L);
        callWithExpectedCount(client, 3, "TU3_GET_LT", 123, 3, 6000000000L);
        callWithExpectedCount(client, 3, "TU3_GET_LET", 123, 3, 6000000000L);
        callWithExpectedCount(client, 2, "TU3_GT_LET", 123, 3, 6000000000L);
        callWithExpectedCount(client, 2, "TU3_GT_LT", 123, 3, 6000000000L);

        // Multi-map, two column index
        client.callProcedure("TM2.insert", 1, 1, "xin");
        client.callProcedure("TM2.insert", 2, 2, "xin");
        client.callProcedure("TM2.insert", 3, 3, "xin");
        client.callProcedure("TM2.insert", 4, 3, "xin");
        client.callProcedure("TM2.insert", 5, 3, "xin");
        client.callProcedure("TM2.insert", 6, 5, "xin");
        client.callProcedure("TM2.insert", 7, 6, "xin");
        client.callProcedure("TM2.insert", 8, 6, "xin");
        client.callProcedure("TM2.insert", 9, 8, "xin");
        client.callProcedure("TM2.insert", 10, 8, "xin");

        client.callProcedure("TM2.insert", 11, 1, "jia");
        client.callProcedure("TM2.insert", 12, 2, "jia");
        client.callProcedure("TM2.insert", 13, 3, "jia");
        client.callProcedure("TM2.insert", 14, 3, "jia");
        client.callProcedure("TM2.insert", 15, 3, "jia");
        client.callProcedure("TM2.insert", 16, 5, "jia");
        client.callProcedure("TM2.insert", 17, 6, "jia");
        client.callProcedure("TM2.insert", 18, 6, "jia");
        client.callProcedure("TM2.insert", 19, 8, "jia");
        client.callProcedure("TM2.insert", 20, 8, "jia");

        client.callProcedure("TM2.insert", 100, null, "xin");
        client.callProcedure("TM2.insert", 200, null, "jia");

        callWithExpectedCount(client, 5, "TM2_GT_LT", "xin", 3, 6000000000L);
        callWithExpectedCount(client, 8, "TM2_GET_LT", "xin", 3, 6000000000L);
        callWithExpectedCount(client, 5, "TM2_GT_LET", "xin", 3, 6000000000L);
        callWithExpectedCount(client, 8, "TM2_GET_LET", "xin", 3, 6000000000L);
    }

    public void testOneColumnUniqueIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TU1.insert", 1, 1);
        client.callProcedure("TU1.insert", 2, 2);
        client.callProcedure("TU1.insert", 3, 3);
        client.callProcedure("TU1.insert", 6, 6);
        client.callProcedure("TU1.insert", 8, 8);
        client.callProcedure("TU1.insert", 10, null);

        VoltTable table;

        table = client.callProcedure("@Explain","SELECT COUNT(ID) FROM TU1 WHERE POINTS < -1").getResults()[0];
        String explainPlan = table.toString();
        // Before ENG-6131 is fixed the plan should have INDEX SCAN and not have INDEX COUNT
        assertTrue(explainPlan.contains("INDEX COUNT"));
        assertFalse(explainPlan.contains("INDEX SCAN"));


        table = client.callProcedure("@AdHoc","SELECT (COUNT(ID) + 1) FROM TU1 WHERE POINTS < 2").getResults()[0];
        assertTrue(table.getRowCount() == 1);
        assertTrue(table.advanceRow());
        assertEquals(2, table.getLong(0));

        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS < -1", 0);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS < 1", 0);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS <= 1", 1);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS < 5", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS <= 5", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS <= 6", 4);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS < 8", 4);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS <= 8", 5);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS < 1000", 5);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS <= 1000", 5);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > -1", 5);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= -1", 5);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 1", 4);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 1", 5);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 2", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 2", 4);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 4", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 4", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 8", 0);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 8", 1);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 1000", 0);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 1000", 0);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > -1 AND POINTS <= 6", 4);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > -1 AND POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= -1 AND POINTS <= 6", 4);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= -1 AND POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 2 AND POINTS <= 6", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 2 AND POINTS <= 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 2 AND POINTS < 6", 1);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 2 AND POINTS < 6", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 5 AND POINTS <= 8", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 5 AND POINTS < 8", 1);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 5 AND POINTS <= 8", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 5 AND POINTS < 8", 1);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 4 AND POINTS <= 9", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS > 4 AND POINTS < 9", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 4 AND POINTS <= 9", 2);
        callAdHocFilterWithExpectedCount(client,"TU1", "POINTS >= 4 AND POINTS < 9", 2);

    }

    public void testTwoOrMoreColumnsUniqueIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TU2.insert", 1, 1, "xin");
        client.callProcedure("TU2.insert", 2, 2, "xin");
        client.callProcedure("TU2.insert", 3, 3, "xin");
        client.callProcedure("TU2.insert", 4, 6, "xin");
        client.callProcedure("TU2.insert", 5, 8, "xin");
        client.callProcedure("TU2.insert", 6, 1, "jiao");
        client.callProcedure("TU2.insert", 7, 2, "jiao");
        client.callProcedure("TU2.insert", 8, 3, "jiao");
        client.callProcedure("TU2.insert", 9, 6, "jiao");
        client.callProcedure("TU2.insert", 10, 8, "jiao");

        client.callProcedure("TU2.insert", 11, null, "xin");
        client.callProcedure("TU2.insert", 12, null, "jiao");

        // test with 2,6
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 2 AND POINTS < 6", 1);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < -1", 0);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < 1", 0);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS <= 1", 1);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < 5", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS <= 5", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS <= 6", 4);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < 8", 4);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS <= 8", 5);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS < 1000", 5);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS <= 1000", 5);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > -1", 5);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= -1", 5);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 1", 4);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 1", 5);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 2", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 2", 4);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 4", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 4", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 8", 0);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 8", 1);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 1000", 0);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 1000", 0);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > -1 AND POINTS <= 6", 4);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > -1 AND POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= -1 AND POINTS <= 6", 4);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= -1 AND POINTS < 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 2 AND POINTS <= 6", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 2 AND POINTS <= 6", 3);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 2 AND POINTS < 6", 1);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 2 AND POINTS < 6", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 5 AND POINTS <= 8", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 5 AND POINTS < 8", 1);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 5 AND POINTS <= 8", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 5 AND POINTS < 8", 1);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 4 AND POINTS <= 9", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS > 4 AND POINTS < 9", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 4 AND POINTS <= 9", 2);
        callAdHocFilterWithExpectedCount(client,"TU2", "UNAME = 'jiao' AND POINTS >= 4 AND POINTS < 9", 2);
    }

    public void testTwoColumnsUniqueOverflowIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TU3.insert", 1, 1, 123);
        client.callProcedure("TU3.insert", 2, 2, 123);
        client.callProcedure("TU3.insert", 3, 3, 123);
        client.callProcedure("TU3.insert", 4, 6, 123);
        client.callProcedure("TU3.insert", 5, 8, 123);
        client.callProcedure("TU3.insert", 6, 1, 456);
        client.callProcedure("TU3.insert", 7, 2, 456);
        client.callProcedure("TU3.insert", 8, 3, 456);
        client.callProcedure("TU3.insert", 9, 6, 456);
        client.callProcedure("TU3.insert", 10, 8, 456);

        client.callProcedure("TU3.insert", 11, null, 123);
        client.callProcedure("TU3.insert", 12, null, 456);

        callWithExpectedCount(client, 5, "TU3_LT", 123, 6000000000L);

        callWithExpectedCount(client, 3, "TU3_GET_LT", 123, 3, 6000000000L);
        callWithExpectedCount(client, 3, "TU3_GET_LET", 123, 3, 6000000000L);
        callWithExpectedCount(client, 2, "TU3_GT_LET", 123, 3, 6000000000L);
        callWithExpectedCount(client, 2, "TU3_GT_LT", 123, 3, 6000000000L);
    }

    public void testThreeColumnsUniqueIndex() throws Exception {
        Client client = getClient();
        client.callProcedure("TU4.insert", 1, 1, "xin", 0);
        client.callProcedure("TU4.insert", 2, 2, "xin", 1);
        client.callProcedure("TU4.insert", 3, 3, "xin", 0);
        client.callProcedure("TU4.insert", 4, 6, "xin", 1);
        client.callProcedure("TU4.insert", 5, 8, "xin", 0);
        client.callProcedure("TU4.insert", 6, 1, "jia", 0);
        client.callProcedure("TU4.insert", 7, 2, "jia", 1);
        client.callProcedure("TU4.insert", 8, 3, "jia", 0);
        client.callProcedure("TU4.insert", 9, 6, "jia", 1);
        client.callProcedure("TU4.insert", 10, 8, "jia", 0);

        client.callProcedure("TU4.insert", 11, null, "xin", 0);
        client.callProcedure("TU4.insert", 12, null, "jia", 0);

        // test with 2,6
        callAdHocFilterWithExpectedCount(client,"TU4", "UNAME = 'xin' AND SEX = 0 AND POINTS < 6", 2);
        callAdHocFilterWithExpectedCount(client,"TU4", "UNAME = 'xin' AND SEX = 0 AND POINTS >= 2 AND POINTS < 6", 1);
    }

    public void testOneColumnMultiIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TM1.insert", 1, 1);
        client.callProcedure("TM1.insert", 2, 2);

        client.callProcedure("TM1.insert", 3, 3);
        client.callProcedure("TM1.insert", 4, 3);
        client.callProcedure("TM1.insert", 5, 3);

        client.callProcedure("TM1.insert", 6, 5);

        client.callProcedure("TM1.insert", 7, 6);
        client.callProcedure("TM1.insert", 8, 6);

        client.callProcedure("TM1.insert", 9, 8);
        client.callProcedure("TM1.insert", 10, 8);

        client.callProcedure("TM1.insert", 11, null);
        client.callProcedure("TM1.insert", 12, null);

        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS < -1", 0);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS < 1", 0);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS <= 1", 1);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS < 5", 5);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS <= 5", 6);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS < 7", 8);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS <= 7", 8);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS < 8", 8);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS <= 8", 10);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS < 12", 10);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS <= 12", 10);

        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 1000", 0);

        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > -1", 10);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 3", 5);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS >= 3", 8);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS >= 4", 5);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 4", 5);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS >= -1 AND POINTS <= 6", 8);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS >= -100 AND POINTS <= 1200", 10);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS >= 2 AND POINTS <= 6", 7);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS >= 2 AND POINTS < 6", 5);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 2 AND POINTS <= 6", 6);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 2 AND POINTS < 6", 4);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 3 AND POINTS <= 6", 3);
        callAdHocFilterWithExpectedCount(client,"TM1", "POINTS > 3 AND POINTS < 6", 1);
    }

    public void testTwoColumnsMultiIndex() throws Exception {
        Client client = getClient();

        client.callProcedure("TM2.insert", 1, 1, "xin");
        client.callProcedure("TM2.insert", 2, 2, "xin");
        client.callProcedure("TM2.insert", 3, 3, "xin");
        client.callProcedure("TM2.insert", 4, 3, "xin");
        client.callProcedure("TM2.insert", 5, 3, "xin");
        client.callProcedure("TM2.insert", 6, 5, "xin");
        client.callProcedure("TM2.insert", 7, 6, "xin");
        client.callProcedure("TM2.insert", 8, 6, "xin");
        client.callProcedure("TM2.insert", 9, 8, "xin");
        client.callProcedure("TM2.insert", 10, 8, "xin");

        client.callProcedure("TM2.insert", 11, 1, "jia");
        client.callProcedure("TM2.insert", 12, 2, "jia");
        client.callProcedure("TM2.insert", 13, 3, "jia");
        client.callProcedure("TM2.insert", 14, 3, "jia");
        client.callProcedure("TM2.insert", 15, 3, "jia");
        client.callProcedure("TM2.insert", 16, 5, "jia");
        client.callProcedure("TM2.insert", 17, 6, "jia");
        client.callProcedure("TM2.insert", 18, 6, "jia");
        client.callProcedure("TM2.insert", 19, 8, "jia");
        client.callProcedure("TM2.insert", 20, 8, "jia");

        client.callProcedure("TM2.insert", 100, null, "xin");
        client.callProcedure("TM2.insert", 200, null, "jia");

        callWithExpectedCount(client, 5, "TM2_GT_LT", "xin", 3, 6000000000L);
        callWithExpectedCount(client, 8, "TM2_GET_LT", "xin", 3, 6000000000L);
        callWithExpectedCount(client, 5, "TM2_GT_LET", "xin", 3, 6000000000L);
        callWithExpectedCount(client, 8, "TM2_GET_LET", "xin", 3, 6000000000L);

        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xxx' AND POINTS > 1 AND POINTS <= 6", 0);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS < 1000", 10);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS <= 1000", 10);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS < 1", 0);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS <= 2", 2);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS < 3", 2);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS <= 3", 5);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS < 4", 5);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS <= 4", 5);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS <= 8", 10);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS >= -1 AND POINTS <= 6", 8);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS >= -100 AND POINTS <= 1200", 10);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS >= 2 AND POINTS <= 6", 7);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS >= 2 AND POINTS < 6", 5);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS > 2 AND POINTS <= 6", 6);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS > 2 AND POINTS < 6", 4);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS > 3 AND POINTS <= 6", 3);
        callAdHocFilterWithExpectedCount(client,"TM2", "UNAME = 'xin' AND POINTS > 3 AND POINTS < 6", 1);
    }

    void testENG4959Float() throws Exception {
        Client client = getClient();

        client.callProcedure("TU5.insert", 1, 0.1);
        client.callProcedure("TU5.insert", 1, 0.2);
        client.callProcedure("TU5.insert", 1, 0.3);
        client.callProcedure("TU5.insert", 1, 0.4);
        client.callProcedure("TU5.insert", 1, 0.5);
        client.callProcedure("TU5.insert", 2, 0.1);
        client.callProcedure("TU5.insert", 2, 0.2);
        client.callProcedure("TU5.insert", 2, 0.3);
        client.callProcedure("TU5.insert", 2, 0.4);
        client.callProcedure("TU5.insert", 2, 0.5);

        client.callProcedure("TU5.insert", 1, null);
        client.callProcedure("TU5.insert", 2, null);

        callAdHocFilterWithExpectedCount(client,"TU5", "ID = 1 AND POINTS >= 0.1", 5);
        callAdHocFilterWithExpectedCount(client,"TU5", "ID = 1 AND POINTS > 0.2", 3);
        callAdHocFilterWithExpectedCount(client,"TU5", "ID = 1 AND POINTS > 0.5", 0);
        callAdHocFilterWithExpectedCount(client,"TU5", "ID = 2 AND POINTS > 0.3", 2);
        callAdHocFilterWithExpectedCount(client,"TU5", "ID = 2 AND POINTS > 0.5", 0);
    }

    public void testENG6131CountDistinctConstant() throws Exception {
        Client client = getClient();
        client.callProcedure("TU3.insert", 1, 1, 123);
        client.callProcedure("TU3.insert", 2, 2, 123);
        client.callProcedure("TU3.insert", 3, 3, 123);
        client.callProcedure("TU3.insert", 4, 6, 123);
        client.callProcedure("TU3.insert", 5, 8, 123);
        client.callProcedure("TU3.insert", 6, 1, 456);
        client.callProcedure("TU3.insert", 7, 2, 456);
        client.callProcedure("TU3.insert", 8, 3, 456);
        client.callProcedure("TU3.insert", 9, 6, 456);
        client.callProcedure("TU3.insert", 10, 8, 456);

        client.callProcedure("TU3.insert", 11, null, 123);
        client.callProcedure("TU3.insert", 12, null, 456);

        callWithExpectedCount(client, 1, "ENG_6131", 123, 2);
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
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestIndexScanCountSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(BatchedMultiPartitionTest.class.getResource("sqlindex-ddl.sql"));

        project.addStmtProcedure("TU1_LT",       "SELECT COUNT(ID) FROM TU1 WHERE POINTS < ?");
        project.addStmtProcedure("TU1_LET",       "SELECT COUNT(ID) FROM TU1 WHERE POINTS <= ?");
        project.addStmtProcedure("TU1_GT",       "SELECT COUNT(ID) FROM TU1 WHERE POINTS > ?");
        project.addStmtProcedure("TU1_GET",       "SELECT COUNT(ID) FROM TU1 WHERE POINTS >= ?");

        project.addStmtProcedure("TU3_LT",       "SELECT COUNT(ID) FROM TU3 WHERE TEL = ? AND POINTS < ?");
        project.addStmtProcedure("TU3_LET",       "SELECT COUNT(ID) FROM TU3 WHERE TEL = ? AND POINTS <= ?");
        project.addStmtProcedure("TU3_GT_LT",       "SELECT COUNT(ID) FROM TU3 WHERE TEL = ? AND POINTS > ? AND POINTS < ?");
        project.addStmtProcedure("TU3_GT_LET",       "SELECT COUNT(ID) FROM TU3 WHERE TEL = ? AND POINTS > ? AND POINTS <= ?");
        project.addStmtProcedure("TU3_GET_LT",       "SELECT COUNT(ID) FROM TU3 WHERE TEL = ? AND POINTS >= ? AND POINTS < ?");
        project.addStmtProcedure("TU3_GET_LET",       "SELECT COUNT(ID) FROM TU3 WHERE TEL = ? AND POINTS >= ? AND POINTS <= ?");

        project.addStmtProcedure("TM1_LT",       "SELECT COUNT(ID) FROM TM1 WHERE POINTS < ?");
        project.addStmtProcedure("TM1_LET",       "SELECT COUNT(ID) FROM TM1 WHERE POINTS <= ?");
        project.addStmtProcedure("TM1_GT",       "SELECT COUNT(ID) FROM TM1 WHERE POINTS > ?");
        project.addStmtProcedure("TM1_GET",       "SELECT COUNT(ID) FROM TM1 WHERE POINTS >= ?");

        project.addStmtProcedure("TM2_LT",       "SELECT COUNT(ID) FROM TM2 WHERE UNAME = ? AND POINTS < ?");
        project.addStmtProcedure("TM2_LET",       "SELECT COUNT(ID) FROM TM2 WHERE UNAME = ? AND POINTS <= ?");
        project.addStmtProcedure("TM2_GT_LT",       "SELECT COUNT(ID) FROM TM2 WHERE UNAME = ? AND POINTS > ? AND POINTS < ?");
        project.addStmtProcedure("TM2_GT_LET",       "SELECT COUNT(ID) FROM TM2 WHERE UNAME = ? AND POINTS > ? AND POINTS <= ?");
        project.addStmtProcedure("TM2_GET_LT",       "SELECT COUNT(ID) FROM TM2 WHERE UNAME = ? AND POINTS >= ? AND POINTS < ?");
        project.addStmtProcedure("TM2_GET_LET",       "SELECT COUNT(ID) FROM TM2 WHERE UNAME = ? AND POINTS >= ? AND POINTS <= ?");

        project.addStmtProcedure("ENG_6131", "SELECT COUNT(DISTINCT 1) FROM TU3 WHERE TEL = ? AND POINTS > ? ");
        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("sqlIndex-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        config = new LocalCluster("sqlIndex-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("sql-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestIndexCountSuite.class);
    }
}
