/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;


public class TestPlansGroupByComplexMaterializedViewSuite extends RegressionSuite {

    public static String longStr = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ;

    private void compareMVcontentsOfLongs(Client client, String mvTable, long[][] expected, String orderbyStmt) {
        VoltTable vt = null;
        try {
            vt = client.callProcedure("@AdHoc",
                    "select * from " + mvTable + " ORDER BY " + orderbyStmt).getResults()[0];
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        if (vt.getRowCount() != 0) {
            validateTableOfLongs(vt, expected);
        } else {
            // null result
            try {
                vt = client.callProcedure("@AdHoc",
                        "select count(*) from " + mvTable).getResults()[0];
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Nice method to get single row from a VoltTable.
            assertEquals(0, vt.asScalarLong());
        }
    }

    public void testMVInsertDeleteR1() throws IOException, ProcCallException {
        System.out.println("Test R1 insert and delete...");
        String mvTable = "V_R1";
        String orderbyStmt = mvTable+"_G1";

        // ID, wage, dept
        // SELECT ABS(dept), count(*), SUM(wage) FROM R1 GROUP BY ABS(dept)
        Client client = this.getClient();

        compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);

        client.callProcedure("R1.insert", 1,  10,  -1 );
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 1, 10}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add", new long [][]{{-1, 1, 11}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply", new long [][]{{-1, 1, 10}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine", new long [][]{{1, 1, 11}}, orderbyStmt);


        client.callProcedure("R1.insert", 2,  20,  1 );
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 2, 30}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 1, 22}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10}, {1, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine", new long [][]{{1, 2, 33}}, orderbyStmt);

        client.callProcedure("R1.insert", 4,  40,  2 );
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 2, 30}, {2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 1, 22}, {2, 1, 44}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 1, 40}, {2, 1, 160}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 2, 33}, {2, 1, 44}}, orderbyStmt);

        client.callProcedure("R1.insert", 3,  30,  1 );
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 3, 60}, {2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 2, 55}, {2, 1, 44}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 2, 130}, {2, 1, 160}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 3, 66}, {2, 1, 44}}, orderbyStmt);

        client.callProcedure("R1.insert", 5,  50,  2 );
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 3, 60}, {2, 2, 90}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 2, 55}, {2, 2, 99}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 2, 130}, {2, 2, 410}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 3, 66}, {2, 2, 99}}, orderbyStmt);

        // Start to delete
        client.callProcedure("R1.delete", 5);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 3, 60}, {2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 2, 55}, {2, 1, 44}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 2, 130}, {2, 1, 160}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 3, 66}, {2, 1, 44}}, orderbyStmt);

        client.callProcedure("R1.delete", 3);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 2, 30}, {2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 2, 30}, {2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 1, 22}, {2, 1, 44}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 1, 40}, {2, 1, 160}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 2, 33}, {2, 1, 44}}, orderbyStmt);

        client.callProcedure("R1.delete", 1);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 1, 20}, {2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{1, 1, 22}, {2, 1, 44}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{1, 1, 40}, {2, 1, 160}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 1, 22}, {2, 1, 44}}, orderbyStmt);

        client.callProcedure("R1.delete", 2);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{2, 1, 40}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{2, 1, 44}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{2, 1, 160}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{2, 1, 44}}, orderbyStmt);

        client.callProcedure("R1.delete", 4);
        compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);
    }

    public void testMVUpdateR1() throws IOException, ProcCallException {
        System.out.println("Test R1 update...");

        Client client = this.getClient();
        String tb = "R1.insert";
        client.callProcedure(tb, 1,  10,  -1 );
        client.callProcedure(tb, 2,  20,  1 );
        client.callProcedure(tb, 3,  30,  1 );
        client.callProcedure(tb, 4,  40,  2 );
        client.callProcedure(tb, 5,  50,  2 );

        String mvTable = "V_R1";
        String orderbyStmt = mvTable+"_G1";
        // ID, wage, dept
        // SELECT ABS(dept), count(*), SUM(wage) FROM R1 GROUP BY ABS(dept)

        // Check the current contents in MVs
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 3, 60}, {2, 2, 90}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 2, 55}, {2, 2, 99}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 2, 130}, {2, 2, 410}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 3, 66}, {2, 2, 99}}, orderbyStmt);

        // Test update
        client.callProcedure("R1.update", 2, 19, 1, 2);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 3, 59}, {2, 2, 90}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 1, 11},{1, 2, 54}, {2, 2, 99}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 1, 10},{1, 2, 128}, {2, 2, 410}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 3, 65}, {2, 2, 99}}, orderbyStmt);

        client.callProcedure("R1.update", 4, 41, -1, 4);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 4, 100}, {2, 1, 50}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 2, 56},{1, 2, 54}, {2, 1, 55}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 2, 174},{1, 2, 128}, {2, 1, 250}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 4, 110}, {2, 1, 55}}, orderbyStmt);

        client.callProcedure("R1.update", 5, 55, 1, 5);
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, 5, 155}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add",
                new long [][]{{-1, 2, 56},{1, 3, 114} }, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply",
                new long [][]{{-1, 2, 174},{1, 3, 403}}, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine",
                new long [][]{{1, 5, 170}}, orderbyStmt);

        client.callProcedure("@AdHoc","Delete from R1");
        compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_add", null, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_multiply", null, orderbyStmt);
        compareMVcontentsOfLongs(client, mvTable+"_combine", null, orderbyStmt);

    }


    public void testMVInsertDeleteR2() throws Exception {
        System.out.println("Test R2 insert and delete...");
        String mvTable = "V_R2";
        String orderbyStmt = mvTable+"_G1, " + mvTable + "_G2";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        long time1 = dateFormat.parse("2013-06-01 00:00:00.000").getTime()*1000;
        long time23 = dateFormat.parse("2013-07-01 00:00:00.000").getTime()*1000;
        long time46 = dateFormat.parse("2013-08-01 00:00:00.000").getTime()*1000;
        long time5 = dateFormat.parse("2013-09-01 00:00:00.000").getTime()*1000;


        // ID, wage, dept, tm
        // "AS SELECT truncate(month,tm), dept count(*), SUM(wage) " +
        // "FROM R2 GROUP BY truncate(month,tm), dept;" +
        Client client = this.getClient();

        if (!isHSQL()) {
            compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);

            // Start to insert
            client.callProcedure("R2.insert", 1,  10,  1 , "2013-06-11 02:00:00.123457");
            compareMVcontentsOfLongs(client, mvTable, new long [][]{{time1, 1, 1, 10}}, orderbyStmt);

            client.callProcedure("R2.insert", 2,  20,  1 , "2013-07-12 03:00:00.123457");
            compareMVcontentsOfLongs(client, mvTable, new long [][]{{time1, 1, 1, 10}, {time23, 1, 1, 20}}, orderbyStmt);

            client.callProcedure("R2.insert", 4,  40,  2 , "2013-08-13 04:00:00.123457");
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 1, 20},
                    {time46, 2, 1, 40}}, orderbyStmt);

            client.callProcedure("R2.insert", 3,  30,  1 , "2013-07-14 05:00:00.123457");
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 50},
                    {time46, 2, 1, 40}}, orderbyStmt);

            client.callProcedure("R2.insert", 5,  50,  2 , "2013-09-15 06:00:00.123457");
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 50},
                    {time46, 2, 1, 40},
                    {time5, 2, 1, 50}}, orderbyStmt);

            client.callProcedure("R2.insert", 6,  60,  2 , "2013-08-16 02:00:00.123457");
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 50},
                    {time46, 2, 2, 100},
                    {time5, 2, 1, 50}}, orderbyStmt);

            // Start to delete
            client.callProcedure("R2.delete", 6);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 50},
                    {time46, 2, 1, 40},
                    {time5, 2, 1, 50}}, orderbyStmt);

            client.callProcedure("R2.delete", 5);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 50},
                    {time46, 2, 1, 40}}, orderbyStmt);

            client.callProcedure("R2.delete", 3);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 1, 20},
                    {time46, 2, 1, 40}}, orderbyStmt);

            client.callProcedure("R2.delete", 1);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{ {time23, 1, 1, 20},{time46, 2, 1, 40}}, orderbyStmt);

            client.callProcedure("R2.delete", 2);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{{time46, 2, 1, 40}}, orderbyStmt);

            client.callProcedure("R2.delete", 4);
            compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);
        }
    }

    public void testMVUpdateR2() throws Exception {
        System.out.println("Test R2 update...");

        if (!isHSQL()) {
            Client client = this.getClient();
            client.callProcedure("R2.insert", 1,  10,  1 , "2013-06-11 02:00:00.123457");
            client.callProcedure("R2.insert", 2,  20,  1 , "2013-07-12 03:00:00.123457");
            client.callProcedure("R2.insert", 3,  30,  1 , "2013-07-14 05:00:00.123457");
            client.callProcedure("R2.insert", 4,  40,  2 , "2013-08-13 04:00:00.123457");
            client.callProcedure("R2.insert", 5,  50,  2 , "2013-09-15 06:00:00.123457");
            client.callProcedure("R2.insert", 6,  60,  2 , "2013-08-16 02:00:00.123457");

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            long time1 = dateFormat.parse("2013-06-01 00:00:00.000").getTime()*1000;
            long time23 = dateFormat.parse("2013-07-01 00:00:00.000").getTime()*1000;
            long time46 = dateFormat.parse("2013-08-01 00:00:00.000").getTime()*1000;
            long time5 = dateFormat.parse("2013-09-01 00:00:00.000").getTime()*1000;

            String mvTable = "V_R2";
            String orderbyStmt = mvTable+"_G1, " + mvTable + "_G2";
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 50},
                    {time46, 2, 2, 100},
                    {time5, 2, 1, 50}}, orderbyStmt);

            // ID, wage, dept, tm
            // "AS SELECT truncate(month,tm), dept count(*), SUM(wage) " +
            // "FROM R2 GROUP BY truncate(month,tm), dept;" +

            client.callProcedure("R2.update", 2, 19, 1, "2013-07-12 03:00:00.123457", 2);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 49},
                    {time46, 2, 2, 100},
                    {time5, 2, 1, 50}}, orderbyStmt);

            client.callProcedure("R2.update", 4, 41, -1, "2013-08-13 04:00:00.123457", 4);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 1, 10},
                    {time23, 1, 2, 49},
                    {time46, -1, 1, 41},
                    {time46, 2, 1, 60},
                    {time5, 2, 1, 50}}, orderbyStmt);

            client.callProcedure("R2.update", 5, 56, 1, "2013-06-11 02:01:00.123457", 5);
            compareMVcontentsOfLongs(client, mvTable, new long [][]{
                    {time1, 1, 2, 66},
                    {time23, 1, 2, 49},
                    {time46, -1, 1, 41},
                    {time46, 2, 1, 60}}, orderbyStmt);

            client.callProcedure("@AdHoc","Delete from R2");
            compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);
        }
    }

    private void compareTableR3 (Client client, String mvTable, Object[][] expected, String orderbyStmt) {
        VoltTable vt = null;
        try {
            vt = client.callProcedure("@AdHoc",
                    "select * from " + mvTable + " ORDER BY " + orderbyStmt).getResults()[0];
        } catch (Exception e) {
            //fail();
            e.printStackTrace();
        }
//        "CREATE VIEW V_R3_short (V_R3_G1,V_R3_CNT, V_R3_sum_wage) " +
//        "AS SELECT substring(vshort,0,1), count(*), SUM(wage) " +
//        "FROM R3 GROUP BY substring(vshort,0,1);" +

        if (vt.getRowCount() != 0) {
            assertEquals(expected.length, vt.getRowCount());
            for (int i=0; i < expected.length; i++) {
                Object[] row = expected[i];
                assertTrue(vt.advanceRow());
                // substring
                assertEquals((String)row[0], vt.getString(0));
                // count(*)
                assertEquals(((Integer)row[1]).longValue(), vt.getLong(1));
                // sum(wage)
                assertEquals(((Integer)row[2]).longValue(), vt.getLong(2));
            }
        } else {
            // null result
            try {
                vt = client.callProcedure("@AdHoc",
                        "select count(*) from " + mvTable).getResults()[0];
            } catch (Exception e) {
                e.printStackTrace();
            }
            assertEquals(0, vt.asScalarLong());
        }
    }

    private void verifyMVTestR3 (Client client, Object[][] expected13,
            Object[][] expected24, String orderbyStmt) {
        String mvTable = "V_R3";

        compareTableR3(client, mvTable+"_test1", expected13, orderbyStmt);
        compareTableR3(client, mvTable+"_test3", expected13, orderbyStmt);

        compareTableR3(client, mvTable+"_test2", expected24, orderbyStmt);
        compareTableR3(client, mvTable+"_test4", expected24, orderbyStmt);
    }

    public void testMVInsertDeleteR3() throws Exception {
        System.out.println("Test R3 insert and delete...");
        String orderbyStmt = "V_R3_CNT, V_R3_sum_wage";
        Client client = this.getClient();

        if (!isHSQL()) {
         // null result for initial mv tables
            verifyMVTestR3(client, null, null, orderbyStmt);

            client.callProcedure("R3.insert", 1,  10,  "VoltDB", "VoltDB");
            verifyMVTestR3(client, new Object[][] {{"Vo", 1, 10}},
                    new Object[][] {{"VoltDB"+longStr, 1, 10}}, orderbyStmt);

            client.callProcedure("R3.insert", 2,  20,  "IBM", "IBM");
            verifyMVTestR3(client, new Object[][] {{"Vo", 1, 10}, {"IB", 1, 20}},
                    new Object[][] {{"VoltDB"+longStr, 1, 10}, {"IBM"+longStr, 1, 20}},
                    orderbyStmt);

            client.callProcedure("R3.insert", 3,  30,  "VoltDB", "VoltDB");
            verifyMVTestR3(client, new Object[][] {{"IB", 1, 20},{"Vo", 2, 40}},
                    new Object[][] {{"IBM"+longStr, 1, 20}, {"VoltDB"+longStr, 2, 40}},
                    orderbyStmt);

            client.callProcedure("R3.insert", 4,  40,  "Apple", "Apple");
            verifyMVTestR3(client, new Object[][] {{"IB",1,20},{"Ap",1,40},{"Vo",2,40}},
                    new Object[][] {{"IBM"+longStr, 1, 20}, {"Apple"+longStr,1,40},
                    {"VoltDB"+longStr, 2, 40}}, orderbyStmt);

            client.callProcedure("R3.insert", 5,  50,  "IBM", "IBM");
            verifyMVTestR3(client, new Object[][] {{"Ap",1,40},{"Vo",2,40},{"IB",2,70}},
                    new Object[][] {{"Apple"+longStr,1,40}, {"VoltDB"+longStr, 2, 40},
                    {"IBM"+longStr,2,70}}, orderbyStmt);

            // Delete
            client.callProcedure("R3.delete", 5);
            verifyMVTestR3(client, new Object[][] {{"IB",1,20},{"Ap",1,40},{"Vo",2,40}},
                    new Object[][] {{"IBM"+longStr, 1, 20}, {"Apple"+longStr,1,40},
                    {"VoltDB"+longStr, 2, 40}}, orderbyStmt);

            client.callProcedure("R3.delete", 4);
            verifyMVTestR3(client, new Object[][] {{"IB", 1, 20},{"Vo", 2, 40}},
                    new Object[][] {{"IBM"+longStr, 1, 20}, {"VoltDB"+longStr, 2, 40}},
                    orderbyStmt);

            client.callProcedure("R3.delete", 1);
            verifyMVTestR3(client, new Object[][] {{"IB", 1, 20},{"Vo", 1, 30}},
                    new Object[][] {{"IBM"+longStr, 1, 20}, {"VoltDB"+longStr, 1, 30}},
                    orderbyStmt);

            client.callProcedure("R3.delete", 3);
            verifyMVTestR3(client, new Object[][] {{"IB", 1, 20}},
                    new Object[][] {{"IBM"+longStr, 1, 20}},orderbyStmt);

            client.callProcedure("R3.delete", 2);
            verifyMVTestR3(client, null, null, orderbyStmt);
        }
    }


    public void testMVUpdateR3() throws Exception {
        System.out.println("Test R3 update...");

        String orderbyStmt = "V_R3_CNT, V_R3_sum_wage";

        Client client = this.getClient();
        if (!isHSQL()) {
            client.callProcedure("R3.insert", 1,  10,  "VoltDB", "VoltDB");
            client.callProcedure("R3.insert", 2,  20,  "IBM", "IBM");
            client.callProcedure("R3.insert", 3,  30,  "VoltDB", "VoltDB");
            client.callProcedure("R3.insert", 4,  40,  "Apple", "Apple");
            client.callProcedure("R3.insert", 5,  50,  "IBM", "IBM");

            verifyMVTestR3(client, new Object[][] {{"Ap",1,40},{"Vo",2,40},{"IB",2,70}},
                    new Object[][] {{"Apple"+longStr,1,40}, {"VoltDB"+longStr, 2, 40},
                    {"IBM"+longStr,2,70}}, orderbyStmt);

            client.callProcedure("R3.update", 2, 22, "IBM", "IBM", 2);
            verifyMVTestR3(client, new Object[][] {{"Ap",1,40},{"Vo",2,40},{"IB",2,72}},
                    new Object[][] {{"Apple"+longStr,1,40}, {"VoltDB"+longStr, 2, 40},
                    {"IBM"+longStr,2,72}}, orderbyStmt);

            client.callProcedure("R3.update", 1, 10, "IBM", "IBM", 1);
            verifyMVTestR3(client, new Object[][] {{"Vo",1,30},{"Ap",1,40},{"IB",3,82}},
                    new Object[][] {{"VoltDB"+longStr,1,30},{"Apple"+longStr,1,40},
                    {"IBM"+longStr,3,82}}, orderbyStmt);

            client.callProcedure("R3.update", 4, 40, "VoltDB", "VoltDB", 4);
            verifyMVTestR3(client, new Object[][] {{"Vo",2,70}, {"IB",3,82}},
                    new Object[][] {{"VoltDB"+longStr,2,70},{"IBM"+longStr,3,82}},
                    orderbyStmt);
        }
    }

    public void testMaterializedViewUpdateR4() throws IOException, ProcCallException {
        System.out.println("Test R4 update...");

        ClientResponse result;
        Client client = this.getClient();
        String insert = "R4.insert";
        // ID, wage, dept, age, rent
        client.callProcedure(insert, 1,  10, -1, 21, 5);
        client.callProcedure(insert, 2,  20,  1, 22, 6);
        client.callProcedure(insert, 3,  30,  1, 23, 7 );
        client.callProcedure(insert, 4,  40,  2, 24, 8);
        client.callProcedure(insert, 5,  50,  2, 25, 9);

        String mvTable = "V2_R4";
        String orderbyStmt = mvTable+"_G1";
        // SELECT dept*dept, dept+dept, count(*), SUM(wage) FROM R4 GROUP BY dept*dept, dept+dept

        // Check the current contents in MVs
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, -2, 1, 10}, {1, 2, 2, 50}, {4, 4, 2, 90}}, orderbyStmt);

        // Test update(id, wage, dept, age, rent, oldid)
        result = client.callProcedure("R4.update", 2, 19, 1, 22, 6, 2);
        assertEquals(ClientResponse.SUCCESS, result.getStatus());
        assertEquals(1, result.getResults()[0].asScalarLong());
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, -2, 1, 10}, {1, 2, 2, 49}, {4, 4, 2, 90}}, orderbyStmt);

        result = client.callProcedure("R4.update", 4, 41, -1, 24, 8, 4);
        assertEquals(ClientResponse.SUCCESS, result.getStatus());
        assertEquals(1, result.getResults()[0].asScalarLong());
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, -2, 2, 51}, {1, 2, 2, 49}, {4, 4, 1, 50}}, orderbyStmt);

        result = client.callProcedure("R4.update", 5, 55, 1, 25, 9, 5);
        assertEquals(ClientResponse.SUCCESS, result.getStatus());
        assertEquals(1, result.getResults()[0].asScalarLong());
        compareMVcontentsOfLongs(client, mvTable, new long [][]{{1, -2, 2, 51}, {1, 2, 3, 104}}, orderbyStmt);

        result = client.callProcedure("@AdHoc","Delete from R4");
        assertEquals(ClientResponse.SUCCESS, result.getStatus());
        assertEquals(5, result.getResults()[0].asScalarLong());
        compareMVcontentsOfLongs(client, mvTable, null, orderbyStmt);
    }

    private void loadTableForMVFixSuite() throws Exception {
        Client client = this.getClient();
        String[] tbs = {"P1", "P2", "R4"};

        for (String tb: tbs) {
            // empty the table first.
            client.callProcedure("@AdHoc", "Delete from " + tb);

            // id, wage, dept, age, rent.
            String insert = tb + ".insert";
            client.callProcedure(insert, 1,  10,  1 , 21, 5);
            client.callProcedure(insert, 2,  20,  2 , 22, 6);
            client.callProcedure(insert, 3,  20,  2 , 23, 7 );
            client.callProcedure(insert, 4,  30,  2 , 24, 8);
            client.callProcedure(insert, 5,  10,  1 , 25, 9);
            client.callProcedure(insert, 6,  30,  3 , 26, 10);
            client.callProcedure(insert, 7,  10,  1 , 27, 11);
            client.callProcedure(insert, 8,  10,  1 , 28, 12);
            client.callProcedure(insert, 9,  30,  3 , 29, 13);
            client.callProcedure(insert, 10, 30,  3 , 30, 14);
        }
        /*
         * View Definition:
         * "CREATE VIEW V_P1 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) " +
         * "AS SELECT wage, dept, count(*), sum(age), sum(rent)  FROM P1 " +
         * "GROUP BY wage, dept;"
         *
         * Current expected data:
         * V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent
         * 10,   1,     4,     101,       37
         * 20,   2,     2,     45,        13
         * 30,   2,     1,     24,        8
         * 30,   3,     3,     85,        37
         * */
    }

    public void testMVBasedP1_NoAgg() throws Exception {
        System.out.println("Test MV partition no agg query...");
        VoltTable vt = null;
        Client client = this.getClient();

        vt = client.callProcedure("@AdHoc", "Select count(*) from V_P1").getResults()[0];
        assertEquals(0, vt.asScalarLong());
        // Load data
        loadTableForMVFixSuite();

        String[] tbs = {"V_P1", "V_P1_ABS", "V_P2", "V_R4"};
        for (String tb: tbs) {
            // (1) Select *
            vt = client.callProcedure("@AdHoc", "Select * from " + tb +
                    " ORDER BY V_G1, V_G2").getResults()[0];
            System.out.println(vt);
            validateTableOfLongs(vt, new long[][]{{ 10, 1, 4, 101, 37},
                    {20, 2, 2, 45, 13}, {30, 2, 1, 24, 8}, {30, 3, 3, 85, 37}});

            // (2) select one of group by keys
            vt = client.callProcedure("@AdHoc", "Select V_G1 from " + tb +
                    " ORDER BY V_G1").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10}, {20}, {30}, {30}});

            vt = client.callProcedure("@AdHoc", "Select distinct V_G1 from " + tb +
                    " ORDER BY V_G1").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10}, {20}, {30} });

            vt = client.callProcedure("@AdHoc", "Select V_G1/2 from " + tb +
                    " ORDER BY V_G1").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{5}, {10}, {15}, {15}});

            // (3) select group by keys
            vt = client.callProcedure("@AdHoc", "Select V_G1, V_G2 from " + tb +
                    " ORDER BY V_G1, V_G2").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 2}, {30, 2}, {30, 3}});

            vt = client.callProcedure("@AdHoc", "Select V_G1 + 1, V_G2 from " + tb +
                    " ORDER BY V_G1, V_G2").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{11, 1}, {21, 2}, {31, 2}, {31, 3}});

            // (4) select non-group by keys
            vt = client.callProcedure("@AdHoc", "Select V_sum_age from " + tb +
                    " ORDER BY V_sum_age").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{24}, {45}, {85}, {101}});

            vt = client.callProcedure("@AdHoc", "Select abs(V_sum_rent - V_sum_age) from " + tb +
                    " ORDER BY V_sum_age").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{16}, {32}, {48}, {64}});
        }
    }

    public void testMVBasedP1_Agg() throws Exception {
        System.out.println("Test MV partition agg query...");
        VoltTable vt = null;
        Client client = this.getClient();
        vt = client.callProcedure("@AdHoc", "Select count(*) from V_P1").getResults()[0];
        assertEquals(0, vt.asScalarLong());
        // Load data
        loadTableForMVFixSuite();
        String[] tbs = {"V_P1", "V_P1_ABS", "V_P2", "V_R4"};
        /*
        * Current expected data:
        * V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent
        * 10,   1,     4,     101,       37
        * 20,   2,     2,     45,        13
        * 30,   2,     1,     24,        8
        * 30,   3,     3,     85,        37
        * */
        for (String tb: tbs) {
            // (1) Table aggregation case.
            // group by keys aggregated.
            vt = client.callProcedure("@AdHoc", "Select count(*) from "
                    + tb).getResults()[0];
            assertEquals(4, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select sum(V_G1) from "
                    + tb).getResults()[0];
            assertEquals(90, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select sum(V_G1) / 2 from "
                    + tb).getResults()[0];
            assertEquals(45, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select sum(distinct V_G1) from "
                    + tb).getResults()[0];
            assertEquals(60, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select MAX(V_G1), MAX(v_sum_age), " +
                    "MIN(v_sum_rent) from " + tb).getResults()[0];
            validateTableOfLongs(vt, new long[][]{{30, 101, 8}});

            // non-group by keys aggregated.
            vt = client.callProcedure("@AdHoc", "Select sum(V_sum_rent) from "
                    + tb).getResults()[0];
            assertEquals(95, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select max(V_sum_rent) from "
                    + tb).getResults()[0];
            assertEquals(37, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select min(V_sum_rent) from "
                    + tb).getResults()[0];
            assertEquals(8, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select count(V_sum_rent) from "
                    + tb).getResults()[0];
            assertEquals(4, vt.asScalarLong());

            vt = client.callProcedure("@AdHoc", "Select avg(V_sum_rent) from "
                    + tb).getResults()[0];
            assertEquals(23, vt.asScalarLong());

            if (!isHSQL()) {
                vt = client.callProcedure("@AdHoc", "Select sum(V_sum_age) + 5 from "
                        + tb).getResults()[0];
                assertEquals(260, vt.asScalarLong());
            }

            vt = client.callProcedure("@AdHoc", "Select avg(V_sum_age) from "
                    + tb).getResults()[0];
            assertEquals(63, vt.asScalarLong());


            // (2) Aggregation with group by.
            // Group by group by keys
            vt = client.callProcedure("@AdHoc", "Select V_G1, sum(V_CNT), max(v_sum_age), min(v_sum_rent) " +
                    "from " + tb + " group by V_G1 " +
                    "order by V_G1").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10, 4, 101, 37},{20, 2, 45, 13}, {30, 4, 85, 8}});

            vt = client.callProcedure("@AdHoc", "Select V_G1, V_G2, sum(V_CNT), min(v_sum_age) " +
                    "from " + tb + " group by V_G1, V_G2 " +
                    "order by V_G1, V_G2").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10,1, 4, 101},{20, 2, 2, 45}, {30, 2, 1, 24}, {30, 3, 3, 85}});

            // Group by non-group by keys.
            vt = client.callProcedure("@AdHoc", "Select V_sum_rent, sum(V_CNT), max(v_sum_age) " +
                    "from " + tb + " group by V_sum_rent order by V_sum_rent").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{8, 1, 24},{13,2, 45}, {37, 7, 101}});

            // Group by mixed.
            vt = client.callProcedure("@AdHoc", "Select V_G1, V_sum_rent, sum(V_CNT), max(v_sum_age) " +
                    "from " + tb + " group by V_G1, V_sum_rent " +
                    "order by V_G1, V_sum_rent").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10, 37, 4, 101},{20, 13, 2, 45},
                    {30, 8, 1, 24}, {30, 37, 3, 85}});

            // (3) complex group by with complex aggregation.
            vt = client.callProcedure("@AdHoc", "Select V_G1/V_G2, sum(V_CNT), min(v_sum_rent) " +
                    "from " + tb + " group by V_G1/V_G2 " +
                    "order by V_G1/V_G2").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10, 9, 13}, {15, 1, 8}});

            vt = client.callProcedure("@AdHoc", "Select V_G1, sum(V_sum_age)/sum(V_CNT), " +
                    "sum(V_sum_rent)/sum(V_CNT), sum(V_sum_age/V_sum_rent) + 1, min(v_sum_age) " +
                    "from " + tb + " group by V_G1 " +
                    "order by V_G1").getResults()[0];
            validateTableOfLongs(vt, new long[][]{{10, 25, 9, 3, 101}, {20, 22, 6, 4, 45}, {30, 27, 11, 6, 24} });
        }
    }

    //
    // Suite builder boilerplate
    //

    public TestPlansGroupByComplexMaterializedViewSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception {
        LocalCluster config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestPlansGroupByComplexMaterializedViewSuite.class);
        String literalSchema = null;
        boolean success = true;
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        String captured = null;
        String[] lines = null;


        VoltProjectBuilder project0 = new VoltProjectBuilder();
        project0.setCompilerDebugPrintStream(capturing);
        literalSchema =
                "CREATE TABLE F ( " +
                "F_PKEY INTEGER NOT NULL, " +
                "F_D1   INTEGER NOT NULL, " +
                "F_D2   INTEGER NOT NULL, " +
                "F_D3   INTEGER NOT NULL, " +
                "F_VAL1 INTEGER NOT NULL, " +
                "F_VAL2 INTEGER NOT NULL, " +
                "F_VAL3 INTEGER NOT NULL, " +
                "PRIMARY KEY (F_PKEY) ); " +

                "CREATE VIEW V0 (V_D1_PKEY, V_D2_PKEY, V_D3_PKEY, V_F_PKEY, CNT, SUM_V1, SUM_V2, SUM_V3) " +
                "AS SELECT F_D1, F_D2, F_D3, F_PKEY, COUNT(*), SUM(F_VAL1)+1, SUM(F_VAL2), SUM(F_VAL3) " +
                "FROM F  GROUP BY F_D1, F_D2, F_D3, F_PKEY;"
                ;
        try {
            project0.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project0);
        assertFalse(success);
        captured = capturer.toString("UTF-8");
        lines = captured.split("\n");

        assertTrue(foundLineMatching(lines,
                ".*V0.*Expressions with aggregate functions are not currently supported in views.*"));

        VoltProjectBuilder project1 = new VoltProjectBuilder();
        project1.setCompilerDebugPrintStream(capturing);
        literalSchema =
                "CREATE TABLE F ( " +
                "F_PKEY INTEGER NOT NULL, " +
                "F_D1   INTEGER NOT NULL, " +
                "F_D2   INTEGER NOT NULL, " +
                "F_D3   INTEGER NOT NULL, " +
                "F_VAL1 INTEGER NOT NULL, " +
                "F_VAL2 INTEGER NOT NULL, " +
                "F_VAL3 INTEGER NOT NULL, " +
                "PRIMARY KEY (F_PKEY) ); " +

                "CREATE VIEW V1 (V_D1_PKEY, V_D2_PKEY, V_D3_PKEY, V_F_PKEY, CNT, SUM_V1, SUM_V2, SUM_V3) " +
                "AS SELECT F_D1, F_D2, F_D3, F_PKEY, COUNT(*) + 1, SUM(F_VAL1), SUM(F_VAL2), SUM(F_VAL3) " +
                "FROM F  GROUP BY F_D1, F_D2, F_D3, F_PKEY;"
                ;
        try {
            project1.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project1);
        assertFalse(success);
        captured = capturer.toString("UTF-8");
        lines = captured.split("\n");

        // Real config for tests
        VoltProjectBuilder project = new VoltProjectBuilder();
        literalSchema =
                "CREATE TABLE R1 ( " +
                "id INTEGER DEFAULT '0' NOT NULL, " +
                "wage INTEGER, " +
                "dept INTEGER, " +
                "PRIMARY KEY (id) );" +

                "CREATE VIEW V_R1 (V_R1_G1, V_R1_CNT, V_R1_sum_wage) " +
                "AS SELECT ABS(dept), count(*), SUM(wage) FROM R1 GROUP BY ABS(dept);" +

                "CREATE VIEW V_R1_add (V_R1_G1, V_R1_CNT, V_R1_sum_wage) " +
                "AS SELECT dept, count(*), SUM(wage+id) FROM R1 GROUP BY dept;" +

                "CREATE VIEW V_R1_multiply (V_R1_G1, V_R1_CNT, V_R1_sum_wage) " +
                "AS SELECT dept, count(*), SUM(wage*id) FROM R1 GROUP BY dept;" +

                "CREATE VIEW V_R1_combine (V_R1_G1, V_R1_CNT, V_R1_sum_wage) " +
                "AS SELECT ABS(dept), count(*), SUM(wage+id) FROM R1 GROUP BY ABS(dept);" +

                "CREATE TABLE R2 ( " +
                "id INTEGER DEFAULT '0' NOT NULL, " +
                "wage INTEGER, " +
                "dept INTEGER, " +
                "tm TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +

                "CREATE VIEW V_R2 (V_R2_G1, V_R2_G2, V_R2_CNT, V_R2_sum_wage) " +
                "AS SELECT truncate(month,tm), dept, count(*), SUM(wage) " +
                "FROM R2 GROUP BY truncate(month,tm), dept;" +

                // R3 mv tests are mainly for memory concerns
                "CREATE TABLE R3 ( " +
                "id INTEGER DEFAULT '0' NOT NULL, " +
                "wage INTEGER, " +
                "vshort VARCHAR(20)," +
                "vlong VARCHAR(200)," +
                "PRIMARY KEY (ID) );" +

                "CREATE VIEW V_R3_test1 (V_R3_G1,V_R3_CNT, V_R3_sum_wage) " +
                "AS SELECT substring(vshort,0,2), count(*), SUM(wage) " +
                "FROM R3 GROUP BY substring(vshort,0,2) ;" +

                "CREATE VIEW V_R3_test2 (V_R3_G1,V_R3_CNT, V_R3_sum_wage) " +
                "AS SELECT vshort || '" + longStr + "', " +
                "count(*), SUM(wage) " +
                "FROM R3 GROUP BY vshort || '" + longStr + "';" +

                "CREATE VIEW V_R3_test3 (V_R3_G1,V_R3_CNT, V_R3_sum_wage) " +
                "AS SELECT substring(vlong,0,2), count(*), SUM(wage) " +
                "FROM R3 GROUP BY substring(vlong,0,2);" +

                "CREATE VIEW V_R3_test4 (V_R3_G1,V_R3_CNT, V_R3_sum_wage) " +
                "AS SELECT vlong || '" + longStr + "', " +
                "count(*), SUM(wage) " +
                "FROM R3 GROUP BY vlong || '" + longStr + "';" +


                "CREATE TABLE P1 ( " +
                "id INTEGER DEFAULT '0' NOT NULL, " +
                "wage INTEGER, " +
                "dept INTEGER, " +
                "age INTEGER,  " +
                "rent INTEGER, " +
                "PRIMARY KEY (id) );" +
                "PARTITION TABLE P1 ON COLUMN id;" +

                "CREATE VIEW V_P1 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) " +
                "AS SELECT wage, dept, count(*), sum(age), sum(rent)  FROM P1 " +
                "GROUP BY wage, dept;" +

                "CREATE VIEW V_P1_ABS (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) " +
                "AS SELECT abs(wage), dept, count(*), sum(age), sum(rent)  FROM P1 " +
                "GROUP BY abs(wage), dept;" +

                // NO Need to fix mv query
                "CREATE TABLE P2 ( " +
                "id INTEGER DEFAULT '0' NOT NULL, " +
                "wage INTEGER, " +
                "dept INTEGER NOT NULL, " +
                "age INTEGER,  " +
                "rent INTEGER, " +
                "PRIMARY KEY (id) );" +
                "PARTITION TABLE P2 ON COLUMN dept;" +

                "CREATE VIEW V_P2 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) " +
                "AS SELECT wage, dept, count(*), sum(age), sum(rent)  FROM P2 " +
                "GROUP BY wage, dept;" +

                // NO Need to fix mv query
                "CREATE TABLE R4 ( " +
                "id INTEGER DEFAULT '0' NOT NULL, " +
                "wage BIGINT, " +
                "dept BIGINT, " +
                "age INTEGER,  " +
                "rent INTEGER, " +
                "PRIMARY KEY (id) );" +

                "CREATE VIEW V_R4 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) " +
                "AS SELECT wage, dept, count(*), sum(age), sum(rent)  FROM R4 " +
                "GROUP BY wage, dept;" +

                // This R4 mv tests bigint math result type
                "CREATE VIEW V2_R4 (V2_R4_G1, V2_R4_G2, V2_R4_CNT, V2_R4_sum_wage) " +
                "AS SELECT dept*dept, dept+dept, count(*), SUM(wage) " +
                "FROM R4 GROUP BY dept*dept, dept+dept;" +
                "";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        //* Single-server configuration  -- please do not remove or corrupt this structured comment
        config = new LocalCluster("plansgroupby-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // End single-server configuration  -- please do not remove or corrupt this structured comment */

        //* HSQL backend server configuration  -- please do not remove or corrupt this structured comment
        config = new LocalCluster("plansgroupby-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // End HSQL backend server configuration  -- please do not remove or corrupt this structured comment */

        //* Multi-server configuration  -- please do not remove or corrupt this structured comment
        config = new LocalCluster("plansgroupby-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        // Disable hasLocalServer -- with hasLocalServer enabled,
        // multi-server pro configs mysteriously hang at startup under eclipse.
        config.setHasLocalServer(false);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
        // End multi-server configuration  -- please do not remove or corrupt this structured comment */

        return builder;
    }

    static private boolean foundLineMatching(String[] lines, String pattern) {
        String contents = "";
        for (String string : lines) {
            contents += string;
        }
        if (contents.matches(pattern)) {
            return true;
        }
        return false;
    }
}
