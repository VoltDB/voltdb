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

import junit.framework.Test;

public class TestPartialIndexesSuite extends RegressionSuite {

    private final String [] partitioned_tbs =  {"P1"};
    private final String [] replicated_tbs =  {"R1"};

    private void emptyTable(Client client, String tb) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure("@AdHoc", "delete from " + tb);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestPartialIndexesSuite(String name) {
        super(name);
    }

    public void testPartialUniqueIndex() throws Exception {
        Client client = getClient();

        // CREATE UNIQUE INDEX r1_pidx_1 ON R1 (a) where b is not null;
        // CREATE UNIQUE INDEX r1_pidx_hash_1 ON R1 (c) where b is not null;
        for (String tb : replicated_tbs) {
            emptyTable(client, tb);
            ClientResponse cr =
                    client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, 1, 1, 1, 1);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(2, 2, 2, 2, 2);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                // Fail the r1_pidx_1 index
                client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(2, 3, 3, 3, 4);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            try {
                // Fail the r1_pidx_hash_1 index
                client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(3, 3, 2, 3, 5);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            // fail the r1_pidx_1 predicate
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, NULL, 4, 4, 6);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(2, NULL, 5, 5, 7);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // fail the r1_pidx_hash_1 predicate
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(5, NULL, 1, 4, 8);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(6, NULL, 2, 5, 9);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            VoltTable vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb +
                    " where a > 0 and b > 0 order by a").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1,1, 1}, {2, 2, 2} });
            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb +
                    " where c > 0 and b > 0 order by a").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1,1, 1}, {2, 2, 2} });

            // Old and new tuples pass index predicate r1_pidx_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 4, B = 4 WHERE A = 2 AND B = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 4 WHERE A = 1 AND B = 1;");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            // Old and new tuples pass index predicate r1_pidx_hash_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET C = 4, B = 4 WHERE C = 2 AND B = 4;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                client.callProcedure("@AdHoc","UPDATE " + tb + " SET C = 4 WHERE C = 1 AND B = 1;");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb + " order by a,c").getResults()[0];
            validateTableOfLongs(vt, new long[][] {
                    {1,1,1}, {1,Long.MIN_VALUE,4}, {2,Long.MIN_VALUE,5},
                    {4, 4, 4}, {5,Long.MIN_VALUE,1}, {6,Long.MIN_VALUE,2} });

            // Old tuple fail index predicate r1_pidx_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 5, B = 5, C = 5  WHERE A = 1 AND B IS NULL;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 5, B = 5, C = 5 WHERE A = 2 AND B IS NULL;");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb + " order by a,c").getResults()[0];
            validateTableOfLongs(vt, new long[][] {
                    {1,1,1}, {2,Long.MIN_VALUE,5}, {4, 4, 4},
                    {5,Long.MIN_VALUE,1}, {5, 5, 5}, {6,Long.MIN_VALUE,2} });


            // Old tuple fail index predicate r1_pidx_hash_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 7, C = 7, B = 7  WHERE C = 1 AND B IS NULL;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 7, C = 7, B = 7 WHERE C = 2 AND B IS NULL;");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb + " order by a, c").getResults()[0];
            validateTableOfLongs(vt, new long[][] {
                    {1,1,1}, {2,Long.MIN_VALUE,5}, {4, 4, 4},
                    {5, 5, 5}, {6,Long.MIN_VALUE,2}, {7, 7, 7} });

            // New tuple fail index predicate r1_pidx_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 1, B = NULL  WHERE A = 5 AND B = 5;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 1, B = NULL WHERE A = 4 AND B = 4;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb + " order by a, c").getResults()[0];
            validateTableOfLongs(vt, new long[][] {
                    {1,1,1}, {1,Long.MIN_VALUE,4}, {1,Long.MIN_VALUE,5},
                    {2,Long.MIN_VALUE,5}, {6,Long.MIN_VALUE,2}, {7, 7, 7} });

            // New tuple fail index predicate r1_pidx_hash_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET C = 1, B = NULL  WHERE C = 1 AND B = 1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET C = 1, B = NULL WHERE C = 7 AND B = 7;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb +" order by a,c;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {
                    {1,Long.MIN_VALUE,1}, {1,Long.MIN_VALUE,4}, {1,Long.MIN_VALUE,5},
                    {2,Long.MIN_VALUE,5}, {6,Long.MIN_VALUE,2}, {7, Long.MIN_VALUE, 1} });

            // Old and new tuples fail index predicate r1_pidx_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 6 WHERE A = 2 AND B is NULL;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // Old and new tuples fail index predicate r1_pidx_1
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET C = 5 WHERE C = 4 AND B is NULL;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            vt = client.callProcedure("@AdHoc", "select a, b, c from " + tb +" order by a,c;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {
                    {1,Long.MIN_VALUE,1}, {1,Long.MIN_VALUE,5}, {1,Long.MIN_VALUE,5},
                    {6,Long.MIN_VALUE,2}, {6,Long.MIN_VALUE,5}, {7, Long.MIN_VALUE, 1} });

            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET B = 7  WHERE A = 7;");
            cr = client.callProcedure("@AdHoc","DELETE FROM " + tb + " WHERE A = 7;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, b from " + tb +
                    " where a = 7").getResults()[0];
            validateTableOfLongs(vt, new long[][] { });

            cr = client.callProcedure("@AdHoc","UPSERT INTO " + tb + " VALUES(6,NULL,3,3,10);");
            cr = client.callProcedure("@AdHoc","UPSERT INTO " + tb + " VALUES(6,1,3,3,11);");
            try {
                client.callProcedure("@AdHoc","UPSERT INTO " + tb + " VALUES(6,1,3,3,12);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            String sql = "select a, b, c from " + tb +" where a > 0 and b > 0 order by a,b,c;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {6, 1, 3} });
            // Verify AdHoc plans
            VoltTable explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_1"));

            sql = "select a, b, c from " + tb +" where c = 3 and b > 0 order by a,b,c;";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {6, 1, 3} });
            explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_HASH_1"));

            // The Ad-Hoc parameterized query can use a partial index with "where b is not null"
            // predicate - "b > ?" expression is NULL rejecting
            explain = client.callProcedure("@Explain", "select a, b, c from " + tb +" where c = 3 and b > ? order by a,b,c;", 0).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_HASH_1"));

        }
    }

    public void testPartitionPartialUniqueIndex() throws Exception {
        Client client = getClient();

        // CREATE UNIQUE INDEX p1_pidx_1 ON P1 (a) where b is not null;
        for (String tb : partitioned_tbs) {
            emptyTable(client, tb);
            ClientResponse cr =
                    client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, 1, 1, 1);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(2, 2, 2, 2);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(2, 3, 3, 3);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, NULL, 4, 4);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(2, NULL, 5, 5);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            VoltTable vt = client.callProcedure("@AdHoc", "select a, b from " + tb +
                    " where a > 0 and b > 0 order by a").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1,1}, {2, 2} });

            cr = client.callProcedure("@AdHoc","DELETE FROM " + tb + " WHERE A = 1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            String sql = "select a, b from " + tb + " where a > 0 and b > 0 order by a, b";
            vt = client.callProcedure("@AdHoc",  sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {2,2} });
            // Verify AdHoc plans
            VoltTable explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_1"));

            // The Ad-Hoc parameterized query can use a partial index with "where b is not null"
            // predicate - "b > ?" expression is NULL rejecting
            explain = client.callProcedure("@Explain", "select a, b from " + tb + " where a > 0 and b > ? order by a, b", 0).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_1"));
        }
        // CREATE UNIQUE INDEX p1_pidx_2 ON P1 (a) where a > 4;
        // CREATE UNIQUE INDEX p1_pidx_3 ON P1 (a) where a > c and d > 3;
        for (String tb : partitioned_tbs) {
            emptyTable(client, tb);
            // p1_pidx_3
            ClientResponse cr =
                    client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, NULL, 0, 4);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                // Rejected by p1_pidx_3
                client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, NULL, -1, 4);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }

            // No index
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, NULL, 0, 0);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // p1_pidx_2
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(5, NULL, 10, 0);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                // Rejected by p1_pidx_2
                client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(5, NULL, 4, 0);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }

            // p1_pidx_2 and p1_pidx_3
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(10, NULL, 9, 4);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            try {
                // Rejected by p1_pidx_2 and p1_pidx_3
                client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(10, NULL, 8, 4);");
                fail("Shouldn't reach there");
            } catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Constraint Type UNIQUE"));
            }
            // No Index
            VoltTable vt = client.callProcedure("@AdHoc", "select a, c, d from " + tb + " order by a, c, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 0, 0}, {1, 0, 4}, {5, 10, 0}, {10, 9, 4} });

            // p1_pidx_2
            String sql = "select a, c, d from " + tb + " where a > 4 order by a, c, d";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 10, 0}, {10, 9, 4} });
            // Verify AdHoc plans
            VoltTable explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_2"));

            // p1_pidx_3
            sql = "select a, c, d from " + tb + " where a > 0 and a > c and d > 3 order by a, c, d";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 0, 4}, {10, 9, 4} });
            explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_3"));

            // Ad-Hoc parameterized query can not use the partial index
            explain = client.callProcedure("@Explain", "select a, c, d from " + tb + " where a > 0 and a > c and d > ? order by a, c, d", 3).getResults()[0];
            assertTrue(!explain.toString().contains(tb + "_PIDX_3"));

        }
    }

    public void testPartialIndex() throws Exception {
        Client client = getClient();

        // CREATE INDEX r1_pidx_2 ON R1 (d) where a > 0;
        // CREATE INDEX r1_pidx_hash_2 ON R1 (d) where a < 0;
        for (String tb : replicated_tbs) {
            emptyTable(client, tb);
            ClientResponse cr =
                    client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(1, NULL, 1, 1, 1);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(-2, NULL, 2, 2, 2);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(3, NULL, 3, 1, 5);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("@AdHoc","INSERT INTO " + tb + " VALUES(-4, NULL, 4, 2, 6);");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // r1_pidx_2
            VoltTable vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d > 0 and a > 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1,1 }, {3, 1} });
            // r1_pidx_hash_2
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d = 2 and a < 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-4, 2}, {-2, 2} });

            // Old and new tuples pass index predicate r1_pidx_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 2, D = 2 WHERE A = 1 AND D = 1;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d > 0 and a > 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {2,2 }, {3, 1} });

            // Old and new tuples pass index predicate r1_pidx_hash_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = -5 WHERE A = -4 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d = 2 and a < 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-5, 2}, {-2, 2} });

            // Old tuple fail index predicate r1_pidx_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 3 WHERE A = -5 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d > 0 and a > 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {2,2}, {3, 1}, {3, 2} });

            // Old tuple fail index predicate r1_pidx_hash_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = -5 WHERE A = 3 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d = 2 and a < 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-5, 2}, {-2, 2} });

            // New tuple fail index predicate r1_pidx_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = -6  WHERE A = 2 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d > 0 and a > 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {3, 1}});

            // New tuple fail index predicate r1_pidx_hash_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 3 WHERE A = -6 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d = 2 and a < 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-5, 2}, {-2, 2} });

            // Old and new tuples fail index predicate r1_pidx_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = -6  WHERE A = -5 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d > 0 and a > 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {3, 1}, {3, 2} });

            // Old and new tuples fail index predicate r1_pidx_hash_2
            cr = client.callProcedure("@AdHoc","UPDATE " + tb + " SET A = 4  WHERE A = 3 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d = 2 and a < 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-6, 2}, {-2, 2} });

            // Delete from index r1_pidx_2
            cr = client.callProcedure("@AdHoc","DELETE FROM " + tb + " WHERE A = 4 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d > 0 and a > 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {3, 1} });

            // Delete from index r1_pidx_hash_2
            cr = client.callProcedure("@AdHoc","DELETE FROM " + tb + " WHERE A = -6 AND D = 2;");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = client.callProcedure("@AdHoc", "select a, d from " + tb + " where d = 2 and a < 0 order by a, d").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-2, 2} });

            // r1_pidx_2
            String sql = "select a, c, d from " + tb + " where d > 0 and a > 0 order by a, c, d";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {3, 3, 1} });
            // Verify AdHoc plans
            VoltTable explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_2"));

            // r1_pidx_hash_2
            sql = "select a, c, d from " + tb + " where d = 2 and a < 0 order by a, c, d";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            validateTableOfLongs(vt, new long[][] { {-2, 2, 2} });
            // Verify AdHoc plans
            explain = client.callProcedure("@Explain", sql).getResults()[0];
            assertTrue(explain.toString().contains(tb + "_PIDX_HASH_2"));

            // Ad-Hoc parameterized query can not use the partial index
            explain = client.callProcedure("@Explain", "select a, c, d from " + tb + " where d = 2 and a < ? order by a, c, d", 0).getResults()[0];
            assertTrue(!explain.toString().contains(tb + "_PIDX_HASH_2"));

        }
    }

    public void testPartialIndexPlanCache() throws Exception {
        Client client = getClient();

        //CREATE INDEX r1_pidx_2 ON R1 (d) where a > 0;
        String sql = "select a from r1 where d > 2 and a > 0;";
        VoltTable explain = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(explain.toString().contains("R1_PIDX_2"));
        // Same
        sql = "select a from r1 where d > 3 and a > 0;";
        explain = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(explain.toString().contains("R1_PIDX_2"));

        // Index R1_PIDX_2 is not eligible. Can't use the previously cached plan
        sql = "select a from r1 where d > 2 and a > 1;";
        explain = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(!explain.toString().contains("R1_PIDX_2"));

        // Index R1_PIDX_2 is again eligible.
        sql = "select a from r1 where d > 2 and a > 0;";
        explain = client.callProcedure("@Explain", sql).getResults()[0];
        assertTrue(explain.toString().contains("R1_PIDX_2"));

    }

    /**
     * Build a list of the tests that will be run when TestIndexColumnLess gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestPartialIndexesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestPartialIndexesSuite.class.getResource("testpartialindexes-ddl.sql"));
        project.addStmtProcedure("InsertR1", "INSERT INTO R1 VALUES(?, ?, ?, ?, ?);");

        // local
        config = new LocalCluster("testpartialindexes-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) {
            fail();
        }
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testpartialindexes-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQLDB does not support partial indexes. If it ever does, here's the code to run it.
        //config = new LocalCluster("testpartialindexes-cluster.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        //if (!config.compile(project)) fail();
        //builder.addServerConfig(config);

        return builder;
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(TestPartialIndexesSuite.class);
    }
}
