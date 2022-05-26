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

public class TestUnionSuite extends RegressionSuite {
    public TestUnionSuite(String name) {
        super(name);
    }

    /**
     * Three table Union - A.PKEY, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnion() throws NoConnectionsException, IOException, ProcCallException {
        final Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("InsertA", 0, 1); // In the final result set - 0
        client.callProcedure("InsertB", 1, 1); // In the final result set - 1
        client.callProcedure("InsertB", 2, 1); // Eliminated (duplicate)
        client.callProcedure("InsertC", 1, 2); // In the final result set - 2
        client.callProcedure("InsertC", 2, 3); // In the final result set - 3
        client.callProcedure("InsertC", 3, 3); // Eliminated (duplicate)
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION SELECT I FROM B UNION SELECT I FROM C order by pkey;")
                .getResults()[0];
        assertEquals(4, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,2,3});

        vt = client.callProcedure("@AdHoc",
                "(SELECT PKEY FROM A UNION SELECT I FROM B) UNION SELECT I FROM C order by pkey;")
                .getResults()[0];
        assertEquals(4, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,2,3});

        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION (SELECT I FROM B UNION SELECT I FROM C) order by pkey;")
                .getResults()[0];
        assertEquals(4, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,2,3});

        // test with parameters
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A where PKEY = 0 UNION SELECT I FROM B where PKEY = 2 "
                + "UNION SELECT I FROM C WHERE I = 3 order by pkey;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,3});

        vt = client.callProcedure("@Explain",
                "SELECT PKEY FROM A where PKEY = 0 UNION SELECT I FROM B "
                + "UNION SELECT I FROM C WHERE I = 3;").getResults()[0];
        String resultStr = vt.toString();
        assertTrue(resultStr.contains("(PKEY = ?0)"));
        assertTrue(resultStr.contains("(column#1 = ?1)"));

        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A where PKEY = 0 UNION SELECT I FROM B WHERE PKEY=? "
                + "UNION SELECT I FROM C WHERE PKEY = ? AND I = 3 order by pkey;", 3, 2)
                .getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,3});

        String sql;
        // data
        client.callProcedure("@AdHoc", "INSERT INTO RPT_P (client_id, config_id, cost) VALUES (140,1,1.0);");
        client.callProcedure("@AdHoc", "INSERT INTO RPT_P (client_id, config_id, cost) VALUES (140,3,3.0);");
        client.callProcedure("@AdHoc", "INSERT INTO rpt_copy_p (client_id, config_id, cost) VALUES (140,2,2.0);");
        client.callProcedure("@AdHoc", "INSERT INTO rpt_copy_p (client_id, config_id, cost) VALUES (140,1,1.0);");

        sql = "select client_id, config_id from RPT_P where client_id=140 UNION " +
              "select client_id, config_id from rpt_copy_p where client_id=140 " +
              " order by client_id, config_id;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfLongs(vt, new long[][]{{140,1},{140,2},{140,3}});

        sql = "select client_id, config_id, sum(cost) as cost from RPT_P where client_id=140 group by client_id, config_id " +
              " UNION " +
              "select client_id, config_id, sum(cost) as cost from rpt_copy_p where client_id=140 group by client_id, config_id " +
              " order by client_id, config_id;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfLongs(vt, new long[][]{{140,1,1},{140,2,2},{140,3,3}});

        vt = client.callProcedure("testunion_p", 140, 140).getResults()[0];
        assertEquals(3, vt.getRowCount());

        vt = client.callProcedure("testunion_p", 10, 10).getResults()[0];
        assertEquals(0, vt.getRowCount());
    }

    /**
     * Three table Union ALL - A.PKEY, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionAll() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //In the final result set
        client.callProcedure("InsertB", 2, 1); //In the final result set
        client.callProcedure("InsertC", 1, 2); //In the final result set
        client.callProcedure("InsertC", 2, 3); //In the final result set
        vt = client.callProcedure("@AdHoc", "SELECT PKEY FROM A UNION ALL SELECT I FROM B "
                + "UNION ALL SELECT I FROM C order by pkey;")
                .getResults()[0];
        assertEquals(5, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1,2,3});

        vt = client.callProcedure("@AdHoc", "(SELECT PKEY FROM A UNION ALL SELECT I FROM B) "
                + "UNION ALL SELECT I FROM C order by pkey;")
                .getResults()[0];
        assertEquals(5, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1,2,3});

        vt = client.callProcedure("@AdHoc", "SELECT PKEY FROM A UNION ALL "
                + "(SELECT I FROM B UNION ALL SELECT I FROM C) order by pkey;")
                .getResults()[0];
        assertEquals(5, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1,2,3});
    }

    /**
     * Two table Union - A.PKEY, A.I and B.PKET, B.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionMultiColumns() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 2, 1); //In the final result set
        vt = client.callProcedure("@AdHoc", "SELECT PKEY, I FROM A "
                + "UNION SELECT PKEY, I FROM B order by pkey, I;").getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfLongs(vt, new long[][]{{0,1},{1,1},{2,1}});
    }

    /**
     * Two table Union ALL - A.PKEY, A.I and B.PKET, B.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionAllMultiColumns() throws NoConnectionsException, IOException, ProcCallException {
        final Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //In the final result set
        client.callProcedure("InsertB", 2, 1); //In the final result set
        vt = client.callProcedure("@AdHoc", "SELECT PKEY, I FROM A UNION ALL "
                + "SELECT PKEY, I FROM B order by pkey, i;").getResults()[0];
        assertEquals(4, vt.getRowCount());
        validateTableOfLongs(vt, new long[][]{{0,1},{1,1},{1,1},{2,1}});
    }

    /**
     * Two table Union - A.* and B.*
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionStar() throws NoConnectionsException, IOException, ProcCallException {
        final Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 2, 1); //In the final result set
        vt = client.callProcedure("@AdHoc", "( SELECT * FROM A UNION SELECT * FROM B ) ORDER BY PKEY ;").getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfLongs(vt, new long[][]{{0,1},{1,1},{2,1}});
    }

    /**
     * Three table Except - C.I, A.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testExcept1() throws NoConnectionsException, IOException, ProcCallException {
        final Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("InsertA", 0, 1); //Eliminated (both in C and A)
        client.callProcedure("InsertA", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertA", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertA", 3, 4); //Eliminated (not in C)
        client.callProcedure("InsertC", 1, 1); //Eliminated (both in C and A)
        client.callProcedure("InsertC", 2, 2); //IN (not in A)
        vt = client.callProcedure("@AdHoc", "SELECT I FROM C EXCEPT SELECT I FROM A;")
                .getResults()[0];
        assertEquals(1, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{2});
    }

    /**
     * Three table Except - A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testExcept2() throws NoConnectionsException, IOException, ProcCallException {
        final Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 1); //Eliminated (by C.PKEY=1)
        client.callProcedure("InsertA", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertA", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertA", 3, 4); //In the final result set
        client.callProcedure("InsertB", 1, 2); //Eliminated (not in A)
        client.callProcedure("InsertC", 1, 1); //Eliminated (by A.PKEY=0)
        client.callProcedure("InsertC", 2, 2); //Eliminated (not in A)
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A EXCEPT SELECT I FROM B EXCEPT SELECT I FROM C;")
                .getResults()[0];
        assertEquals(1, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{4});

        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A EXCEPT SELECT I FROM B) EXCEPT SELECT I FROM C;")
                .getResults()[0];
        assertEquals(1, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{4});
    }

    /**
     * Three table Except ALL - A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testExceptAll1() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 0); //In the final result set
        client.callProcedure("InsertA", 1, 0); //In the final result set
        client.callProcedure("InsertA", 2, 1); //Eliminated (by B.PKEY=1)
        client.callProcedure("InsertA", 3, 2); //Eliminated (by B.PKEY=2)
        client.callProcedure("InsertA", 4, 2); //Eliminated (by B.PKEY=3)
        client.callProcedure("InsertA", 5, 5); //Eliminated (by B.PKEY=5)
        client.callProcedure("InsertA", 6, 5); //Eliminated (by C.PKEY=1)
        client.callProcedure("InsertA", 7, 5); //In the final result set
        client.callProcedure("InsertB", 1, 1); //Eliminated (by A.PKEY=2)
        client.callProcedure("InsertB", 2, 2); //Eliminated (by A.PKEY=3)
        client.callProcedure("InsertB", 3, 2); //Eliminated (by A.PKEY=4)
        client.callProcedure("InsertB", 4, 3); //Eliminated (not in A)
        client.callProcedure("InsertB", 5, 5); //Eliminated (by A.PKEY=5)
        client.callProcedure("InsertC", 0, 2); //Eliminated (not in (A-B))
        client.callProcedure("InsertC", 1, 5); //Eliminated (by A.PKEY=6)
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A EXCEPT ALL SELECT I FROM B EXCEPT ALL SELECT I FROM C order by i;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,0,5});

        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A EXCEPT ALL SELECT I FROM B) EXCEPT ALL SELECT I FROM C order by I;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,0,5});
    }

    /**
     * Three table Except ALL - B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testExceptAll2() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertB", 1, 1); //Eliminated (not in C)
        client.callProcedure("InsertB", 2, 2); //Eliminated (both in C and B)
        client.callProcedure("InsertB", 3, 2); //Eliminated (C has only 1)
        client.callProcedure("InsertB", 4, 3); //Eliminated (not in C)
        client.callProcedure("InsertB", 5, 5); //Eliminated (both in C and B)
        client.callProcedure("InsertC", 0, 2); //Eliminated (both in C and B)
        client.callProcedure("InsertC", 1, 5); //Eliminated (both in C and B)
        VoltTable result = client.callProcedure("@AdHoc",
                "SELECT I FROM C EXCEPT ALL SELECT I FROM B;").getResults()[0];
        assertEquals(0, result.getRowCount());
    }

    /**
     * Three table Intersect - A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testIntersect() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 0); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertA", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 1, 0); //Eliminated (duplicate)
        client.callProcedure("InsertB", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 3, 2); //Eliminated (not in A)
        client.callProcedure("InsertC", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertC", 2, 2); //Eliminated (not in A)
        client.callProcedure("InsertC", 3, 0); //Eliminated (duplicate)
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A INTERSECT SELECT I FROM B INTERSECT SELECT I FROM C order by i;")
                .getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1});

        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A INTERSECT SELECT I FROM B) INTERSECT SELECT I FROM C order by i;")
                .getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1});

        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A INTERSECT (SELECT I FROM B INTERSECT SELECT I FROM C) order by i;")
                .getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1});
    }

    /**
     * Three table Intersect ALL- A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testIntersectAll() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 0); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertA", 2, 1); //In the final result set
        client.callProcedure("InsertA", 3, 3); //Eliminated (not in B & C)
        client.callProcedure("InsertA", 4, 3); //Eliminated (not in B & C)
        client.callProcedure("InsertA", 5, 2); //Eliminated (not in B)
        client.callProcedure("InsertB", 0, 1); //Eliminated (same as A.PKEY=1)
        client.callProcedure("InsertB", 1, 1); //Eliminated (same as A.PKEY=2)
        client.callProcedure("InsertB", 2, 1); //Eliminated (not in A & C)
        client.callProcedure("InsertB", 3, 0); //Eliminated (same as A.PKEY=0)
        client.callProcedure("InsertC", 0, 1); //Eliminated (same as A.PKEY=1)
        client.callProcedure("InsertC", 1, 1); //Eliminated (same as A.PKEY=2)
        client.callProcedure("InsertC", 2, 2); //Eliminated (not in B)
        client.callProcedure("InsertC", 3, 0); //Eliminated (same as A.PKEY=0)
        client.callProcedure("InsertC", 4, 0); //Eliminated (A & B have only one 0)
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A INTERSECT ALL SELECT I FROM B INTERSECT ALL SELECT I FROM C order by i;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1});

        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A INTERSECT ALL SELECT I FROM B) INTERSECT ALL SELECT I FROM C order by i;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1});

        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A INTERSECT ALL (SELECT I FROM B INTERSECT ALL SELECT I FROM C) order by i;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1});
    }

    /**
     * (A.I union B.I) except C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultipleSetOperations1() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 0); // in A,B union. Eliminated by C.PKEY=3
        client.callProcedure("InsertA", 1, 1); // in A,B union. Eliminated by C.PKEY=1
        client.callProcedure("InsertA", 2, 1); // Eliminated (duplicate in A,B union)
        client.callProcedure("InsertA", 3, 2); // in A,B union. Not in C. In final result set
        client.callProcedure("InsertB", 1, 0); // Eliminated (duplicate A.PKEY=0)
        client.callProcedure("InsertB", 2, 1); // Eliminated (duplicate A.PKEY=1)
        client.callProcedure("InsertB", 3, 2); // Eliminated (duplicate A.PKEY=3)
        client.callProcedure("InsertC", 1, 1); // Eliminated ( in A,B union)
        client.callProcedure("InsertC", 3, 0); // Eliminated ( in A,B union)
        client.callProcedure("InsertC", 4, 3); // Eliminated ( not in A or B)
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A UNION SELECT I FROM B EXCEPT SELECT I FROM C order by i;").getResults()[0];
        assertEquals(1, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{2});

        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A UNION SELECT I FROM B) EXCEPT SELECT I FROM C order by i;").getResults()[0];
        assertEquals(1, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{2});

        // test with parameters
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A where I = 0 UNION SELECT I FROM B EXCEPT SELECT I FROM C WHERE I = 3 order by i;")
                .getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,2});
    }

    /**
     * (A.I union B.I) except (C.I union D.I)
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultipleSetOperations2() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 1, 1); // in A,B and C,D unions. Eliminated by EXCEPT
        client.callProcedure("InsertA", 3, 4); // in A,B union. Not in C,D. In final result set
        client.callProcedure("InsertB", 1, 0); // in A,B and C,D unions. Eliminated by EXCEPT
        client.callProcedure("InsertB", 3, 2); // in A,B and C,D unions. Eliminated by EXCEPT
        client.callProcedure("InsertC", 1, 1); // in A,B and C,D unions. Eliminated by EXCEPT
        client.callProcedure("InsertC", 3, 0); // in A,B and C,D unions. Eliminated by EXCEPT
        client.callProcedure("InsertC", 4, 3); // only in C,D union. Eliminated by EXCEPT
        client.callProcedure("InsertD", 0, 2); // in A,B and C,D unions. Eliminated by EXCEPT
        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A UNION SELECT I FROM B) EXCEPT (SELECT I FROM C UNION SELECT I FROM D);")
                .getResults()[0];
        assertEquals(1, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{4});
    }

    /**
     * A.I intersect all (B.I except all C.I)
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultipleSetOperations3() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;
        client.callProcedure("InsertA", 0, 0); // in A but not in B-C. Eliminated by final INTERSECT
        client.callProcedure("InsertA", 1, 1); // in A but not in B-C. Eliminated by final INTERSECT
        client.callProcedure("InsertA", 2, 1); // in A but not in B-C. Eliminated by final INTERSECT
        client.callProcedure("InsertA", 3, 2); // in A and in B-C. In final result set
        client.callProcedure("InsertA", 4, 2); // in A and in B-C. In final result set
        client.callProcedure("InsertA", 5, 2); // in A but not in B-C. Eliminated by final INTERSECT
        client.callProcedure("InsertB", 1, 0); // in B and C. Eliminated by B-C
        client.callProcedure("InsertB", 2, 1); // in B and C. Eliminated by B-C
        client.callProcedure("InsertB", 3, 2); // in B-C and in A. In final result set
        client.callProcedure("InsertB", 4, 2); // in B-C and in A. In final result set
        client.callProcedure("InsertC", 1, 1); // in B and C. Eliminated by B-C
        client.callProcedure("InsertC", 3, 0); // in B and C. Eliminated by B-C
        client.callProcedure("InsertC", 4, 3); // not in B. Eliminated by B-C
        vt = client.callProcedure("@AdHoc",
                "SELECT I FROM A INTERSECT ALL (SELECT I FROM B EXCEPT ALL SELECT I FROM C);").getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{2,2});
    }

    /**
     * (A.I) except (B.I except C.I)
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultipleSetOperations4() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 0); // in A and B and C. Eliminated by inner EXCEPT, so IN final result.
        client.callProcedure("InsertA", 1, 1); // in A and B, not in C. Eliminated by outer EXCEPT.
        client.callProcedure("InsertA", 2, 2); // in A and has no effect in C. IN final result set.
        client.callProcedure("InsertA", 3, 3); // in A only. IN final result set
        client.callProcedure("InsertB", 0, 0); // in A and B and C. Eliminated by inner EXCEPT, so IN final result.
        client.callProcedure("InsertB", 1, 1); // in A and B, not in C. Eliminated by outer EXCEPT.
        client.callProcedure("InsertB", 4, 4); // Not in A. Has no effect in B and C. Not in final result.
        client.callProcedure("InsertB", 5, 5); // Not in A. Has no effect in B. Not in final result.
        client.callProcedure("InsertC", 0, 0); // in A and B and C. Eliminated by inner EXCEPT, so IN final result.
        client.callProcedure("InsertC", 2, 2); // in A and has no effect in C. IN final result set.
        client.callProcedure("InsertC", 4, 4); // Not in A. Has no effect in B and C. Not in final result.
        client.callProcedure("InsertC", 6, 6); // Not in A. Has no effect in C. Not in final result.
        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A) EXCEPT (SELECT I FROM B EXCEPT SELECT I FROM C) order by i;").getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,2,3});
    }

    /**
     * (A.I) except (B.I except C.I) except (D.I)
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultipleSetOperations5()
    throws NoConnectionsException, IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 0); // in A and B and C, not in D. Eliminated by inner EXCEPT, so IN final result.
        client.callProcedure("InsertA", 1, 1); // in A and B, not in C and D. Eliminated by the first outer EXCEPT.
        client.callProcedure("InsertA", 2, 2); // in A and has no effect in C and not in D. IN final result set.
        client.callProcedure("InsertA", 3, 3); // in A and D. Eliminated by the second outer EXCEPT
        client.callProcedure("InsertB", 0, 0); // in A and B and C, not in D. Eliminated by inner EXCEPT, so IN final result.
        client.callProcedure("InsertB", 1, 1); // in A and B, not in C and D. Eliminated by the first outer EXCEPT.
        client.callProcedure("InsertB", 4, 4); // Not in A. Has no effect in B and C. Not in final result.
        client.callProcedure("InsertB", 5, 5); // Not in A. Has no effect in B. Not in final result.
        client.callProcedure("InsertC", 0, 0); // in A and B and C, not in D. Eliminated by inner EXCEPT, so IN final result.
        client.callProcedure("InsertC", 2, 2); // in A and has no effect in C and D. IN final result set.
        client.callProcedure("InsertC", 4, 4); // Not in A. Has no effect in B and C. Not in final result.
        client.callProcedure("InsertC", 6, 6); // Not in A. Has no effect in C. Not in final result.
        client.callProcedure("InsertD", 1, 3); // in A and D only. Eliminated by the second outer EXCEPT.
        vt = client.callProcedure("@AdHoc",
                "(SELECT I FROM A) EXCEPT (SELECT I FROM B EXCEPT SELECT I FROM C) EXCEPT SELECT I FROM D order by i;")
                .getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,2});
    }

    public void testStoredProcUnionWithParams()
    throws NoConnectionsException, IOException, ProcCallException {
        // Test that parameterized query with union can be invoked.
        Client client = getClient();

        client.callProcedure("InsertB", 2, 2);
        client.callProcedure("InsertC", 3, 3);
        client.callProcedure("InsertD", 4, 4);
        VoltTable vt;
        vt = client.callProcedure("UnionBCD", 2, "XYZ", 4).getResults()[0];
        assertEquals(3, vt.getRowCount());

        vt = client.callProcedure("UnionBCD", 4, "ABC", 2).getResults()[0];
        assertEquals(1, vt.getRowCount());
    }

    /**
     * Three table Union ALL - A.PKEY, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionOrderLimitOffset() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable vt;

        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //In the final result set
        client.callProcedure("InsertB", 2, 1); //In the final result set
        client.callProcedure("InsertC", 1, 2); //In the final result set
        client.callProcedure("InsertC", 2, 3); //In the final result set

        // No limit, offset
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                + "UNION ALL SELECT I FROM C WHERE PKEY > 0 order by pkey;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0,1,1,2,3});

        // Order by column
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                + "UNION ALL SELECT I FROM C WHERE PKEY > 0 ORDER BY PKEY DESC;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3,2,1,1,0});

        // order by number
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                + "UNION ALL SELECT I FROM C WHERE PKEY > 0 ORDER BY 1 DESC;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3,2,1,1,0});

        // order by parameter
        try {
            client.callProcedure("@AdHoc",
                    "SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                    + "UNION ALL SELECT I FROM C WHERE PKEY > 0 ORDER BY ? DESC;", 1);
            fail();
        } catch(Exception ex) {
            final String msg = ex.getMessage();
            if (USING_CALCITE) {
                assertTrue(msg.contains("invalid ORDER BY expression") ||
                        msg.contains("class org.apache.calcite.sql.SqlDynamicParam: "));
            } else {
                assertTrue(msg.contains("invalid ORDER BY expression"));
            }
        }

        // Make sure the query is parameterized
        vt = client.callProcedure("@Explain",
                "SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                + "UNION ALL SELECT I FROM C WHERE PKEY > 0 LIMIT 2 OFFSET 2;").getResults()[0];
        String explainPlan = vt.toString();
        assertTrue(explainPlan.contains("LIMIT with parameter"));
        assertTrue(explainPlan.contains("uniquely match (PKEY = ?0)"));
        assertTrue(explainPlan.contains("filter by (column#1 = ?1)"));
        assertTrue(explainPlan.contains("range-scan covering from (PKEY > ?2)"));


        vt = client.callProcedure("@AdHoc",
                "SELECT ABS(PKEY) as AP FROM A WHERE PKEY = 0 UNION ALL "
                + "SELECT I FROM B WHERE I = 1 UNION ALL SELECT I FROM C WHERE PKEY > 0 ORDER BY AP DESC;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3,2,1,1,0});


        vt = client.callProcedure("@AdHoc",
                "SELECT cast ((PKEY+1) as INTEGER) as AP FROM A WHERE PKEY = 0 UNION ALL "
                + "SELECT I FROM B WHERE I = 1 UNION ALL SELECT I FROM C WHERE PKEY > 0 ORDER BY AP DESC;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{3,2,1,1,1});

        //
        // with ORDER BY
        //

        // limit 3, no offset
        vt = client.callProcedure("@AdHoc",
                "(SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                + "UNION ALL SELECT I FROM C WHERE PKEY > 0) order by pkey LIMIT 3;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0,1,1});

        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL SELECT I FROM C order by pkey LIMIT ?;", 3)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{0,1,1});

        // limit 2, offset 2
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                + " UNION ALL SELECT I FROM C WHERE PKEY > 0 ORDER BY PKEY LIMIT 2 OFFSET 2;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1,2});

        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL "
                + "SELECT I FROM C order by pkey LIMIT ? OFFSET ?;", 2, 2).getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{1,2});

        vt = client.callProcedure("@AdHoc",
                "(SELECT PKEY FROM A UNION ALL SELECT I FROM B order by pkey LIMIT 1) "
                + "UNION ALL SELECT I FROM C order by pkey;").getResults()[0];
        assertEquals(3, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,2,3});

        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION ALL (SELECT I FROM B UNION ALL "
                + "SELECT I FROM C order by I LIMIT 1) order by pkey;").getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1});

        vt = client.callProcedure("@AdHoc",
                "(SELECT PKEY FROM A UNION ALL SELECT I FROM B ORDER BY PKEY) UNION ALL "
                + "SELECT I FROM C order by pkey;").getResults()[0];
        assertEquals(5, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1,2,3});

        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION ALL (SELECT I FROM B UNION ALL "
                + "SELECT I FROM C ORDER BY I) order by pkey;").getResults()[0];
        assertEquals(5, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{0,1,1,2,3});

        //
        vt = client.callProcedure("@AdHoc",
                "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL "
                + "SELECT I FROM C ORDER BY PKEY LIMIT 2 OFFSET 3;").getResults()[0];
        assertEquals(2, vt.getRowCount());
        validateTableOfScalarLongs(vt, new long[]{2,3});

        // without ORDER BY
        // hsqldb bug ENG-8382: hsqldb does not apply the LIMIT, returning wrong answers
        if (!isHSQL()) {
            // limit 3, no offset
            vt = client.callProcedure("@AdHoc",
                    "(SELECT PKEY FROM A WHERE PKEY = 0 UNION ALL SELECT I FROM B WHERE I = 1 "
                    + "UNION ALL SELECT I FROM C WHERE PKEY > 0) LIMIT 3;").getResults()[0];
            assertEquals(3, vt.getRowCount());

            // parameter
            vt = client.callProcedure("@AdHoc",
                    "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL SELECT I FROM C LIMIT ?;", 3)
                    .getResults()[0];
            assertEquals(3, vt.getRowCount());

            // parameter
            vt = client.callProcedure("@AdHoc",
                    "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL "
                    + "SELECT I FROM C LIMIT ? OFFSET ?;", 2, 2).getResults()[0];
            assertEquals(2, vt.getRowCount());

            // hsqdldb bug ENG-8381: without LIMIT, the OFFSET has NPE in hsqldb backend
            vt = client.callProcedure("@AdHoc",
                    "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL "
                    + "SELECT I FROM C ORDER BY PKEY OFFSET 3;").getResults()[0];
            assertEquals(2, vt.getRowCount());
            validateTableOfScalarLongs(vt, new long[]{2,3});
        }
    }

    public void testUnionVarchar() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();

        String state = "MA";
        String hex = "10";
        client.callProcedure("MY_VOTES.insert", 1,
                state, state, state, state, state, state,
                state, state, state, state, state, state,
                hex, hex, hex, hex, hex, "11");
        client.callProcedure("AREA_CODE_STATE.insert", 1803, "RI");
        client.callProcedure("AREA_CODE_STATE.insert", 1804, "RI");

        String[] columns = new String[]{"state2", "state15", "state16", "state63", "state64", "state100",
                "state2_b", "state15_b", "state16_b", "state63_b", "state64_b", "state100_b"};

        for (String col : columns) {
            validateTableColumnOfScalarVarchar(client,
                    "select "+ col +" from my_votes union select 'MA' from area_code_state;",
                    new String[] {"MA"});

            validateTableColumnOfScalarVarchar(client,
                    "select "+ col +
                            " from my_votes union select 'VOLTDB_VOLTDB_VOLTDB_VOLTDB_VOLTDB' from area_code_state order by 1;",
                    new String[] {"MA", "VOLTDB_VOLTDB_VOLTDB_VOLTDB_VOLTDB"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" from my_votes except select 'MA' from area_code_state;",
                  new String[] {});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" from my_votes union all select 'MA' from area_code_state;",
                  new String[] {"MA","MA", "MA"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" from my_votes union select state from area_code_state order by 1;",
                  new String[] {"MA","RI"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" || '_USA' from my_votes union select state from area_code_state order by 1;",
                  new String[] {"MA_USA","RI"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" from my_votes union select state || '_USA' from area_code_state order by 1;",
                  new String[] {"MA","RI_USA"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" || '_USA' from my_votes union select state || '_USA' from area_code_state order by 1;",
                  new String[] {"MA_USA","RI_USA"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" || '_USA' from my_votes union select state || '_USA' from area_code_state order by 1;",
                  new String[] {"MA_USA","RI_USA"});

            validateTableColumnOfScalarVarchar(client,
                  "select "+ col +" || '_USA' from my_votes union select state100 || '_USA' from my_votes order by 1;",
                  new String[] {"MA_USA"});
        }

        // varbinary
        String[] binaryColumns = new String[]{"binary2", "binary15", "binary16", "binary63", "binary64", "binary100"};

        for (String col : binaryColumns) {
            validateTableColumnOfScalarVarbinary(client,
                    "select "+ col +" from my_votes union select binary100 from my_votes order by 1;",
                    col.equals("binary100") ? new String[] {"11"} : new String[] {"10", "11"});

            validateTableColumnOfScalarVarbinary(client,
                    "select binary100 from my_votes union select "+ col +" from my_votes order by 1;",
                    col.equals("binary100") ? new String[] {"11"} : new String[] {"10", "11"});

            validateTableColumnOfScalarVarbinary(client,
                    "select "+ col +" from my_votes union select "+ col +" from my_votes order by 1;",
                    new String[] { col.equals("binary100") ? "11" : "10"});
        }
    }

    public void testEng12941() throws Exception {
        Client client = getClient();

        assertSuccessfulDML(client, "insert into t0_eng_12941 values ('foo', 10);");
        assertSuccessfulDML(client, "insert into t0_eng_12941 values ('bar', 20);");
        assertSuccessfulDML(client, "insert into t0_eng_12941 values ('baz', 30);");

        assertSuccessfulDML(client, "insert into t1_eng_12941 values ('bar', 40);");

        String SQL = "SELECT * FROM T0_ENG_12941 AS OUTER_TBL WHERE (" +
                "SELECT MIN(V) FROM T1_ENG_12941 AS INNER_TBL WHERE INNER_TBL.STR = OUTER_TBL.STR) IS NOT NULL "
                + "UNION ALL (SELECT TOP 2 * FROM T0_ENG_12941 ORDER BY 2);";

        VoltTable vt = client.callProcedure("@AdHoc", SQL).getResults()[0];
        assertContentOfTable(new Object[][] {{"bar", 20}, {"foo", 10}, {"bar", 20}}, vt);
    }

    public void testEng13536() throws Exception {
        Client client = getClient();
        assertSuccessfulDML(client, "insert into t0_eng_13536 values (0);");
        assertSuccessfulDML(client, "insert into t0_eng_13536 values (1);");
        assertSuccessfulDML(client, "insert into t0_eng_13536 values (2);");
        String SQL = "((select * from t0_eng_13536 order by id limit 1) "
                + "  union all "
                + "  select * from t0_eng_13536) order by id desc;";
        ClientResponse cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        // The first three values are from the right hand side.
        // The last is from the left hand side.  (The 0 could be from either.)
        assertContentOfTable(new Object[][] { {2}, { 1 }, { 0 }, { 0 } }, vt);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestUnionSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestUnionSuite.class.getResource("testunion-ddl.sql"));
        project.addStmtProcedure("InsertA", "INSERT INTO A VALUES(?, ?);");
        project.addStmtProcedure("InsertB", "INSERT INTO B VALUES(?, ?);");
        project.addStmtProcedure("InsertC", "INSERT INTO C VALUES(?, ?);");
        project.addStmtProcedure("InsertD", "INSERT INTO D VALUES(?, ?);");
        // Test that parameterized query with union compiles properly.
        project.addStmtProcedure("UnionBCD",
                "((SELECT I FROM B WHERE PKEY = ?) UNION " +
                "    (SELECT I FROM C WHERE PKEY = CHAR_LENGTH(''||?))) UNION " +
                "        SELECT I FROM D WHERE PKEY = ?");

        // local
        config = new LocalCluster("testunion-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) {
            fail();
        }
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testunion-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testunion-cluster.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
