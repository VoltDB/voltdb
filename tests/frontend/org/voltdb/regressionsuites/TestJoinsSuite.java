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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJoinsSuite extends RegressionSuite {
    public TestJoinsSuite(String name) {
        super(name);
    }

    private void clearSeqTables(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("@AdHoc", "DELETE FROM R1;");
        client.callProcedure("@AdHoc", "DELETE FROM R2;");
        client.callProcedure("@AdHoc", "DELETE FROM P1;");
    }

    public void testSeqJoins()
            throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        clearSeqTables(client);
        subtestTwoTableSeqInnerJoin(client);
        clearSeqTables(client);
        subtestTwoTableSeqInnerWhereJoin(client);
        clearSeqTables(client);
        subtestTwoTableSeqInnerFunctionJoin(client);
        clearSeqTables(client);
        subtestTwoTableSeqInnerMultiJoin(client);
        clearSeqTables(client);
        subtestThreeTableSeqInnerMultiJoin(client);
        clearSeqTables(client);
        subtestSeqOuterJoin(client);
        clearSeqTables(client);
        subtestSelfJoin(client);
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,3
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated
        client.callProcedure("InsertR1", 3, 3, 3); // 3,3,3,4
        client.callProcedure("InsertR2", 1, 3); // 1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // 3,3,3,4
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 USING(A);")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
        client.callProcedure("InsertP1", 1, 1); // 1,1,1,3
        client.callProcedure("InsertP1", 1, 1); // 1,1,1,3
        client.callProcedure("InsertP1", 2, 2); // Eliminated
        client.callProcedure("InsertP1", 3, 3); // 3,3,3,4
        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 ON P1.A = R2.A;")
                .getResults()[0];
        assertEquals(3, result.getRowCount());
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerWhereJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 5, 1); // 1,5,1,1,3
        client.callProcedure("InsertR1", 1, 1, 1); // eliminated by WHERE
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR1", 3, 3, 3); // eliminated by WHERE
        client.callProcedure("InsertR2", 1, 3); // 1,5,1,1,3
        client.callProcedure("InsertR2", 3, 4); // eliminated by WHERE
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 INNER JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1, R2 WHERE R1.A = R2.A AND R1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        client.callProcedure("InsertP1", 1, 5); // 1,5,1,1,3
        client.callProcedure("InsertP1", 1, 1); // eliminated by WHERE
        client.callProcedure("InsertP1", 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertP1", 3, 3); // eliminated by WHERE
        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 ON P1.A = R2.A WHERE P1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerFunctionJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", -1, 5, 1); //  -1,5,1,1,3
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR1", 3, 3, 3); // 3,3,3,3,4

        client.callProcedure("InsertR2", 1, 3); // 1,1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // 3,3,3,3,4
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON ABS(R1.A) = R2.A;")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(3, result.getRowCount());
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableSeqInnerMultiJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,1,1
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR1", 3, 3, 3); // Eliminated by JOIN
        client.callProcedure("InsertR2", 1, 1); // 1,1,1,1,1
        client.callProcedure("InsertR2", 1, 3); // Eliminated by JOIN
        client.callProcedure("InsertR2", 3, 4); // Eliminated by JOIN
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A AND R1.C = R2.C;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1, R2 WHERE R1.A = R2.A AND R1.C = R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 USING (A,C);")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 USING (A,C) WHERE A > 0;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 USING (A,C) WHERE A > 4;")
                .getResults()[0];
        assertEquals(0, result.getRowCount());

        client.callProcedure("InsertP1", 1, 1); // 1,1,1,1,1
        client.callProcedure("InsertP1", 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertP1", 3, 3); // Eliminated by JOIN
        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 USING (A,C);")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 USING (A,C) WHERE A > 0;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 USING (A,C) WHERE A > 4;")
                .getResults()[0];
        assertEquals(0, result.getRowCount());
}

    /**
     * Three table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestThreeTableSeqInnerMultiJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by P1 R1 JOIN
        client.callProcedure("InsertR1", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE

        client.callProcedure("InsertR2", 1, 1); // Eliminated by P1 R2 JOIN
        client.callProcedure("InsertR2", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // Eliminated by P1 R2 JOIN
        client.callProcedure("InsertR2", 4, 0); // Eliminated by WHERE

        client.callProcedure("InsertP1", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("InsertP1", -1, 0); // Eliminated by WHERE
        client.callProcedure("InsertP1", 8, 4); // Eliminated by P1 R1 JOIN
        VoltTable result = client.callProcedure(
                "@AdHoc", "select * FROM P1 JOIN R1 ON P1.A = R1.A JOIN R2 ON P1.C = R2.C WHERE P1.A > 0")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    private void clearIndexTables(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("@AdHoc", "DELETE FROM R1;");
        client.callProcedure("@AdHoc", "DELETE FROM R2;");
        client.callProcedure("@AdHoc", "DELETE FROM R3;");
        client.callProcedure("@AdHoc", "DELETE FROM P2;");
        client.callProcedure("@AdHoc", "DELETE FROM P3;");
    }

    /**
     * Self Join table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestSelfJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 2, 7);
        client.callProcedure("InsertR1", 2, 2, 7);
        client.callProcedure("InsertR1", 4, 3, 2);
        client.callProcedure("InsertR1", 5, 6, null);
        // 2,2,1,1,2,7
        //2,2,1,2,2,7
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 A JOIN R1 B ON A.A = B.C;")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());

        // 1,2,7,NULL,NULL,NULL
        // 2,2,7,4,3,2
        // 4,3,2,NULL,NULL,NULL
        // 5,6,NULL,NULL,NULL,NULL
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 A LEFT JOIN R1 B ON A.A = B.D;")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
    }

    public void testIndexJoins()
            throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        clearIndexTables(client);
        subtestTwoTableIndexInnerJoin(client);
        clearIndexTables(client);
        subtestTwoTableIndexInnerWhereJoin(client);
        clearIndexTables(client);
        subtestThreeTableIndexInnerMultiJoin(client);
        clearIndexTables(client);
        subtestIndexOuterJoin(client);
        clearIndexTables(client);
        subtestDistributedOuterJoin(client);
    }
    /**
     * Two table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableIndexInnerJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR3", 1, 1); // 1,1,1,3
        client.callProcedure("InsertR3", 1, 1); // 1,1,1,3
        client.callProcedure("InsertR3", 2, 2); // Eliminated
        client.callProcedure("InsertR3", 3, 3); // 3,3,3,4
        client.callProcedure("InsertR2", 1, 3); // 1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // 3,3,3,4
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R3 JOIN R2 ON R3.A = R2.A;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());

        client.callProcedure("InsertP3", 1, 1); // 1,1,1,3
        client.callProcedure("InsertP3", 2, 2); // Eliminated
        client.callProcedure("InsertP3", 3, 3); // 3,3,3,4
        result = client.callProcedure("@AdHoc", "SELECT * FROM P3 JOIN R2 ON P3.A = R2.A;")
                .getResults()[0];
        assertEquals(2, result.getRowCount());
    }

    /**
     * Two table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoTableIndexInnerWhereJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR3", 1, 5); // eliminated by WHERE
        client.callProcedure("InsertR3", 1, 1); // eliminated by WHERE
        client.callProcedure("InsertR3", 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR3", 3, 3); // eliminated by WHERE
        client.callProcedure("InsertR3", 4, 5); // 4,5,4,2
        client.callProcedure("InsertR2", 1, 3); // 1,5,1,1,3
        client.callProcedure("InsertR2", 3, 4); // eliminated by WHERE
        client.callProcedure("InsertR2", 4, 2); // 4,5,4,2
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R3 JOIN R2 ON R3.A = R2.A WHERE R3.A > R2.C;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());

        client.callProcedure("InsertP3", 1, 5); // eliminated by WHERE
        client.callProcedure("InsertP3", 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertP3", 3, 3); // eliminated by WHERE
        client.callProcedure("InsertP3", 4, 3); // 4,3,4,2
        result = client.callProcedure("@AdHoc", "SELECT * FROM P3 JOIN R2 ON P3.A = R2.A WHERE P3.A > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    /**
     * Three table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestThreeTableIndexInnerMultiJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by P3 R1 JOIN
        client.callProcedure("InsertR1", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE
        client.callProcedure("InsertR2", 1, 1); // Eliminated by P3 R2 JOIN
        client.callProcedure("InsertR2", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // Eliminated by P3 R2 JOIN
        client.callProcedure("InsertR2", 4, 0); // Eliminated by WHERE
        client.callProcedure("InsertP3", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("InsertP3", -1, 0); // Eliminated by WHERE
        client.callProcedure("InsertP3", 8, 4); // Eliminated by P3 R1 JOIN
        VoltTable result = client.callProcedure(
                "@AdHoc", "select * FROM P3 JOIN R1 ON P3.A = R1.A JOIN R2 ON P3.F = R2.C WHERE P3.A > 0")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    /**
     * Two table left and right NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestSeqOuterJoin(Client client)
              throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", 1, 1, 1);
        client.callProcedure("InsertR1", 1, 2, 1);
        client.callProcedure("InsertR1", 2, 2, 2);
        client.callProcedure("InsertR1", -1, 3, 3);
        // R1 1st joined with R2 null
        // R1 2nd joined with R2 null
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        VoltTable result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        VoltTableRow row = result.fetchRow(2);
        assertEquals(2, row.getLong(1));

        client.callProcedure("InsertR2", 1, 1);
        client.callProcedure("InsertR2", 1, 3);
        client.callProcedure("InsertR2", 3, null);
        // R1 1st joined with R2 1st
        // R1 2nd joined with R2 1st
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 RIGHT JOIN R1 ON R1.A = R2.C")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // Same as above but with partitioned table
        client.callProcedure("InsertP1", 1, 1);
        client.callProcedure("InsertP1", 1, 2);
        client.callProcedure("InsertP1", 2, 2);
        client.callProcedure("InsertP1", -1, 3);
        result = client.callProcedure(
                "@AdHoc", "select * FROM P1 LEFT JOIN R2 ON P1.A = R2.C")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // R1 1st joined with R2 with R2 1st
        // R1 2nd joined with R2 null (failed R1.C = 1)
        // R1 3rd joined with R2 null (failed  R1.A = R2.C)
        // R1 4th3rd joined with R2 null (failed  R1.A = R2.C)
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C AND R1.C = 1")
                                 .getResults()[0];
        assertEquals(4, result.getRowCount());
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 RIGHT JOIN R1 ON R1.A = R2.C AND R1.C = 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        // Same as above but with partitioned table
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 RIGHT JOIN P1 ON P1.A = R2.C AND P1.C = 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // R1 1st joined with R2 null - eliminated by the second join condition
        // R1 2nd joined with R2 null
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C AND R2.A = 100")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // R1 1st - joined with R2 not null and eliminated by the filter condition
        // R1 2nd - joined with R2 not null and eliminated by the filter condition
        // R1 3rd - joined with R2 null
        // R1 4th - joined with R2 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C WHERE R2.A IS NULL")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
        // Same as above but with partitioned table
        result = client.callProcedure(
                "@AdHoc", "select * FROM P1 LEFT JOIN R2 ON P1.A = R2.C WHERE R2.A IS NULL")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());

        // R1 1st - joined with R2 1st row
        // R1 2nd - joined with R2 null eliminated by the filter condition
        // R1 3rd - joined with R2 null eliminated by the filter condition
        // R1 4th - joined with R2 null eliminated by the filter condition
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C WHERE R1.C = 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(1, result.getRowCount());

        // R1 1st - eliminated by the filter condition
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - eliminated by the filter condition
        // R1 3rd - joined with the R2 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C WHERE R1.A = -1")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());
        // Same as above but with partitioned table
        result = client.callProcedure(
                "@AdHoc", "select * FROM P1 LEFT JOIN R2 ON P1.A = R2.C WHERE P1.A = -1")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        System.out.println(result.toString());

        // R1 1st - joined with the R2
        // R1 1st - joined with the R2
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - eliminated by the filter condition
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C WHERE R1.A = 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());

        // R1 1st - eliminated by the filter condition
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - joined with R2 null and pass the filter
        // R1 4th - joined with R2 null and pass the filter
        result = client.callProcedure(
                "@AdHoc", "select * FROM R1 LEFT JOIN R2 ON R1.A = R2.C WHERE R2.A is NULL")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
    }

    /**
     * Two table left and right NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestIndexOuterJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR2", 1, 1);
        client.callProcedure("InsertR2", 2, 2);
        client.callProcedure("InsertR2", 3, 3);
        client.callProcedure("InsertR2", 4, 4);
        // R2 1st joined with R3 null
        // R2 2nd joined with R3 null
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        VoltTable result = client.callProcedure(
                "@AdHoc", "select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A")
                                 .getResults()[0];
        VoltTableRow row = result.fetchRow(2);
        assertEquals(3, row.getLong(1));
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        client.callProcedure("InsertR3", 1, 1);
        client.callProcedure("InsertR3", 2, 2);
        client.callProcedure("InsertR3", 5, 5);

        // R2 1st joined with R3 1st
        // R2 2nd joined with R3 2nd
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        result = client.callProcedure(
                "@AdHoc", "select * FROM R3 RIGHT JOIN R2 ON R3.A = R2.A")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        // Same as above but with partitioned table
        client.callProcedure("InsertP2", 1, 1);
        client.callProcedure("InsertP2", 2, 2);
        client.callProcedure("InsertP2", 3, 3);
        client.callProcedure("InsertP2", 4, 4);
        result = client.callProcedure(
                "@AdHoc", "select * FROM P2 LEFT JOIN R3 ON R3.A = P2.A")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // R2 1st joined with R3 NULL R2.C < 0
        // R2 2nd joined with R3 null R2.C < 0
        // R2 3rd joined with R3 null R2.C < 0
        // R2 4th joined with R3 null R2.C < 0
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A AND R2.C < 0")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        result = client.callProcedure(
                "@AdHoc", "select * FROM R3 RIGHT JOIN R2 ON R3.A = R2.A AND R2.C < 0")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        // Same as above but with partitioned table
        result = client.callProcedure(
                "@AdHoc", "select * FROM P2 LEFT JOIN R3 ON R3.A = P2.A AND P2.E < 0")
                                 .getResults()[0];
        System.out.println(result.toString());


        // R2 1st joined with R3 null (eliminated by  R3.A > 1
        // R2 2nd joined with R3 2nd
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A AND R3.A > 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());
        result = client.callProcedure(
                "@AdHoc", "select * FROM R3 RIGHT JOIN R2 ON R3.A = R2.A AND R3.A > 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // R2 1st joined with R3 1st  but eliminated by  R3.A IS NULL
        // R2 2nd joined with R3 2nd  but eliminated by  R3.A IS NULL
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A WHERE R3.A IS NULL")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
        result = client.callProcedure(
                "@AdHoc", "select * FROM R3 RIGHT JOIN R2 ON R3.A = R2.A WHERE R3.A IS NULL")
                                 .getResults()[0];
        System.out.println(result.toString());
        if ( ! isHSQL()) assertEquals(2, result.getRowCount()); //// PENDING HSQL flaw investigation
        // Same as above but with partitioned table
        result = client.callProcedure(
                "@AdHoc", "select * FROM R3 RIGHT JOIN P2 ON R3.A = P2.A WHERE R3.A IS NULL")
                                 .getResults()[0];
        System.out.println(result.toString());
        if ( ! isHSQL())  assertEquals(2, result.getRowCount()); //// PENDING HSQL flaw investigation

        // R2 1st eliminated by R2.C < 0
        // R2 2nd eliminated by R2.C < 0
        // R2 3rd eliminated by R2.C < 0
        // R2 4th eliminated by R2.C < 0
        result = client.callProcedure(
                "@AdHoc", "select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A WHERE R2.C < 0")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(0, result.getRowCount());
        // Same as above but with partitioned table
        result = client.callProcedure(
                "@AdHoc", "select * FROM P2 LEFT JOIN R3 ON R3.A = P2.A WHERE P2.E < 0")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(0, result.getRowCount());

        // Outer table index scan
        // R3 1st eliminated by R3.A > 0 where filter
        // R3 2nd joined with R3 2
        // R3 3rd joined with R2 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM R3 LEFT JOIN R2 ON R3.A = R2.A WHERE R3.A > 1")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
    }

    /**
     * Two table left and right NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestDistributedOuterJoin(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertP2", 1, 1);
        client.callProcedure("InsertP2", 2, 2);
        client.callProcedure("InsertP2", 3, 3);
        client.callProcedure("InsertP2", 4, 4);
        client.callProcedure("InsertR3", 1, 1);
        client.callProcedure("InsertR3", 2, 2);
        client.callProcedure("InsertR3", 4, 4);
        client.callProcedure("InsertR3", 5, 5);
        // R3 1st joined with P2 not null and eliminated by P2.A IS NULL
        // R3 2nd joined with P2 not null and eliminated by P2.A IS NULL
        // R3 3rd joined with P2 null (P2.A < 3)
        // R3 4th joined with P2 null

        VoltTable result = client.callProcedure(
                "@AdHoc", "select *  FROM P2 RIGHT JOIN R3 ON R3.A = P2.A AND P2.A < 3 WHERE P2.A IS NULL")
                .getResults()[0];
        System.out.println(result.toString());
        if ( ! isHSQL()) assertEquals(2, result.getRowCount()); //// PENDING HSQL flaw investigation

        client.callProcedure("InsertP3", 1, 1);
        client.callProcedure("InsertP3", 2, 2);
        client.callProcedure("InsertP3", 4, 4);
        client.callProcedure("InsertP3", 5, 5);
        // P3 1st joined with P2 not null and eliminated by P2.A IS NULL
        // P3 2nd joined with P2 not null and eliminated by P2.A IS NULL
        // P3 3rd joined with P2 null (P2.A < 3)
        // P3 4th joined with P2 null
        result = client.callProcedure(
                "@AdHoc", "select *  FROM P2 RIGHT JOIN P3 ON P3.A = P2.A AND P2.A < 3 WHERE P2.A IS NULL")
                .getResults()[0];
        System.out.println(result.toString());
        if ( ! isHSQL()) assertEquals(2, result.getRowCount()); //// PENDING HSQL flaw investigation
        // Outer table index scan
        // P3 1st eliminated by P3.A > 0 where filter
        // P3 2nd joined with P2 2
        // P3 3nd joined with P2 4
        // R3 4th joined with P2 null
        result = client.callProcedure(
                "@AdHoc", "select * FROM P3 LEFT JOIN P2 ON P3.A = P2.A WHERE P3.A > 1")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(3, result.getRowCount());
    }

    /**
     * IN LIST JOIN/WHERE Expressions
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testInListJoin()
            throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = this.getClient();
        client.callProcedure("InsertR1", 1, 1, 1);
        client.callProcedure("InsertR1", 2, 2, 2);
        client.callProcedure("InsertR1", 3, 3, 3);
        client.callProcedure("InsertR1", 4, 4, 4);
        client.callProcedure("InsertR3", 1, 1);
        client.callProcedure("InsertR3", 2, 2);
        client.callProcedure("InsertR3", 4, 4);
        client.callProcedure("InsertR3", 5, 5);
        client.callProcedure("InsertR3", 6, 6);

        // Outer join - IN LIST is outer table join index expression
        VoltTable result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 LEFT JOIN R1 on R3.A = R1.A and R3.A in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(5, result.getRowCount());
        // Outer join - IN LIST is outer table join non-index expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 LEFT JOIN R1 on R3.A = R1.A and R3.C in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(5, result.getRowCount());
        // Inner join - IN LIST is join index expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 JOIN R1 on R3.A = R1.A and R3.A in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());

        // Outer join - IN LIST is inner table join index expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R1 LEFT JOIN R3 on R3.A = R1.A and R3.A in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // Outer join - IN LIST is inner table join scan expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R1 LEFT JOIN R3 on R3.A = R1.A and R3.C in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(4, result.getRowCount());

        // Outer join - IN LIST is outer table where index expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 LEFT JOIN R1 on R3.A = R1.A WHERE R3.A in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
        // Outer join - IN LIST is outer table where scan expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 LEFT JOIN R1 on R3.A = R1.A WHERE R3.C in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
        // Inner join - IN LIST is where index expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 JOIN R1 on R3.A = R1.A WHERE R3.A in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
        // Inner join - IN LIST is where scan expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R3 JOIN R1 on R3.A = R1.A WHERE R3.C in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
        // Outer join - IN LIST is inner table where index expression
        result = client.callProcedure(
                "@AdHoc", "select *  FROM R1 LEFT JOIN R3 on R3.A = R1.A WHERE R3.A in (1,2)")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
    }

    /**
     * Multi table outer join
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultiTableOuterJoin() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = this.getClient();
        client.callProcedure("InsertR1", 11, 11, 11);
        client.callProcedure("InsertR1", 12, 12, 12);
        client.callProcedure("InsertR1", 13, 13, 13);
        client.callProcedure("InsertR2", 21, 21);
        client.callProcedure("InsertR2", 22, 22);
        client.callProcedure("InsertR2", 12, 12);
        client.callProcedure("InsertR3", 31, 31);
        client.callProcedure("InsertR3", 32, 32);
        client.callProcedure("InsertR3", 33, 21);
        VoltTable result = client.callProcedure(
                "@AdHoc", "select *  FROM R1 RIGHT JOIN R2 on R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(3, result.getRowCount());

        result = client.callProcedure(
                "@AdHoc", "select *  FROM R1 RIGHT JOIN R2 on R1.A = R2.A LEFT JOIN R3 ON R3.C = R2.C WHERE R1.C > 0")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(1, result.getRowCount());
    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJoinsSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestJoinsSuite.class.getResource("testjoins-ddl.sql"));
        project.addStmtProcedure("InsertR1", "INSERT INTO R1 VALUES(?, ?, ?);");
        project.addStmtProcedure("InsertR2", "INSERT INTO R2 VALUES(?, ?);");
        project.addStmtProcedure("InsertR3", "INSERT INTO R3 VALUES(?, ?);");
        project.addStmtProcedure("InsertP1", "INSERT INTO P1 VALUES(?, ?);");
        project.addStmtProcedure("InsertP2", "INSERT INTO P2 VALUES(?, ?);");
        project.addStmtProcedure("InsertP3", "INSERT INTO P3 VALUES(?, ?);");
        /*
        config = new LocalCluster("testunion-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);
        */
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
