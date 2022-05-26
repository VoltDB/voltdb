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
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJoinsSuite extends RegressionSuite {
    private static final long NULL_VALUE = Long.MIN_VALUE;

    private static final String[] SEQ_TABLES = new String[] { "R1", "R2", "P1" };

    private static final String[] INDEXED_TABLES = new String[] { "R1", "R2", "R3", "R4", "P2", "P3", "P4" };

    // Operators that should be safe and effective for use as partition key
    // filters to minimally enable partition table joins.
    private static final String[] JOIN_OPS = {"=", "IS NOT DISTINCT FROM"};

    public TestJoinsSuite(String name) {
        super(name);
    }

    public void testSeqJoins() throws Exception {
        Client client = getClient();
        for (String joinOp : JOIN_OPS) {
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerWhereJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerFunctionJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestTwoTableSeqInnerMultiJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestThreeTableSeqInnerMultiJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestSeqOuterJoin(client, joinOp);
        }
        truncateTables(client, SEQ_TABLES);
        subtestSelfJoin(client);
    }

    /**
     * Two table NLJ
     */
    private void subtestTwoTableSeqInnerJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,3
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated
        client.callProcedure("R1.INSERT", 3, 3, 3); // 3,3,3,4

        client.callProcedure("R2.INSERT", 1, 3); // 1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // 3,3,3,4

        String query;

        validateRowCount(client, "SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A;", 3);
        validateRowCount(client, "SELECT * FROM R1 JOIN R2 USING(A);", 3);
        client.callProcedure("P1.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P1.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P1.INSERT", 2, 2); // Eliminated
        client.callProcedure("P1.INSERT", 3, 3); // 3,3,3,4

        validateRowCount(client, "SELECT * FROM P1 JOIN R2 ON P1.A " + joinOp + " R2.A;", 3);

        // Insert some null values to validate the difference between "="
        // and "IS NOT DISTINCT FROM".
        query = "SELECT * FROM P1 JOIN R2 ON P1.C " + joinOp + " R2.C";

        final int BASELINE_COUNT = 1;
        // Validate a baseline result without null join key values.
        validateRowCount(client, query, BASELINE_COUNT);

        client.callProcedure("P1.INSERT", 4, null);
        client.callProcedure("P1.INSERT", 5, null);
        final int LHS_NULL_COUNT = 2;

        // With nulls on just ONE one side, the joinOp makes no difference.
        // The result still matches the baseline.
        validateRowCount(client, query, BASELINE_COUNT);

        // With N nulls on one side and M nulls on the other,
        // expect "=" to continue returning the baseline result while
        // "IS NOT DISTINCT FROM" matches NxM more matches.
        client.callProcedure("R2.INSERT", 6, null);
        client.callProcedure("R2.INSERT", 7, null);
        client.callProcedure("R2.INSERT", 8, null);
        final int RHS_NULL_COUNT = 3;
        validateRowCount(client, query, joinOp.equals("=") ?
                BASELINE_COUNT :
                (BASELINE_COUNT + LHS_NULL_COUNT*RHS_NULL_COUNT));
    }

    /**
     * Two table NLJ
     */
    private void subtestTwoTableSeqInnerWhereJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 5, 1); // 1,5,1,1,3
        client.callProcedure("R1.INSERT", 1, 1, 1); // eliminated by WHERE
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("R1.INSERT", 3, 3, 3); // eliminated by WHERE
        client.callProcedure("R2.INSERT", 1, 3); // 1,5,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // eliminated by WHERE

        validateRowCount(client, "SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C;", 1);
        validateRowCount(client, "SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C;", 1);
        validateRowCount(client, "SELECT * FROM R1 INNER JOIN R2 ON R1.A " + joinOp + " R2.A WHERE R1.C > R2.C;", 1);
        validateRowCount(client, "SELECT * FROM R1, R2 WHERE R1.A " + joinOp + " R2.A AND R1.C > R2.C;", 1);

        client.callProcedure("P1.INSERT", 1, 5); // 1,5,1,1,3
        client.callProcedure("P1.INSERT", 1, 1); // eliminated by WHERE
        client.callProcedure("P1.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("P1.INSERT", 3, 3); // eliminated by WHERE

        validateRowCount(client, "SELECT * FROM P1 JOIN R2 ON P1.A " + joinOp + " R2.A WHERE P1.C > R2.C;", 1);
    }

    /**
     * Two table NLJ
     */
    private void subtestTwoTableSeqInnerFunctionJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", -1, 5, 1); //  -1,5,1,1,3
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("R1.INSERT", 3, 3, 3); // 3,3,3,3,4

        client.callProcedure("R2.INSERT", 1, 3); // 1,1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // 3,3,3,3,4

        validateRowCount(client, "SELECT * FROM R1 JOIN R2 ON ABS(R1.A) " + joinOp + " R2.A;", 3);
    }

    /**
     * Two table NLJ
     */
    private void subtestTwoTableSeqInnerMultiJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,1,1,1,1
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("R1.INSERT", 3, 3, 3); // Eliminated by JOIN
        client.callProcedure("R2.INSERT", 1, 1); // 1,1,1,1,1
        client.callProcedure("R2.INSERT", 1, 3); // Eliminated by JOIN
        client.callProcedure("R2.INSERT", 3, 4); // Eliminated by JOIN

        validateRowCount(client, "SELECT * FROM R1 JOIN R2 ON R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R2.C;", 1);
        validateRowCount(client, "SELECT * FROM R1, R2 WHERE R1.A " + joinOp + " R2.A AND R1.C " + joinOp + " R2.C;", 1);
        validateRowCount(client, "SELECT * FROM R1 JOIN R2 USING (A,C);", 1);
        validateRowCount(client, "SELECT * FROM R1 JOIN R2 USING (A,C) WHERE R1.A > 0;", 1);
        validateRowCount(client, "SELECT * FROM R1 JOIN R2 USING (A,C) WHERE R1.A > 4;", 0);

        client.callProcedure("P1.INSERT", 1, 1); // 1,1,1,1,1
        client.callProcedure("P1.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("P1.INSERT", 3, 3); // Eliminated by JOIN
        validateRowCount(client, "SELECT * FROM P1 JOIN R2 USING (A,C);", 1);
        validateRowCount(client, "SELECT * FROM P1 JOIN R2 USING (A,C) WHERE P1.A > 0;", 1);
        validateRowCount(client, "SELECT * FROM P1 JOIN R2 USING (A,C) WHERE P1.A > 4;", 0);
    }

    /**
     * Three table NLJ
     */
    private void subtestThreeTableSeqInnerMultiJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by P1 R1 JOIN
        client.callProcedure("R1.INSERT", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE

        client.callProcedure("R2.INSERT", 1, 1); // Eliminated by P1 R2 JOIN
        client.callProcedure("R2.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // Eliminated by P1 R2 JOIN
        client.callProcedure("R2.INSERT", 4, 0); // Eliminated by WHERE

        client.callProcedure("P1.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("P1.INSERT", -1, 0); // Eliminated by WHERE
        client.callProcedure("P1.INSERT", 8, 4); // Eliminated by P1 R1 JOIN

        validateRowCount(client,
                "SELECT * FROM P1 JOIN R1 ON P1.A " + joinOp + " R1.A JOIN R2 ON P1.C " + joinOp + " R2.C WHERE P1.A > 0",
                1);
    }

    /**
     * Self Join table NLJ
     */
    private void subtestSelfJoin(Client client) throws Exception {
        client.callProcedure("R1.INSERT", 1, 2, 7);
        client.callProcedure("R1.INSERT", 2, 2, 7);
        client.callProcedure("R1.INSERT", 4, 3, 2);
        client.callProcedure("R1.INSERT", 5, 6, null);

        // 2,2,1,1,2,7
        // 2,2,1,2,2,7
        validateRowCount(client, "SELECT * FROM R1 A JOIN R1 B ON A.A = B.C;", 2);

        // 1,2,7,NULL,NULL,NULL
        // 2,2,7,4,3,2
        // 4,3,2,NULL,NULL,NULL
        // 5,6,NULL,NULL,NULL,NULL
        validateRowCount(client, "SELECT * FROM R1 A LEFT JOIN R1 B ON A.A = B.D;", 4);
    }

    public void testIndexJoins() throws Exception {
        Client client = getClient();
        for (String joinOp : JOIN_OPS) {
            truncateTables(client, INDEXED_TABLES);
            subtestTwoTableIndexInnerJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestTwoTableIndexInnerWhereJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestThreeTableIndexInnerMultiJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestIndexOuterJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestDistributedOuterJoin(client, joinOp);
        }
    }
    /**
     * Two table NLIJ
     */
    private void subtestTwoTableIndexInnerJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R2.INSERT", 1, 3); // 1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // 3,3,3,4

        client.callProcedure("R3.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R3.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R3.INSERT", 2, 2); // Eliminated
        client.callProcedure("R3.INSERT", 3, 3); // 3,3,3,4

        client.callProcedure("R4.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R4.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("R4.INSERT", 2, 2); // Eliminated
        client.callProcedure("R4.INSERT", 3, 4); // 3,3,3,4
        // Add null values to match null R2 values to be inserted later.
        // Each may match from 2 to 4 R2 rows depending on the ON clause.
        client.callProcedure("R4.INSERT", null, 21);
        client.callProcedure("R4.INSERT", 22, null);
        client.callProcedure("R4.INSERT", null, null);

        client.callProcedure("P3.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P3.INSERT", 2, 2); // Eliminated
        client.callProcedure("P3.INSERT", 3, 3); // 3,3,3,4

        client.callProcedure("P4.INSERT", 1, 1); // 1,1,1,3
        client.callProcedure("P4.INSERT", 2, 2); // Eliminated
        client.callProcedure("P4.INSERT", 3, 4); // 3,3,3,4
        // Add null values to match null R2 values to be inserted later.
        // Each may match from 2 to 4 R2 rows depending on the ON clause.
        client.callProcedure("P4.INSERT", 22, null);

        String query;

        // loop once before inserting non-matched R2 nulls and once after.
        for (int ii = 0; ii < 2; ++ii) {
            validateRowCount(client, "SELECT * FROM R3 JOIN R2 ON R3.A " + joinOp + " R2.A;", 3);
            validateRowCount(client, "SELECT * FROM P3 JOIN R2 ON P3.A " + joinOp + " R2.A;", 2);

            // Add null values that both joinOps must initially ignore
            // as not having any matches but that only "=" should ignore
            // when tables with null matches are queried.
            // It's OK for the second iteration to generate duplicates.
            client.callProcedure("R2.INSERT", null, 21);
            client.callProcedure("R2.INSERT", 22, null);
            client.callProcedure("R2.INSERT", null, null);
        }

        String anotherJoinOp;

        //
        // Joins with R4's matching nulls.
        //
        query = "SELECT * FROM R4 JOIN R2 ON R4.A " + joinOp + " R2.A;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 5);
        } else {
            validateRowCount(client, query, 13);
        }

        query = "SELECT * FROM R4 JOIN R2 ON R4.G " + joinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 3);
        } else {
            // "IS NOT DISTINCT FROM" in the end expression, but not in the search expression (range scan).
            validateRowCount(client, query, 11);
        }

        anotherJoinOp = JOIN_OPS[0];
        query = "SELECT * FROM R4 JOIN R2 ON R4.A " + joinOp + " R2.A AND R4.G " + anotherJoinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 1);
        } else {
            validateRowCount(client, query, 3);
        }

        anotherJoinOp = JOIN_OPS[1];
        query = "SELECT * FROM R4 JOIN R2 ON R4.A " + joinOp + " R2.A AND R4.G " + anotherJoinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 3);
        } else {
            validateRowCount(client, query, 7);
        }

        //
        // Joins with P4's matching nulls.
        //
        validateRowCount(client, "SELECT * FROM P4 JOIN R2 ON P4.A " + joinOp + " R2.A;", 4);

        query = "SELECT * FROM P4 JOIN R2 ON P4.G " + joinOp + " R2.C;";
        if ("=".equals(joinOp)) {
            validateRowCount(client, query, 1);
        } else {
            validateRowCount(client, query, 5);
        }

        anotherJoinOp = JOIN_OPS[0];
        validateRowCount(client,
                "SELECT * FROM P4 JOIN R2 ON P4.A " + joinOp + " R2.A AND P4.G " + anotherJoinOp + " R2.C;",
                1);

        anotherJoinOp = JOIN_OPS[1];
        validateRowCount(client,
                "SELECT * FROM P4 JOIN R2 ON P4.A " + joinOp + " R2.A AND P4.G " + anotherJoinOp + " R2.C;",
                3);
    }

    /**
     * Two table NLIJ
     */
    private void subtestTwoTableIndexInnerWhereJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R3.INSERT", 1, 5); // eliminated by WHERE
        client.callProcedure("R3.INSERT", 1, 1); // eliminated by WHERE
        client.callProcedure("R3.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("R3.INSERT", 3, 3); // eliminated by WHERE
        client.callProcedure("R3.INSERT", 4, 5); // 4,5,4,2
        client.callProcedure("R2.INSERT", 1, 3); // 1,5,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // eliminated by WHERE
        client.callProcedure("R2.INSERT", 4, 2); // 4,5,4,2

        String query;

        query = "SELECT * FROM R3 JOIN R2 ON R3.A " + joinOp + " R2.A WHERE R3.A > R2.C;";
        validateRowCount(client, query, 1);

        client.callProcedure("P3.INSERT", 1, 5); // eliminated by WHERE
        client.callProcedure("P3.INSERT", 2, 2); // Eliminated by JOIN
        client.callProcedure("P3.INSERT", 3, 3); // eliminated by WHERE
        client.callProcedure("P3.INSERT", 4, 3); // 4,3,4,2
        query = "SELECT * FROM P3 JOIN R2 ON P3.A " + joinOp + " R2.A WHERE P3.A > R2.C;";
        validateRowCount(client, query, 1);
    }

    /**
     * Three table NLIJ
     */
    private void subtestThreeTableIndexInnerMultiJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("R1.INSERT", 2, 2, 2); // Eliminated by P3 R1 JOIN
        client.callProcedure("R1.INSERT", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE

        client.callProcedure("R2.INSERT", 1, 1); // Eliminated by P3 R2 JOIN
        client.callProcedure("R2.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("R2.INSERT", 3, 4); // Eliminated by P3 R2 JOIN
        client.callProcedure("R2.INSERT", 4, 0); // Eliminated by WHERE

        client.callProcedure("P3.INSERT", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("P3.INSERT", -1, 0); // Eliminated by WHERE
        client.callProcedure("P3.INSERT", 8, 4); // Eliminated by P3 R1 JOIN

        validateRowCount(client,
                "SELECT * FROM P3 JOIN R1 ON P3.A " + joinOp + " R1.A JOIN R2 ON P3.F " + joinOp + " R2.C WHERE P3.A > 0",
                1);
    }

    /**
     * Two table left and right NLJ
     */
    private void subtestSeqOuterJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 1, 2, 1);
        client.callProcedure("R1.INSERT", 2, 2, 2);
        client.callProcedure("R1.INSERT", -1, 3, 3);
        // R1 1st joined with R2 null
        // R1 2nd joined with R2 null
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null

        VoltTable result;

        // Execution result:
        // 2, 2, 2, NULL, NULL
        // 1, 2, 1, NULL, NULL
        // 1, 1, 1, NULL, NULL
        // -1, 3, 3, NULL, NULL
        result = client.callProcedure("@AdHoc",
                "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C ORDER BY R1.A DESC, R1.C DESC").getResults()[0];
        //* enable to debug */ System.out.println(result);
        assertEquals(4, result.getRowCount());
        VoltTableRow row = result.fetchRow(1);
        assertEquals(1, row.getLong(0));
        assertEquals(2, row.getLong(1));
        assertEquals(1, row.getLong(2));

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 1, 3);
        client.callProcedure("R2.INSERT", 3, null);
        // R1 1st joined with R2 1st
        // R1 2nd joined with R2 1st
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C", 4);
        validateRowCount(client, "SELECT * FROM R2 RIGHT JOIN R1 ON R1.A " + joinOp + " R2.C", 4);

        // Same as above but with partitioned table
        client.callProcedure("P1.INSERT", 1, 1);
        client.callProcedure("P1.INSERT", 1, 2);
        client.callProcedure("P1.INSERT", 2, 2);
        client.callProcedure("P1.INSERT", -1, 3);
        validateRowCount(client, "SELECT * FROM P1 LEFT JOIN R2 ON P1.A " + joinOp + " R2.C", 4);

        // R1 1st joined with R2 with R2 1st
        // R1 2nd joined with R2 null (failed R1.C = 1)
        // R1 3rd joined with R2 null (failed  R1.A " + joinOp + " R2.C)
        // R1 4th3rd joined with R2 null (failed  R1.A " + joinOp + " R2.C)
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C AND R1.C = 1", 4);
        validateRowCount(client, "SELECT * FROM R2 RIGHT JOIN R1 ON R1.A " + joinOp + " R2.C AND R1.C = 1", 4);
        // Same as above but with partitioned table
        validateRowCount(client, "SELECT * FROM R2 RIGHT JOIN P1 ON P1.A " + joinOp + " R2.C AND P1.C = 1", 4);

        // R1 1st joined with R2 null - eliminated by the second join condition
        // R1 2nd joined with R2 null
        // R1 3rd joined with R2 null
        // R1 4th joined with R2 null
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C AND R2.A = 100", 4);

        // R1 1st - joined with R2 not null and eliminated by the filter condition
        // R1 2nd - joined with R2 not null and eliminated by the filter condition
        // R1 3rd - joined with R2 null
        // R1 4th - joined with R2 null
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C WHERE R2.A IS NULL", 2);
        // Same as above but with partitioned table
        validateRowCount(client, "SELECT * FROM P1 LEFT JOIN R2 ON P1.A " + joinOp + " R2.C WHERE R2.A IS NULL", 2);

        // R1 1st - joined with R2 1st row
        // R1 2nd - joined with R2 null eliminated by the filter condition
        // R1 3rd - joined with R2 null eliminated by the filter condition
        // R1 4th - joined with R2 null eliminated by the filter condition
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C WHERE R1.C = 1", 1);

        // R1 1st - eliminated by the filter condition
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - eliminated by the filter condition
        // R1 3rd - joined with the R2 null
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C WHERE R1.A = -1", 1);
        //* enable to debug */ System.out.println(result);
        // Same as above but with partitioned table
        validateRowCount(client, "SELECT * FROM P1 LEFT JOIN R2 ON P1.A " + joinOp + " R2.C WHERE P1.A = -1", 1);
        //* enable to debug */ System.out.println(result);

        // R1 1st - joined with the R2
        // R1 1st - joined with the R2
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - eliminated by the filter condition
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C WHERE R1.A = 1", 2);

        // R1 1st - eliminated by the filter condition
        // R1 2nd - eliminated by the filter condition
        // R1 3rd - joined with R2 null and pass the filter
        // R1 4th - joined with R2 null and pass the filter
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R2 ON R1.A " + joinOp + " R2.C WHERE R2.A is NULL", 2);
    }

    /**
     * Two table left and right NLIJ
     */
    private void subtestIndexOuterJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 3, 3);
        client.callProcedure("R2.INSERT", 4, 4);

        String query;
        VoltTable result;

        // R2 1st joined with R3 null
        // R2 2nd joined with R3 null
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null

        result = client.callProcedure("@AdHoc",
                "SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A ORDER BY R2.A").getResults()[0];
        assertEquals(4, result.getRowCount());
        VoltTableRow row = result.fetchRow(2);
        assertEquals(3, row.getLong(1));

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 5, 5);

        // R2 1st joined with R3 1st
        // R2 2nd joined with R3 2nd
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        validateRowCount(client, "SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A", 4);
        validateRowCount(client, "SELECT * FROM R3 RIGHT JOIN R2 ON R3.A " + joinOp + " R2.A", 4);

        // Same as above but with partitioned table
        client.callProcedure("P2.INSERT", 1, 1);
        client.callProcedure("P2.INSERT", 2, 2);
        client.callProcedure("P2.INSERT", 3, 3);
        client.callProcedure("P2.INSERT", 4, 4);
        validateRowCount(client, "SELECT * FROM P2 LEFT JOIN R3 ON R3.A = P2.A", 4);

        // R2 1st joined with R3 NULL R2.C < 0
        // R2 2nd joined with R3 null R2.C < 0
        // R2 3rd joined with R3 null R2.C < 0
        // R2 4th joined with R3 null R2.C < 0
        validateRowCount(client, "SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A AND R2.C < 0", 4);
        validateRowCount(client, "SELECT * FROM R3 RIGHT JOIN R2 ON R3.A " + joinOp + " R2.A AND R2.C < 0", 4);
        // Same as above but with partitioned table
        query = "SELECT * FROM P2 LEFT JOIN R3 ON R3.A " + joinOp + " P2.A AND P2.E < 0";

        // R2 1st joined with R3 null eliminated by R3.A > 1
        // R2 2nd joined with R3 2nd
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        validateRowCount(client, "SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A AND R3.A > 1", 4);
        validateRowCount(client, "SELECT * FROM R3 RIGHT JOIN R2 ON R3.A " + joinOp + " R2.A AND R3.A > 1", 4);

        // R2 1st joined with R3 1st  but eliminated by  R3.A IS NULL
        // R2 2nd joined with R3 2nd  but eliminated by  R3.A IS NULL
        // R2 3rd joined with R3 null
        // R2 4th joined with R3 null
        query = "SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A WHERE R3.A IS NULL";
        if (joinOp.equals("=") || ! isHSQL()) {
            validateRowCount(client, query, 2); //// PENDING HSQL flaw investigation
        } else {
            result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        }
        query = "SELECT * FROM R3 RIGHT JOIN R2 ON R3.A " + joinOp + " R2.A WHERE R3.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        } else {
            validateRowCount(client, query, 2);
        }
        // Same as above but with partitioned table
        query = "SELECT * FROM R3 RIGHT JOIN P2 ON R3.A " + joinOp + " P2.A WHERE R3.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        } else {
            validateRowCount(client, query, 2);
        }

        // R2 1st eliminated by R2.C < 0
        // R2 2nd eliminated by R2.C < 0
        // R2 3rd eliminated by R2.C < 0
        // R2 4th eliminated by R2.C < 0
        validateRowCount(client, "SELECT * FROM R2 LEFT JOIN R3 ON R3.A " + joinOp + " R2.A WHERE R2.C < 0", 0);
        // Same as above but with partitioned table
        validateRowCount(client, "SELECT * FROM P2 LEFT JOIN R3 ON R3.A " + joinOp + " P2.A WHERE P2.E < 0", 0);

        // Outer table index scan
        // R3 1st eliminated by R3.A > 0 where filter
        // R3 2nd joined with R3 2
        // R3 3rd joined with R2 null
        validateRowCount(client, "select * FROM R3 LEFT JOIN R2 ON R3.A " + joinOp + " R2.A WHERE R3.A > 1", 2);
    }

    /**
     * Two table left and right NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestDistributedOuterJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("P2.INSERT", 1, 1);
        client.callProcedure("P2.INSERT", 2, 2);
        client.callProcedure("P2.INSERT", 3, 3);
        client.callProcedure("P2.INSERT", 4, 4);

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 4, 4);
        client.callProcedure("R3.INSERT", 5, 5);
        // R3 1st joined with P2 not null and eliminated by P2.A IS NULL
        // R3 2nd joined with P2 not null and eliminated by P2.A IS NULL
        // R3 3rd joined with P2 null (P2.A < 3)
        // R3 4th joined with P2 null

        String query;

        query = "SELECT * FROM P2 RIGHT JOIN R3 ON R3.A " + joinOp + " P2.A AND P2.A < 3 WHERE P2.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        } else {
            validateRowCount(client, query, 2);
        }

        client.callProcedure("P3.INSERT", 1, 1);
        client.callProcedure("P3.INSERT", 2, 2);
        client.callProcedure("P3.INSERT", 4, 4);
        client.callProcedure("P3.INSERT", 5, 5);
        // P3 1st joined with P2 not null and eliminated by P2.A IS NULL
        // P3 2nd joined with P2 not null and eliminated by P2.A IS NULL
        // P3 3rd joined with P2 null (P2.A < 3)
        // P3 4th joined with P2 null
        query = "select *  FROM P2 RIGHT JOIN P3 ON P3.A " + joinOp + " P2.A AND P2.A < 3 WHERE P2.A IS NULL";
        if (isHSQL()) { //// PENDING HSQL flaw investigation
            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);
            if (2 == result.getRowCount()) {
                System.out.println("The HSQL error MAY have been solved. Consider simplifying this test.");
            }
        } else {
            validateRowCount(client, query, 2);
        }
        // Outer table index scan
        // P3 1st eliminated by P3.A > 0 where filter
        // P3 2nd joined with P2 2
        // P3 3nd joined with P2 4
        // R3 4th joined with P2 null
        validateRowCount(client, "select * FROM P3 LEFT JOIN P2 ON P3.A " + joinOp + " P2.A WHERE P3.A > 1", 3);

        // NLJ join of (P2, P2) on a partition column P2.A
        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.E, RHS.A, RHS.E FROM P2 LHS LEFT JOIN P2 RHS ON LHS.A " + joinOp + " RHS.A AND " +
                        "LHS.A < 2 AND RHS.E = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {1, 1, 1, 1},
                        {2, 2, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.E, RHS.A, RHS.E FROM P2 LHS RIGHT JOIN P2 RHS ON LHS.A " + joinOp + " RHS.A AND " +
                        "LHS.A < 2 AND RHS.E = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, 1, 1}
                });

        // NLJ join of (P2, P2) on a partition column P1.A
        // and a constant partition key pseudo-filter
        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.E, RHS.A, RHS.E FROM P2 LHS LEFT JOIN P2 RHS ON LHS.A " + joinOp +
                        " RHS.A AND LHS.A = 1 AND RHS.E = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {1, 1, 1, 1},
                        {2, 2, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // NLJ join of (P2, P2) on a partition column P1.A
        // and a constant partition key pseudo-filter
        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.E, RHS.A, RHS.E FROM P2 LHS RIGHT JOIN P2 RHS ON LHS.A " + joinOp +
                        " RHS.A AND LHS.A = 1 AND RHS.E = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, 1, 1}
                });

        // NLIJ join of (P2, P3) on partition columns

        //* enable to debug */ System.out.println(client.callProcedure("@Explain", query).getResults()[0]);
        validateTableOfLongs(client,
                "SELECT P2.A, P2.E, P3.A, P3.F FROM P2 LEFT JOIN P3 ON P2.A = P3.A AND P2.A < 2 AND P3.F = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {1, 1, 1, 1},
                        {2, 2, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // NLIJ join of (P2, P3) on partition columns
        validateTableOfLongs(client,
                "SELECT P2.A, P2.E, P3.A, P3.F FROM P2 RIGHT JOIN P3 " +
                        "ON P2.A = P3.A AND P2.A < 2 AND P3.F = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, 1, 1},
                });

        // NLIJ join of (P2, P3) on partition columns
        validateTableOfLongs(client,
                "SELECT P2.A, P2.E, P3.A, P3.F FROM P2 LEFT JOIN P3 " +
                        "ON P2.A = P3.A AND P2.A = 1 AND P3.F = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {1, 1, 1, 1},
                        {2, 2, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // NLIJ join of (P2, P3) on partition columns
        validateTableOfLongs(client,
                "SELECT P2.A, P2.E, P3.A, P3.F FROM P2 RIGHT JOIN P3 " +
                        "ON P2.A = P3.A AND P2.A = 1 AND P3.F = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, 1, 1},
                });
    }

    /**
     * IN LIST JOIN/WHERE Expressions
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testInListJoin(String joinOp) throws Exception {
        Client client = this.getClient();
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 2, 2, 2);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 4, 4);
        client.callProcedure("R3.INSERT", 5, 5);
        client.callProcedure("R3.INSERT", 6, 6);

        // Outer join - IN LIST is outer table join index expression
        validateRowCount(client, "SELECT * FROM R3 LEFT JOIN R1 ON R3.A " + joinOp + " R1.A AND R3.A IN (1,2)", 5);
        // Outer join - IN LIST is outer table join non-index expression
        validateRowCount(client, "SELECT * FROM R3 LEFT JOIN R1 ON R3.A " + joinOp + " R1.A AND R3.C IN (1,2)", 5);
        // Inner join - IN LIST is join index expression
        validateRowCount(client, "SELECT * FROM R3 JOIN R1 ON R3.A " + joinOp + " R1.A and R3.A in (1,2)", 2);
        // Outer join - IN LIST is inner table join index expression
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R3 ON R3.A " + joinOp + " R1.A and R3.A in (1,2)", 4);
        // Outer join - IN LIST is inner table join scan expression
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R3 ON R3.A " + joinOp + " R1.A and R3.C in (1,2)", 4);
        // Outer join - IN LIST is outer table where index expression
        validateRowCount(client, "SELECT * FROM R3 LEFT JOIN R1 ON R3.A " + joinOp + " R1.A WHERE R3.A in (1,2)", 2);
        // Outer join - IN LIST is outer table where scan expression
        validateRowCount(client, "SELECT * FROM R3 LEFT JOIN R1 ON R3.A " + joinOp + " R1.A WHERE R3.C in (1,2)", 2);
        // Inner join - IN LIST is where index expression
        validateRowCount(client, "SELECT * FROM R3 JOIN R1 ON R3.A " + joinOp + " R1.A WHERE R3.A in (1,2)", 2);
        // Inner join - IN LIST is where scan expression
        validateRowCount(client, "SELECT * FROM R3 JOIN R1 ON R3.A " + joinOp + " R1.A WHERE R3.C in (1,2)", 2);
        // Outer join - IN LIST is inner table where index expression
        validateRowCount(client, "SELECT * FROM R1 LEFT JOIN R3 ON R3.A " + joinOp + " R1.A WHERE R3.A in (1,2)", 2);
    }

    /**
     * Multi table outer join
     */
    public void testOuterJoin() throws Exception {
        Client client = getClient();
        for (String joinOp : JOIN_OPS) {
            subtestOuterJoinMultiTable(client, joinOp);
            subtestOuterJoinENG8692(client, joinOp);
        }
    }

    private void subtestOuterJoinMultiTable(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 11, 11, 11);
        client.callProcedure("R1.INSERT", 12, 12, 12);
        client.callProcedure("R1.INSERT", 13, 13, 13);

        client.callProcedure("R2.INSERT", 21, 21);
        client.callProcedure("R2.INSERT", 22, 22);
        client.callProcedure("R2.INSERT", 12, 12);

        client.callProcedure("R3.INSERT", 31, 31);
        client.callProcedure("R3.INSERT", 32, 32);
        client.callProcedure("R3.INSERT", 33, 21);

        validateRowCount(client,
                "SELECT * FROM R1 RIGHT JOIN R2 ON R1.A " + joinOp + " R2.A LEFT JOIN R3 ON R3.C " + joinOp + " R2.C",
                3);

        validateRowCount(client,
                "SELECT * FROM R1 RIGHT JOIN R2 ON R1.A " + joinOp + " R2.A LEFT JOIN R3 ON R3.C " + joinOp +
                        " R2.C WHERE R1.C > 0", 1);

         // truncate tables
         client.callProcedure("@AdHoc", "truncate table R1;");
         client.callProcedure("@AdHoc", "truncate table R2;");
         client.callProcedure("@AdHoc", "truncate table R3;");
    }

    private void subtestOuterJoinENG8692(Client client, String joinOp) throws Exception {
        client.callProcedure("@AdHoc", "truncate table t1;");
        client.callProcedure("@AdHoc", "truncate table t2;");
        client.callProcedure("@AdHoc", "truncate table t3;");
        client.callProcedure("@AdHoc", "truncate table t4;");
        client.callProcedure("@AdHoc", "INSERT INTO t1 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t2 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t3 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t4 VALUES(1);");
        client.callProcedure("@AdHoc", "INSERT INTO t4 VALUES(null);");

        // case 1: missing join expression
        validateTableOfLongs(client,
                "SELECT * FROM t1 INNER JOIN t2 ON t1.i1 " + joinOp + " t2.i2 RIGHT OUTER JOIN t3 ON t1.i1 = 1000;",
                new long[][]{{NULL_VALUE, NULL_VALUE, 1}});

        // case 2: more than 5 table joins
        validateTableOfLongs(client,
                "SELECT * FROM t1 INNER JOIN t2 AS t2_copy1 ON t1.i1 " + joinOp + " t2_copy1.i2 " +
                        "INNER JOIN t2 AS t2_copy2 ON t1.i1 " + joinOp + " t2_copy2.i2 " +
                        "INNER JOIN t2 AS t2_copy3 ON t1.i1 " + joinOp + " t2_copy3.i2 " +
                        "INNER JOIN t2 AS t2_copy4 ON t1.i1 " + joinOp + " t2_copy4.i2 " +
                        "RIGHT OUTER JOIN t3 ON t1.i1 " + joinOp + " t3.i3 AND t3.i3 < -1000;",
                new long[][]{{NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 1}});

        // case 3: reverse scan with null data
        validateTableOfLongs(client,
                "SELECT * FROM t1 INNER JOIN t2 ON t1.i1 " + joinOp + " t2.i2 INNER JOIN t4 ON t4.i4 < 45;",
                new long[][]{{1, 1, 1}});
    }

    public void testFullJoins() throws Exception {
        Client client = getClient();
        truncateTables(client, SEQ_TABLES);
        subtestNonEqualityFullJoin(client);
        truncateTables(client, SEQ_TABLES);
        subtestUsingFullJoin(client);

        for (String joinOp : JOIN_OPS) {
            truncateTables(client, SEQ_TABLES);
            subtestTwoReplicatedTableFullNLJoin(client, joinOp);
            truncateTables(client, INDEXED_TABLES);
            subtestTwoReplicatedTableFullNLIJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            truncateTables(client, INDEXED_TABLES);
            subtestDistributedTableFullJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            subtestLimitOffsetFullNLJoin(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            truncateTables(client, INDEXED_TABLES);
            subtestMultipleFullJoins(client, joinOp);
            truncateTables(client, SEQ_TABLES);
            truncateTables(client, INDEXED_TABLES);
            subtestFullJoinOrderBy(client, joinOp);
        }
    }

    private void subtestTwoReplicatedTableFullNLJoin(Client client, String joinOp) throws Exception {
        // case: two empty tables
        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp +
                        " R2.C ORDER BY 1, 2, 3, 4",
                new long[][]{});

        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 1, 1);
        client.callProcedure("R1.INSERT", 2, 4, 4);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);
        // Delete one row to have non-active tuples in the table
        client.callProcedure("@AdHoc", "DELETE FROM R1 WHERE A = 2 AND C = 4 AND D = 4;");

        // case: Right table is empty
        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp +
                        " R2.C ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE},
                });

        // case: Left table is empty
        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R2 FULL JOIN R1 ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp +
                        " R2.C ORDER BY R1.A, R1.D, R2.A, R2.C",
                new long[][]{
                        {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE},
                });

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 3, 3);
        client.callProcedure("R2.INSERT", 4, 4);
        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 5, null);
        // Delete one row to have non-active tuples in the table
        client.callProcedure("@AdHoc", "DELETE FROM R2 WHERE A = 4 AND C = 4;");

        // case 1: equality join on two columns
        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                        "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, 2, 1},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 2: equality join on two columns plus outer join expression
        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 " +
                        "ON R1.A " + joinOp + " R2.A AND R1.D " + joinOp + " R2.C AND R1.C = 1 " +
                        "ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, 2, 1},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 5: equality join on single column
        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, NULL_VALUE, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, 2, 1},
                        {2, 1, 2, 2},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 6: equality join on single column and WHERE inner expression

        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp +
                        " R2.A WHERE R2.C = 3 OR R2.C IS NULL ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        String query = "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A " +
                "WHERE R1.A = 3 OR R1.A IS NULL ORDER BY 1, 2, 3, 4";
        if (isHSQL()) {
            VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
            System.out.println("Ignoring erroneous(?) HSQL baseline: " + result);

            // HSQL incorrectly returns
            //   NULL,NULL,1,1
            //   NULL,NULL,2,1
            //   NULL,NULL,2,2
            //   NULL,NULL,5,NULL
            //   NULL,NULL,5,5
            //   3,3,3,3
        } else {
            // case 7: equality join on single column and WHERE outer expression

            validateTableOfLongs(client, query, new long[][]{
                {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                {NULL_VALUE, NULL_VALUE, 5, 5},
                {3, 3, 3, 3}
            });
        }

        // case 8: equality join on single column and WHERE inner-outer expression

        validateTableOfLongs(client,
                "SELECT R1.A, R1.D, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A WHERE R1.A = 3 OR R2.C IS NULL ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });
    }

    private void subtestLimitOffsetFullNLJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 3, 1);
        client.callProcedure("R1.INSERT", 3, 4, 3);
        client.callProcedure("R1.INSERT", 4, 5, 4);

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 2, 3);
        client.callProcedure("R2.INSERT", 3, 4);
        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 5, 6);

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 2, 3);
        client.callProcedure("R3.INSERT", 3, 4);
        client.callProcedure("R3.INSERT", 5, 5);
        client.callProcedure("R3.INSERT", 5, 6);

        // NLJ SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A
        //  1,1,1,1             outer-inner match
        //  1,2,1,1             outer-inner match
        //  2,3,2,2             outer-inner match
        //  2,3,2,3             outer-inner match
        //  3,4,3,4             outer-inner match
        //  4,5,NULL,NULL       outer no match
        //  NULL,NULL,5,5       inner no match
        //  NULL,NULL,5,6       inner no match
        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A " +
                        "ORDER BY R1.A, R2.C LIMIT 2 OFFSET 5",
                new long[][]{ {2, 3, 2, 3}, {3, 4, 3, 4} });

        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp +
                        " R2.A ORDER BY R1.A, R2.C LIMIT 2 OFFSET 6",
                new long[][]{ {3, 4, 3, 4}, {4,5, NULL_VALUE, NULL_VALUE} });

        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R2.A, R2.C FROM R1 FULL JOIN R2 ON R1.A " + joinOp +
                        " R2.A ORDER BY COALESCE(R1.C, 10), R2.C LIMIT 3 OFFSET 4",
                new long[][]{
                        {3, 4, 3, 4},
                        {4,5, NULL_VALUE, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5}
                });

        validateRowCount(client,
                "SELECT MAX(R1.C), R1.A, R2.A FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A GROUP BY R1.A, R2.A LIMIT 2 OFFSET 2",
                2);

        // NLIJ SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 on R1.A " + joinOp + " R3.A
        //  1,1,1,1             outer-inner match
        //  1,2,1,1             outer-inner match
        //  2,3,2,2             outer-inner match
        //  2,3,2,3             outer-inner match
        //  3,4,3,4             outer-inner match
        //  4,5,NULL,NULL       outer no match
        //  NULL,NULL,5,5       inner no match
        //  NULL,NULL,5,6       inner no match

        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 ON R1.A " + joinOp +
                        " R3.A ORDER BY COALESCE(R1.A, 10), R3.C LIMIT 2 OFFSET 3",
                new long[][]{ {2, 3, 2, 3}, {3, 4, 3, 4} });

        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 ON R1.A " + joinOp +
                        " R3.A ORDER BY R1.A, R3.C LIMIT 2 OFFSET 6",
                new long[][]{ {3, 4, 3, 4}, {4, 5, NULL_VALUE, NULL_VALUE} });

        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 ON R1.A " + joinOp +
                        " R3.A ORDER BY COALESCE(R1.A, 10), R3.C LIMIT 3 OFFSET 4",
                new long[][]{
                        {3, 4, 3, 4L},
                        {4,5, NULL_VALUE, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5}
                });
        validateRowCount(client,
                "SELECT MAX(R1.C), R1.A, R3.A FROM R1 FULL JOIN R3 ON R1.A " + joinOp +
                        " R3.A GROUP BY R1.A, R3.A LIMIT 2 OFFSET 2",
                2);
    }

    private void subtestDistributedTableFullJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("P1.INSERT", 1, 1);
        client.callProcedure("P1.INSERT", 1, 2);
        client.callProcedure("P1.INSERT", 2, 1);
        client.callProcedure("P1.INSERT", 3, 3);
        client.callProcedure("P1.INSERT", 4, 4);

        client.callProcedure("P3.INSERT", 1, 1);
        client.callProcedure("P3.INSERT", 2, 1);
        client.callProcedure("P3.INSERT", 3, 3);
        client.callProcedure("P3.INSERT", 4, 4);

        client.callProcedure("R2.INSERT", 1, 1);
        client.callProcedure("R2.INSERT", 2, 1);
        client.callProcedure("R2.INSERT", 2, 2);
        client.callProcedure("R2.INSERT", 3, 3);
        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 5, null);

        // case 1: equality join of (P1, R2) on a partition column P1.A

        validateTableOfLongs(client,
                "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 ON P1.A " + joinOp +
                        " R2.A ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, 2, 1},
                        {2, 1, 2, 2},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 2: equality join of (P1, R2) on a non-partition column

        validateTableOfLongs(client,
                "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 ON P1.C " + joinOp +
                        " R2.A WHERE (P1.A > 1 OR P1.A IS NULL) AND (R2.A = 3 OR R2.A IS NULL) ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 3: NLJ FULL join (R2, P1) on partition column  P1.E " + joinOp + " R2.A AND P1.A > 2 are join predicate

        validateTableOfLongs(client,
                "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 ON P1.C " + joinOp +
                        " R2.A AND P1.A > 2 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 2},
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 4: NLJ FULL join (R2, P1) on partition column  P1.E " + joinOp + " R2.A AND R2.A > 1 are join predicate

        validateTableOfLongs(client,
                "SELECT P1.A, P1.C, R2.A, R2.C FROM P1 FULL JOIN R2 ON P1.C " + joinOp +
                        " R2.A AND R2.A > 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 1},
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {1, 2, 2, 1},
                        {1, 2, 2, 2},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 5: equality join of (P3, R2) on a partition/index column P1.A, Still NLJ
        validateTableOfLongs(client,
                "SELECT P3.A, P3.F, R2.A, R2.C FROM P3 FULL JOIN R2 ON P3.A " + joinOp + " R2.A ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, 1, 1},
                        {2, 1, 2, 1},
                        {2, 1, 2, 2},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 6: NLJ join of (P1, P1) on a partition column P1.A
        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.C, RHS.A, RHS.C FROM P1 LHS FULL JOIN P1 RHS ON LHS.A " + joinOp +
                        " RHS.A AND LHS.A < 2 AND RHS.C = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 2},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 7: NLJ join of (P1, P1) on a partition column P1.A
        // and a constant partition key pseudo-filter

        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.C, RHS.A, RHS.C FROM P1 LHS FULL JOIN P1 RHS ON LHS.A " + joinOp +
                        " RHS.A AND LHS.A = 1 AND RHS.C = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 2},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 8: NLIJ join of (P1, P3) on partition columns

        validateTableOfLongs(client,
                "SELECT P1.A, P1.C, P3.A, P3.F FROM P1 FULL JOIN P3 ON P1.A " + joinOp +
                        " P3.A AND P1.A < 2 AND P3.F = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 8: NLIJ join of (P1, P3) on partition columns

        validateTableOfLongs(client,
                "SELECT P1.A, P1.C, P3.A, P3.F FROM P1 FULL JOIN P3 ON P1.A " + joinOp +
                        " P3.A AND P1.A = 1 AND P3.F = 1 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });
    }

    private void subtestTwoReplicatedTableFullNLIJoin(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 1, 1);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);

        // case 0: Empty FULL NLIJ, inner join R3.A > 0 is added as a post-predicate to the inline Index scan
        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 ON R3.A " + joinOp +
                        " R1.A AND R3.A > 2 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {4, 4, NULL_VALUE, NULL_VALUE},
                });

        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 1);
        client.callProcedure("R3.INSERT", 3, 2);
        client.callProcedure("R3.INSERT", 4, 3);
        client.callProcedure("R3.INSERT", 5, 5);

        // case 1: FULL NLIJ, inner join R3.A > 0 is added as a post-predicate to the inline Index scan
        validateTableOfLongs(client,
                "SELECT R1.A, R1.C, R3.A, R3.C FROM R1 FULL JOIN R3 ON R3.A " + joinOp +
                        " R1.A AND R3.A > 2 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {1, 2, NULL_VALUE, NULL_VALUE},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 3, 3, 2},
                        {4, 4, 4, 3}
                });

        // case 2: FULL NLIJ, inner join L.A > 0 is added as a pre-predicate to the NLIJ

        validateTableOfLongs(client,
                "SELECT LHS.A, LHS.C, RHS.A, RHS.C FROM R3 LHS FULL JOIN R3 RHS ON LHS.A " + joinOp +
                        " RHS.A AND LHS.A > 3 ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 2},
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {2, 1, NULL_VALUE, NULL_VALUE},
                        {3, 2, NULL_VALUE, NULL_VALUE},
                        {4, 3, 4, 3},
                        {5, 5, 5, 5}
                });
    }

    private void subtestNonEqualityFullJoin(Client client) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 10, 10, 2);

        client.callProcedure("R2.INSERT", 5, 5);
        client.callProcedure("R2.INSERT", 8, 8);

        client.callProcedure("P2.INSERT", 5, 5);
        client.callProcedure("P2.INSERT", 8, 8);

        // case 1: two replicated tables joined on non-equality condition
        validateTableOfLongs(client,
                "SELECT R1.A, R2.A FROM R1 FULL JOIN R2 ON R1.A > 15 ORDER BY R1.A, R2.A",
                new long[][]{ {NULL_VALUE, 5}, {NULL_VALUE, 8}, {1, NULL_VALUE}, {10, NULL_VALUE} });

        // case 2: two replicated tables joined on non-equality inner and outer conditions
        validateTableOfLongs(client,
                "SELECT R1.A, R2.A FROM R1 FULL JOIN R2 ON R1.A > 5 AND R2.A < 7 ORDER BY R1.A, R2.A",
                new long[][]{ {NULL_VALUE, 8}, {1, NULL_VALUE}, {10, 5} });

        // case 3: distributed table joined on non-equality inner and outer conditions
        validateTableOfLongs(client,
                "SELECT R1.A, P2.A FROM R1 FULL JOIN P2 ON R1.A > 5 AND P2.A < 7 ORDER BY R1.A, P2.A",
                new long[][]{ {NULL_VALUE, 8}, {1, NULL_VALUE}, {10, 5} });
    }

    private void subtestMultipleFullJoins(Client client, String joinOp) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, 1);
        client.callProcedure("R1.INSERT", 10, 10, 2);

        client.callProcedure("R2.INSERT", 1, 2);
        client.callProcedure("R2.INSERT", 3, 8);

        client.callProcedure("P2.INSERT", 1, 3);
        client.callProcedure("P2.INSERT", 8, 8);

        String query;

        // The R1-R2 FULL join is an inner node in the RIGHT join with P2
        // The P2.A = R2.A join condition is NULL-rejecting for the R2 table
        // simplifying the FULL to be R1 RIGHT JOIN R2 which gets converted to R2 LEFT JOIN R1
        validateTableOfLongs(client,
                "SELECT * FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A RIGHT JOIN P2 ON P2.A " + joinOp + " R1.A ORDER BY P2.A",
                new long[][]{{1, 1, 1, 1, 2, 1, 3}, {NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 8, 8}});

        // The R1-R2 FULL join is an outer node in the top LEFT join and is not simplified
        // by the P2.A " + joinOp + " R2.A expression
        validateTableOfLongs(client,
                "SELECT * FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A LEFT JOIN P2 ON P2.A " + joinOp + " R2.A ORDER BY P2.A",
                new long[][]{
                        {10, 10, 2, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, 3, 8, NULL_VALUE, NULL_VALUE},
                        {1, 1, 1, 1, 2, 1, 3}
                });

        // The R1-R2 RIGHT join is an outer node in the top FULL join and is not simplified
        // by the P2.A = R1.A expression
        validateTableOfLongs(client,
                "SELECT * FROM R1 RIGHT JOIN R2 ON R1.A " + joinOp + " R2.A FULL JOIN P2 ON R1.A = P2.A ORDER BY P2.A",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, 3, 8, NULL_VALUE, NULL_VALUE},
                        {1, 1, 1, 1, 2, 1, 3},
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 8, 8}
                });

        // The R1-R2 FULL join is an outer node in the top FULL join and is not simplified
        // by the P2.A " + joinOp + " R1.A expression
        validateTableOfLongs(client,
                "SELECT * FROM R1 FULL JOIN R2 ON R1.A " + joinOp + " R2.A FULL JOIN P2 ON R1.A = P2.A ORDER BY P2.A",
                new long[][]{
                        {10, 10, 2, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, 3, 8, NULL_VALUE, NULL_VALUE},
                        {1, 1, 1, 1, 2, 1, 3},
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, NULL_VALUE, 8, 8}
                });
    }

    private void subtestUsingFullJoin(Client client) throws Exception {
        client.callProcedure("R1.INSERT", 1, 1, null);
        client.callProcedure("R1.INSERT", 1, 2, 2);
        client.callProcedure("R1.INSERT", 2, 1, 1);
        client.callProcedure("R1.INSERT", 3, 3, 3);
        client.callProcedure("R1.INSERT", 4, 4, 4);

        client.callProcedure("R2.INSERT", 1, 3);
        client.callProcedure("R2.INSERT", 3, 8);
        client.callProcedure("R2.INSERT", 5, 8);

        client.callProcedure("R3.INSERT", 1, 3);
        client.callProcedure("R3.INSERT", 6, 8);

        validateTableOfLongs(client,
                 "SELECT MAX(R1.C), A FROM R1 FULL JOIN R2 USING (A) WHERE A > 0 GROUP BY A ORDER BY A",
                new long[][]{ {2, 1}, {1, 2}, {3, 3}, {4, 4}, {NULL_VALUE, 5} });
        validateTableOfLongs(client,
                "SELECT A FROM R1 JOIN R2 USING (A) JOIN R3 USING(A) WHERE A > 0 ORDER BY A",
                new long[][]{{1}, {1}});
        validateTableOfLongs(client,
                "SELECT A FROM R1 FULL JOIN R2 USING (A) FULL JOIN R3 USING(A) WHERE A > 0 ORDER BY A",
                new long[][]{ {1}, {1}, {2}, {3}, {4}, {5}, {6} });
    }

    private void subtestFullJoinOrderBy(Client client, String joinOp) throws Exception {
        client.callProcedure("R3.INSERT", 1, null);
        client.callProcedure("R3.INSERT", 1, 1);
        client.callProcedure("R3.INSERT", 2, 2);
        client.callProcedure("R3.INSERT", 2, 3);
        client.callProcedure("R3.INSERT", 3, 1);

        long[][] toExpect;
        if (joinOp.equals("=")) {
            toExpect = new long[][]{ {NULL_VALUE}, {1}, {1}, {1}, {2}, {2}, {3}, {3} };
        } else {
            // Accepting NULL values in L.C IS NOT DISTINCT FROM R.C
            // eliminates one left-padded row with a null L.A and
            // substitutes a match row with value L.A = 1 indistinguishable
            // here from the right-padded row with L.A = 1 it replaces.
            // {NULL_VALUE},
            toExpect = new long[][]{ {1}, {1}, {1}, {2}, {2}, {3}, {3} };
        }
        validateTableOfLongs(client,
                "SELECT L.A FROM R3 L FULL JOIN R3 R ON L.C " + joinOp + " R.C ORDER BY A", toExpect);

        if (joinOp.equals("=")) {
            toExpect = new long[][]{ {NULL_VALUE, NULL_VALUE}, {1, 2}, {2, 5}, {3, 2} };
        } else {
                // Accepting NULL values in L.C IS NOT DISTINCT FROM R.C
                // eliminates null pad rows and adds a match row with L.C = null
                // that has no effect on the sums.
                // {NULL_VALUE, NULL_VALUE},
            toExpect = new long[][]{ {1, 2}, {2, 5}, {3, 2} };
        }
        validateTableOfLongs(client,
                "SELECT L.A, SUM(L.C) FROM R3 L FULL JOIN R3 R ON L.C " + joinOp + " R.C GROUP BY L.A ORDER BY 1",
                toExpect);
    }

    public void testEng13603() throws Exception {
        Client client = getClient();

        assertSuccessfulDML(client,"insert into T1_ENG_13603 values (null, 1.0, 1);");
        assertSuccessfulDML(client,"insert into T1_ENG_13603 values (0, null, null);");
        assertSuccessfulDML(client,"insert into T2_ENG_13603 values (null);");
        assertSuccessfulDML(client,"insert into T2_ENG_13603 values (0);");

        String query = "select ratio, count(*) "
                + "from T1_ENG_13603 T1 full outer join T2_ENG_13603 T2 on T1.id = T2.id "
                + "group by T1.ratio "
                + "order by T1.ratio;";
        ClientResponse cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        System.out.println(cr.getResults()[0]);
        assertContentOfTable(new Object[][] {{null, 2}, {1.0, 1}}, cr.getResults()[0]);
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJoinsSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestJoinsSuite.class.getResource("testjoins-ddl.sql"));
//        try {
//          project.addLiteralSchema("CREATE PROCEDURE R4_INSERT AS INSERT INTO R4 VALUES(?, ?);");
//      }
//        catch (IOException e) {
//          e.printStackTrace();
//          fail();
//      }

        LocalCluster config;

        config = new LocalCluster("testjoin-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testjoin-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testjoin-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }
}
