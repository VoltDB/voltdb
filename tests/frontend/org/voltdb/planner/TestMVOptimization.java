/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.planner;
import java.util.Arrays;
import java.util.List;

import org.voltdb.VoltProcedure;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.SQLStmt;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.sysprocs.saverestore.StreamSnapshotRequestConfig;

public class TestMVOptimization extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(PlannerTestCase.class.getResource("testplans-materialized-view-optimization-ddl.sql"),
        "testplans-MV-optimizer", false);
    }

    static private String explainSimpleMVScan(final String viewName) {
        return "RETURN RESULTS TO STORED PROCEDURE \n" +
                "INDEX SCAN of \"" + viewName +
                "\" using its primary key index (for deterministic order only)";
    }

    private String explain(final String query) {        // normalize ws
        List<AbstractPlanNode> pns = compileToFragments(query);
        assertFalse(pns.isEmpty());
        return pns.get(pns.size() - 1).toExplainPlanString()
                .replace("\n", "")
                .replaceAll(" +", " ");
    }

    // normalize word breaks/line breaks across results
    private void assertMatch(final String query, final String expected) {
        assertEquals(expected.replace("\n", "").replaceAll(" +", " "), explain(query));
    }

    public void testSimple() {      // Test SELECT stmt that is "almost" same as view definition, with except of column aliasing
        checkQueriesPlansAreTheSame("SELECT a1 a1, COUNT(b) count_b, SUM(a) sum_a, COUNT(*) FROM t1 GROUP BY a1",
                "SELECT distinct_a, count_b00, sum_a00, counts from v5_2");
        // Negative test: MV does not support HAVING clause
        assertMatch("SELECT a1 a1, COUNT(b) count_b, SUM(a) sum_a, COUNT(*) FROM t1 GROUP BY a1 HAVING SUM(a) >= 0",
                "RETURN RESULTS TO STORED PROCEDURE INDEX SCAN of \"T1\" " +
                        "using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" (for deterministic order only) " +
                        "inline Hash AGGREGATION ops: COUNT(T1.B), SUM(T1.A), COUNT(*) HAVING (SUM_A >= 0)");
        // Negative test: SELECT stmt from multiple tables are not supported (for now)
        assertMatch("SELECT DISTINCT a1, COUNT(b) count_b, SUM(a) sum_a, COUNT(*) FROM t1 INNER JOIN t2 ON t1.b1 = t2.b0 GROUP BY a1",
                "RETURN RESULTS TO STORED PROCEDURE NESTLOOP INDEX " +
                        "INNER JOIN inline Hash AGGREGATION ops: COUNT(T1.B), SUM(T1.A), COUNT(*) " +
                        "inline INDEX SCAN of \"T1\" using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" uniquely match " +
                        "(B1 = T2.B0) SEQUENTIAL SCAN of \"T2\"");
    }

    public void testDistinct() {  // Test SELECT stmt that is "almost" same as view definition with exception of GBY column distinctiveness
        checkQueriesPlansAreTheSame("SELECT distinct a1 distinct_a1, count(b) count_b, sum(a) sum_a, count(*) from t1 group by a1",
                "SELECT distinct_a, count_b00, sum_a00, counts from v5_2");
        checkQueriesPlansAreTheSame("SELECT a1 a1, count(b) count_b, sum(a) sum_a, count(*) from t1 group by a1",
                "SELECT distinct_a, count_b00, sum_a00, counts from v5_2");
        // Distinctness is not distinguished for multiple GBY columns,
        checkQueriesPlansAreTheSame("SELECT B, A, SUM(A1), MIN(B1), COUNT(*) FROM T1 GROUP BY A, B",
                "SELECT b, a, sum_a1, min_b1, counts from v3");
        checkQueriesPlansAreTheSame("SELECT distinct B, A, SUM(A1), MIN(B1), COUNT(*) FROM T1 GROUP BY A, B",
                "SELECT b, a, sum_a1, min_b1, counts from v3");
        // or GBY expressions
        checkQueriesPlansAreTheSame("SELECT a * 2 + a1, b - a, SUM(a1), MIN(b1), COUNT(*) FROM t1 GROUP BY a * 2 + a1, b - a;",
                "SELECT a2pa1, b_minus_a, sum_a1, min_b1, counts from v4");
        checkQueriesPlansAreTheSame("SELECT distinct (a * 2 + a1), b - a, SUM(a1), MIN(b1), COUNT(*) FROM t1 GROUP BY a * 2 + a1, b - a;",
                "SELECT a2pa1, b_minus_a, sum_a1, min_b1, counts from v4");
    }

    public void testSubset() {      // test column order shuffling
        // GBY one column
        checkQueriesPlansAreTheSame("SELECT COUNT(b) count_b, a1 a1, SUM(a) sum_a, COUNT(*) FROM t1 GROUP BY a1",
                "SELECT count_b00, distinct_a, sum_a00, counts from v5_2");
        // GBY multiple columns
        checkQueriesPlansAreTheSame("SELECT MIN(B1) FROM T1 GROUP BY A, B",
                "SELECT min_b1 from v3");
        // GBY complex expressions
        checkQueriesPlansAreTheSame("SELECT b - a, SUM(a1), MIN(b1), COUNT(*) FROM t1 GROUP BY a * 2 + a1, b - a;",
                "SELECT b_minus_a, sum_a1, min_b1, counts from v4");
    }
    public void testSubsetReorder() { // test subset of VIEW definition columns, but same order
        checkQueriesPlansAreTheSame("SELECT COUNT(b) count_b, SUM(a) sum_a, COUNT(*) FROM t1 GROUP BY a1",
                "SELECT count_b00, sum_a00, counts from v5_2");
    }

    public void testFilterMatch() { // test ordering and logically equivalent filtering rule matches
        final String expected = explainSimpleMVScan("V5_1");
        assertMatch("SELECT a1, SUM(a), COUNT(b1), COUNT(*) FROM t1 WHERE b >= 2 OR b1 IN (3, 30, 300) GROUP BY a1", expected);
        assertMatch("SELECT a1, SUM(a), COUNT(b1), COUNT(*) FROM t1 WHERE b >= 2 OR b1 IN (3, 300, 30) GROUP BY a1", expected);
        assertMatch("SELECT a1, SUM(a), COUNT(b1), COUNT(*) FROM t1 WHERE b >= 2 OR b1 IN (3, 300, 30, 3) GROUP BY a1", expected);
        assertMatch("SELECT a1, SUM(a), COUNT(b1), COUNT(*) FROM t1 WHERE b1 IN (3, 300, 30) OR b >= 2 GROUP BY a1", expected);
        assertMatch("SELECT a1, SUM(a), COUNT(b1), COUNT(*) FROM t1 WHERE b1 IN (3, 300, 30) OR 2 <= b GROUP BY a1", expected);
    }

    public void testGroupByMultipleColumns() {
        checkQueriesPlansAreTheSame("SELECT A, B, SUM(A1), MIN(B1), COUNT(*) FROM T1 GROUP BY A, B",
                "SELECT a, b, sum_a1, min_b1, counts from v3");
        // GBY ordering does not matter
        checkQueriesPlansAreTheSame("SELECT A, B, SUM(A1), MIN(B1), COUNT(*) FROM T1 GROUP BY B, A",
                "SELECT a, b, sum_a1, min_b1, counts from v3");
    }

    public void testComplexGbyExpr() {  // test complex GBY expressions
        checkQueriesPlansAreTheSame("SELECT a * 2 + a1, b - a, SUM(a1), MIN(b1), COUNT(*) FROM t1 GROUP BY a * 2 + a1, b - a;",
                "SELECT a2pa1, b_minus_a, sum_a1, min_b1, counts FROM v4");
        // negative test: equivalent but different expression
        assertMatch(
                "SELECT a1 + a * 2, b - a, SUM(a1), MIN(b1), COUNT(*) FROM t1 GROUP BY a1 + a * 2, b - a;",
                "RETURN RESULTS TO STORED PROCEDURE" +
                        " INDEX SCAN of \"T1\" using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" (for deterministic order only)" +
                        " inline Hash AGGREGATION ops: SUM(T1.A1), MIN(T1.B1), COUNT(*)");
    }

    public void testNestedSelQuery() {  // test SELECT stmt matching & rewriting inside sub-query
        assertMatch("SELECT a_times_2_plus_a1 + min_b1 - sum_a1 subquery_expr FROM (\n" +
                        " SELECT b - a b_minus_a, a * 2 + a1 a_times_2_plus_a1, SUM(a1) sum_a1, MIN(b1) min_b1, COUNT(*) FROM t1 \n" +
                        "    GROUP BY a * 2 + a1, b - a) AS foo",
                "RETURN RESULTS TO STORED PROCEDURE SEQUENTIAL SCAN of \"FOO\" " +
                        "INDEX SCAN of \"V4\" using its primary key index (for deterministic order only)");
    }

    public void testPartitionedTables() {
        assertMatch("SELECT c0 distinct_c, SUM(b0) sum_b, COUNT(*) counts_of FROM t2 GROUP BY c0",
                explainSimpleMVScan("VT2"));
        assertMatch("SELECT b0, SUM(c0), COUNT(*) FROM t2 GROUP BY b0",
                explainSimpleMVScan("VT2_1"));
    }

    public void testStoredProcedures() {
        // For store procedures with parameters, views will never match on aggregations/filters
        assertMatch("SELECT distinct a1 distinct_a1, COUNT(b1) count_b1, SUM(a) sum_a, COUNT(*) counts " +
                "FROM t1 WHERE b >= ? OR b1 IN (3, 30, 300) GROUP BY a1",
                "RETURN RESULTS TO STORED PROCEDURE INDEX SCAN of \"T1\" using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" " +
                        "(for deterministic order only) filter by ((B >= ?0) OR (B1 IN ANY (3, 30, 300))) " +
                        "inline Hash AGGREGATION ops: COUNT(T1.B1), SUM(T1.A), COUNT(*)");
        // But could match when parameter is somewhere else.
        assertMatch("SELECT distinct a1 distinct_a1, COUNT(b1) count_b1, SUM(a) sum_a, COUNT(*) counts " +
                "FROM t1 WHERE b >= 2 OR b1 IN (3, 30, 300) GROUP BY a1 LIMIT ? OFFSET ?",
                "RETURN RESULTS TO STORED PROCEDURE INDEX SCAN of \"V5_1\" " +
                        "using its primary key index (for deterministic order only) inline LIMIT with parameter");
    }
}
