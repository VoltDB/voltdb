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

package org.voltdb.planner;
import java.util.List;
import org.voltdb.plannodes.AbstractPlanNode;

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

    private String explain(final String query) {        // normalize white spaces into single white space, and delete new lines
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
        checkQueriesPlansAreTheSame("SELECT MIN(b) minb, SUM(a) sum_a, COUNT(*) counts FROM t3 WHERE abs(b) > abs(a)",
                "SELECT min_b minb, sum_a, counts FROM v1");
        checkQueriesPlansAreTheSame("SELECT a + a1 aa, ABS(b) abs_b, COUNT(*) FROM t1 WHERE a1 in (0, a, b, 1, b) GROUP BY ABS(b), a + a1",
                "SELECT aas aa, abs_b, counts C2 FROM v2_1");
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
        // Negative tests: GBY column mismatch -- V2_0 from testplans-materialized-view-optimization-ddl.sql gby column a1, not a
        assertMatch("SELECT a, ABS(b), COUNT(*) FROM t1 WHERE b > 2 GROUP BY ABS(b), a;",
                "RETURN RESULTS TO STORED PROCEDURE INDEX SCAN of \"T1\" using \"TA\" " +
                "(for optimized grouping only) filter by (B > 2) inline Partial AGGREGATION ops: COUNT(*)");
        // Negative tests: with LIMIT or OFFSET - ENG-14415
        assertMatch("SELECT a1 a1, COUNT(b) count_b, SUM(a) sum_a, COUNT(*) FROM t1 GROUP BY a1 LIMIT 2",
                "RETURN RESULTS TO STORED PROCEDURE LIMIT 2 " +
                        "INDEX SCAN of \"T1\" using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" (for deterministic order only) " +
                        "inline Hash AGGREGATION ops: COUNT(T1.B), SUM(T1.A), COUNT(*)");
        assertMatch("SELECT a1 a1, COUNT(b) count_b, SUM(a) sum_a, COUNT(*) FROM t1 GROUP BY a1 OFFSET 5",
                "RETURN RESULTS TO STORED PROCEDURE OFFSET 5 " +
                        "INDEX SCAN of \"T1\" using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" (for deterministic order only) " +
                        "inline Hash AGGREGATION ops: COUNT(T1.B), SUM(T1.A), COUNT(*)");
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
        checkQueriesPlansAreTheSame("SELECT MIN(B1) FROM T1 GROUP BY A, B", "SELECT min_b1 from v3");
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
        // functions in filter
        checkQueriesPlansAreTheSame("SELECT COUNT(a1), SUM(a) FROM t1 WHERE a + b1 > a1 OR power(a, a1) + b1 <= mod(a1, b) + abs(b) GROUP BY a1",
                "SELECT count_a1, sum_a FROM v5_3");
        // function order for power() changed
        checkQueriesPlansAreDifferent("SELECT COUNT(a1), SUM(a) FROM t1 WHERE a + b1 > a1 OR power(a1, a) + b1 <= mod(a1, b) + abs(b) GROUP BY a1",
                "SELECT count_a1, sum_a FROM v5_3", "Function POWER's args changed");
    }

    public void testPlanCaching() { // Test that constants in filters of ad hoc queries don't get cached incorrectly
        explain("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= 4 / 2 OR b1 in (300, 30, 300, 3) GROUP BY a1");
        explain("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= cast(power(2, 1) as int) OR b1 in (300, 30, 300, 3) GROUP BY a1");
        checkQueriesPlansAreTheSame("SELECT SUM(a), COUNT(b1) FROM t1 WHERE 200 / 100 <= b OR b1 in (3, 30, 300, 300) GROUP BY a1",
                "SELECT sum_a, count_b1 FROM v5_1");
        checkQueriesPlansAreTheSame("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= 2 OR b1 in (300, 30, 300, 3) GROUP BY a1",
                "SELECT sum_a, count_b1 FROM v5_1");
        // const expressions are evaluated when no function
        checkQueriesPlansAreTheSame("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= 4 / 2 OR b1 in (300, 30, 300, 3) GROUP BY a1",
                "SELECT sum_a, count_b1 FROM v5_1");
        // and expressions with functions are not evaluated
        checkQueriesPlansAreDifferent("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= cast(power(2, 1) as int) OR b1 in (300, 30, 300, 3) GROUP BY a1",
                "SELECT sum_a, count_b1 FROM v5_1",
                "Constant expression with functions are not evaluated: should not match view");
        // Change in PVE value -- shouldn't match
        checkQueriesPlansAreDifferent("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= 4 OR b1 in (3, 30, 303) GROUP BY a1",
                "SELECT sum_a, count_b1 FROM v5_1", "filter PVE value different");
        // Change in vector value expression  -- shouldn't match
        checkQueriesPlansAreDifferent("SELECT SUM(a), COUNT(b1) FROM t1 WHERE b >= 2 OR b1 in (3, 30, 303) GROUP BY a1",
                "SELECT sum_a, count_b1 FROM v5_1", "filter vector value expression different");
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
        checkQueriesPlansAreDifferent("SELECT a1 + a * 2, b - a, SUM(a1), MIN(b1), COUNT(*) FROM t1 GROUP BY a1 + a * 2, b - a;",
                "SELECT a2pa1, b_minus_a, sum_a1, min_b1, counts FROM v4",
                "GBY expression equivalent but different: should not match view");
        // negative tests: SELECT stmt's display column contains aggregates on its group by column
        // Future improvements should rewrite the expression to match MV.
        assertMatch("SELECT ABS(WAGE), COUNT(*), MAX(ID), SUM(RENT), MIN(AGE),  COUNT(DEPT) FROM P2 M02  GROUP BY WAGE",
                "RETURN RESULTS TO STORED PROCEDURE INDEX SCAN of \"P2 (M02)\" " +
                        "using its primary key index (for deterministic order only) inline Hash AGGREGATION " +
                        "ops: COUNT(*), MAX(M02.ID), SUM(M02.RENT), MIN(M02.AGE), COUNT(M02.DEPT)");
        assertMatch("SELECT ABS(WAGE), COUNT(*), MIN(AGE),    SUM(RENT), MAX(ID) FROM R2 M10  GROUP BY DEPT,  WAGE",
                "RETURN RESULTS TO STORED PROCEDURE INDEX SCAN of \"R2 (M10)\" " +
                        "using its primary key index (for deterministic order only) inline Hash AGGREGATION " +
                        "ops: COUNT(*), MIN(M10.AGE), SUM(M10.RENT), MAX(M10.ID)");
    }

    public void testNestedSelQuery() {  // test SELECT stmt matching & rewriting inside sub-query
        assertMatch("SELECT a_times_2_plus_a1 + min_b1 - sum_a1 subquery_expr FROM (\n" +
                        " SELECT b - a b_minus_a, a * 2 + a1 a_times_2_plus_a1, SUM(a1) sum_a1, MIN(b1) min_b1, COUNT(*) FROM t1 \n" +
                        "    GROUP BY a * 2 + a1, b - a) AS foo",
                "RETURN RESULTS TO STORED PROCEDURE SEQUENTIAL SCAN of \"FOO\" " +
                        "INDEX SCAN of \"V4\" using its primary key index (for deterministic order only)");
        // NOTE: right now the query plan is not optimal, missing opportunity to use index lookup inside nested query
        assertMatch("SELECT * FROM (\n" +
                        " SELECT distinct a1 distinct_a, COUNT(*) count_of FROM t1 WHERE b > 2 GROUP BY a1) foo\n" +
                        "INNER JOIN (SELECT a1 distinct_a, COUNT(*) count_of FROM t1 WHERE b >= 2 OR b1 in (3, 30, 300) GROUP BY a1) bar\n" +
                        "  ON foo.distinct_a = bar.distinct_a \n" +
                        "LEFT JOIN (SELECT a1 distinct_a, COUNT(*) count_of FROM t1 WHERE b < 200 GROUP BY a1) baz\n" +
                        "  ON bar.distinct_a = baz.distinct_a",
                "RETURN RESULTS TO STORED PROCEDURE NEST LOOP LEFT JOIN filter by (BAZ.DISTINCT_A = BAR.DISTINCT_A) " +
                        "NEST LOOP INNER JOIN filter by (BAR.DISTINCT_A = FOO.DISTINCT_A) SEQUENTIAL SCAN of \"FOO\" " +
                        "INDEX SCAN of \"V2\" using its primary key index (for deterministic order only) " +
                        "SEQUENTIAL SCAN of \"BAR\" INDEX SCAN of \"V5_1\" using its primary key index (for deterministic order only) " +
                        "SEQUENTIAL SCAN of \"BAZ\" INDEX SCAN of \"T1\" using \"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" (for deterministic order only) " +
                        "filter by (B < 200) inline Hash AGGREGATION ops: COUNT(*)");
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
                "RETURN RESULTS TO STORED PROCEDURE LIMIT with parameter INDEX SCAN of \"T1\" using " +
                        "\"VOLTDB_AUTOGEN_IDX_CT_T1_B1\" (for deterministic order only) filter by ((B >= 2) OR " +
                        "(B1 IN ANY (3, 30, 300))) inline Hash AGGREGATION ops: COUNT(T1.B1), SUM(T1.A), COUNT(*)");
    }

    public void testUnionStmt() {
        checkQueriesPlansAreTheSame(
                "(SELECT a1, SUM(a) FROM t1 as FOO where b >= 2 or b1 in (3, 300, 30) group by a1) UNION ALL (SELECT a1, SUM(a) from t3 group by a1)",
                "(SELECT distinct_a1, sum_a FROM v5_1) UNION ALL (SELECT a1, SUM(a) from t3 group by a1)");
        // nested union stmt
        checkQueriesPlansAreTheSame(
                "(SELECT SUM(a) sum_a, COUNT(b) count_b FROM t1 as FOO GROUP BY a1) INTERSECT (\n" +
                        " (SELECT SUM(a) sum_a, COUNT(b) count_b FROM t3 WHERE abs(b) > abs(a) GROUP BY a1) UNION \n" +
                        "   (SELECT SUM(a) sum_a, COUNT(b) count_b FROM t3 GROUP BY a1))",
                "(SELECT sum_a00, count_b00 FROM v5_2) INTERSECT (\n" +
                        " (SELECT sum_a, count_b FROM vt3) UNION (SELECT SUM(a) sum_a, COUNT(b) count_b FROM t3 GROUP BY a1))");
        checkQueriesPlansAreTheSame(
                "(SELECT SUM(a) sum_a, COUNT(b) count_b FROM t1 as FOO GROUP BY a1) INTERSECT (\n" +
                        " (SELECT SUM(a) sum_a, COUNT(b) count_b FROM t3 WHERE abs(b) > abs(a) GROUP BY a1) UNION \n" +
                        "   (SELECT SUM(a) sum_a, COUNT(b) count_b FROM t3 WHERE abs(b) > abs(a) GROUP BY a1))",
                "(SELECT sum_a00, count_b00 FROM v5_2) INTERSECT ((SELECT sum_a, count_b FROM vt3) UNION (SELECT sum_a, count_b FROM vt3))");
    }
}
