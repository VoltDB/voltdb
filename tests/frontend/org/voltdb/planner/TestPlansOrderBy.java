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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class TestPlansOrderBy extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansGroupBy.class.getResource("testplans-orderby-ddl.sql"),
                    "testplansorderby", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void validatePlan(String sql, boolean expectIndexScan,
            boolean expectSeqScan, boolean expectOrderBy)
    {
        validatePlan(sql, expectIndexScan, expectSeqScan, expectOrderBy, false, false);
    }

    private void validatePlan(String sql, boolean expectIndexScan,
            boolean expectSeqScan, boolean expectOrderBy, boolean expectHashAggregate,
            boolean expectedAggregate)
    {
        AbstractPlanNode pn = compile(sql);
        //* to debug */ System.out.println(pn.getChild(0).toJSONString());
        //* to debug */ System.out.println(pn.getChild(0).toExplainPlanString());
        assertEquals(expectIndexScan, pn.hasAnyNodeOfType(PlanNodeType.INDEXSCAN));
        assertEquals(expectSeqScan, pn.hasAnyNodeOfType(PlanNodeType.SEQSCAN));
        assertEquals(expectOrderBy, pn.hasAnyNodeOfType(PlanNodeType.ORDERBY));
        assertEquals(expectHashAggregate, pn.hasAnyNodeOfType(PlanNodeType.HASHAGGREGATE));
        assertEquals(expectedAggregate, pn.hasAnyNodeOfType(PlanNodeType.AGGREGATE));
    }

    private void validateMultiPartitionedPlan(String sql,
            boolean expectTopOrderBy, boolean expectTopHashAggregate, boolean expectedTopAggregate,
            boolean expectIndexScan, boolean expectSeqScan, boolean expectOrderBy, boolean expectHashAggregate,
            boolean expectedAggregate)
    {
        List<AbstractPlanNode> frags = compileToFragments(sql);
        assertEquals(2, frags.size());
        AbstractPlanNode pn = frags.get(0);
        assertEquals(expectTopOrderBy, pn.hasAnyNodeOfType(PlanNodeType.ORDERBY));
        assertEquals(expectTopHashAggregate, pn.hasAnyNodeOfType(PlanNodeType.HASHAGGREGATE));
        assertEquals(expectedTopAggregate, pn.hasAnyNodeOfType(PlanNodeType.AGGREGATE));

        pn = frags.get(1);
        assertEquals(expectIndexScan, pn.hasAnyNodeOfType(PlanNodeType.INDEXSCAN));
        assertEquals(expectSeqScan, pn.hasAnyNodeOfType(PlanNodeType.SEQSCAN));
        assertEquals(expectOrderBy, pn.hasAnyNodeOfType(PlanNodeType.ORDERBY));
        assertEquals(expectHashAggregate, pn.hasAnyNodeOfType(PlanNodeType.HASHAGGREGATE));
        assertEquals(expectedAggregate, pn.hasAnyNodeOfType(PlanNodeType.AGGREGATE));
    }

    /// Validate that a plan uses the full bag of tricks
    /// -- that it uses an index scan but no seq scan, order by, or hash aggregate.
    private void validateOptimalPlan(String sql)
    {
        validatePlan(sql, true, false, false);
    }

    /// Validate that a plan does not unintentionally use
    /// a completely inapplicable index scan for order.
    private void validateBruteForcePlan(String sql)
    {
        validatePlan(sql, false, true, true);
    }

    /// Validate that a plan does not unintentionally use
    /// a completely inapplicable index scan for order,
    /// when it still uses an index scan for non-determinism.
    /// This arguably should not happen for plans where the final
    /// sort already confers determinism (yet it currently does)?.
    private void validateIndexedBruteForcePlan(String sql)
    {
        validatePlan(sql, true, false, true);
    }

    public void testOrderByOneOfTwoIndexKeys()
    {
        validateOptimalPlan("SELECT * from T ORDER BY T_D0");
        validateOptimalPlan("SELECT * from T WHERE T_D0 = 1 ORDER BY T_D1");
    }

    public void testOrderByIndexedColumns() {
        validateOptimalPlan("SELECT * from T ORDER BY T_D0, T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys ORDER BY T_D0, T_D1, T_D2");
    }

    public void testOrderByTwoDesc() {
        validateOptimalPlan("SELECT * from T ORDER BY T_D0 DESC, T_D1 DESC");
    }

    public void testOrderByTwoAscDesc() {
        validateBruteForcePlan("SELECT * from T ORDER BY T_D0, T_D1 DESC");
        validateBruteForcePlan("SELECT * from Tnokey ORDER BY T_D0, T_D1 DESC");
    }

    public void testOrderByTwoOfThreeIndexKeys()
    {
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D0 = ? ORDER BY T_D1, T_D2");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D1 = ? ORDER BY T_D0, T_D2");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D2 = ? ORDER BY T_D0, T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys                ORDER BY T_D0, T_D1");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys WHERE T_D0 <= ? ORDER BY T_D1, T_D2");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys WHERE T_D0 <= ? ORDER BY T_D1 DESC, T_D2 DESC");
    }

    public void testOrderByOneOfThreeIndexKeys()
    {
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D0 = ? AND T_D1 = ? ORDER BY T_D2");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D1 = ? AND T_D2 = ? ORDER BY T_D0");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D2 = ? AND T_D0 = ? ORDER BY T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D0 = ?              ORDER BY T_D1");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D1 = ?              ORDER BY T_D0");
        validateOptimalPlan("SELECT * from Tmanykeys WHERE T_D2 = ?              ORDER BY T_D0");
        validateOptimalPlan("SELECT * from Tmanykeys                             ORDER BY T_D0");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys WHERE T_D0 = ? AND T_D1 < ? ORDER BY T_D2");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys WHERE T_D0 = ? AND T_D1 < ? ORDER BY T_D2 DESC");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys WHERE T_D0 = ? ORDER BY T_D2");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys WHERE T_D0 = ? ORDER BY T_D2 DESC");
    }

    public void testOrderByWrongPermutation()
    {
        // Order determinism, so do not make it worse by using index scan.
        validateBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D2, T_D1, T_D0");
        validateBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D2, T_D0, T_D1");
        validateBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D1, T_D0, T_D2");
        validateBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D1, T_D2, T_D0");
        validateBruteForcePlan("SELECT * from Tmanykeys ORDER BY T_D0, T_D2, T_D1");
        // This is order deterministic.  The primary key is T_D0, T_D1, T_D2. We
        // are ordering by two of these, and the third is constrained to be a constant.
        // So, there can only be one row with three different values for all three
        // columns, and it must be order deterministic.
        validateBruteForcePlan("SELECT * from Tmanykeys WHERE T_D1 = ? ORDER BY T_D2, T_D0");

        // Use index for filter.
        validateIndexedBruteForcePlan("SELECT * from Tmanykeys WHERE T_D0 = ? ORDER BY T_D2, T_D1");
    }

    public void testOrderByTooManyToIndex()
    {
        validateBruteForcePlan("SELECT * from T ORDER BY T_D0, T_D1, T_D2");
        validateBruteForcePlan("SELECT * from Tnokey ORDER BY T_D0, T_D1, T_D2");
    }

    public void testOrderByTooMany()
    {
        validateBruteForcePlan("SELECT * from Tnokey ORDER BY T_D0, T_D1, T_D2");
        validateBruteForcePlan("SELECT * from T ORDER BY T_D0, T_D1, T_D2");
    }

    public void testNoIndexToOrderBy() {
        validateIndexedBruteForcePlan("SELECT * FROM T ORDER BY T_D2");
        validateBruteForcePlan("SELECT * FROM Tnokey ORDER BY T_D2");
        validateIndexedBruteForcePlan("SELECT * FROM Tmanykeys ORDER BY T_D2");
    }

    public void testOrderByNLIJ()
    {
        validatePlan("SELECT Tnokey.T_D1, T.T_D0, T.T_D1 from Tnokey, T " +
                     "where Tnokey.T_D2 = 2 AND T.T_D0 = Tnokey.T_D0 " +
                     "ORDER BY T.T_D0, T.T_D1", true, true, true);
    }

    public void testTableAgg() {
        validatePlan("SELECT SUM(T_D0) from T", false, true, false, false, true);
        validatePlan("SELECT SUM(T_D0), COUNT(*), AVG(T_D1) from T", false, true, false, false, true);

        validatePlan("SELECT SUM(T_D0) from T ORDER BY T_D0, T_D1",
                false, true, false, false, true);
        validatePlan("SELECT SUM(T_D0), COUNT(*), AVG(T_D1) from T ORDER BY T_D0, T_D1",
                false, true, false, false, true);
    }

    public void testOrderByCountStar() {
        validatePlan("SELECT T_D0, COUNT(*) AS FOO FROM T GROUP BY T_D0 ORDER BY FOO", true, false, true, false, true);
        validatePlan("SELECT T_D0, COUNT(*) AS FOO FROM Tnokey GROUP BY T_D0 ORDER BY FOO", false, true, true, true, false);
    }

    public void testOrderByAggWithoutAlias() {
        validatePlan("SELECT T_D0, SUM(T_D1) FROM T GROUP BY T_D0 ORDER BY SUM(T_D1)",
                true, false, true, false, true);
        validatePlan("SELECT T_D0, COUNT(*) FROM T GROUP BY T_D0 ORDER BY COUNT(*)",
                true, false, true, false, true);
        validatePlan("SELECT T_D0, AVG(T_D1) FROM T GROUP BY T_D0 ORDER BY AVG(T_D1)",
                true, false, true, false, true);
        validatePlan("SELECT T_D0, COUNT(T_D1) FROM T GROUP BY T_D0 ORDER BY COUNT(T_D1)",
                true, false, true, false, true);
        validatePlan("SELECT T_D0, MIN(T_D1) FROM T GROUP BY T_D0 ORDER BY MIN(T_D1)",
                true, false, true, false, true);
        validatePlan("SELECT T_D0, MAX(T_D1) FROM T GROUP BY T_D0 ORDER BY MAX(T_D1)",
                true, false, true, false, true);

        // Complex aggregation
        validatePlan("SELECT T_D0, COUNT(*)+1 FROM T GROUP BY T_D0 ORDER BY COUNT(*)+1",
                true, false, true, false, true);
        validatePlan("SELECT T_D0, abs(MAX(T_D1)) FROM T GROUP BY T_D0 ORDER BY abs(MAX(T_D1))",
                true, false, true, false, true);
    }

    public void testEng450()
    {
        // This used to not compile. It does now. That's all we care about.
        compile("select T.T_D0, " +
                     "sum(T.T_D1) " +
                     "from T " +
                     "group by T.T_D0 " +
                     "order by T.T_D0;");
    }

    public void testOrderByBooleanConstants()
    {
        String[] conditions = {"1=1", "1=0", "TRUE", "FALSE", "1>2"};
        for (String condition : conditions) {
            failToCompile(String.format("SELECT * FROM T WHERE T_D0 = 2 ORDER BY %s", condition),
                          "invalid ORDER BY expression");
        }
    }

    public void testOrderDescWithEquality() {
        validateOptimalPlan("SELECT * FROM T WHERE T_D0 = 2 ORDER BY T_D1");
        // See ENG-5084 to optimize this query to use inverse scan in future.
        validateIndexedBruteForcePlan("SELECT * FROM T WHERE T_D0 = 2 ORDER BY T_D1 DESC");
        //validateOptimalPlan("SELECT * FROM T WHERE T_D0 = 2 ORDER BY T_D1 DESC");
    }

    // Indexes on T (T_D0, T_D1), T2 (T_D0, T_D1), and Tmanykeys (T_D0, T_D1, T_D2)
    // no index on Tnokey and Tnokey2
    public void testENG4676() {
        // single column ORDER BY
        // ORDER BY on indexed key ascending, JOIN on indexed key from one table and
        // unindexed key from the other table -> no ORDER BY node
        validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D0 = T.T_D2 " +
                "ORDER BY T.T_D0 LIMIT ?");
        // ORDER BY on indexed key descending, JOIN on indexed key from one table
        // and unindexed key from the other table -> no ORDER BY node
        validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D0 = T.T_D2 " +
                "ORDER BY T.T_D0 DESC LIMIT ?");

        // multiple columns ORDER BY
        // ORDER BY on indexed key ascending, JOIN on indexed key from one table and
        // unindexed key from the other table -> no ORDER BY node
        validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D0 = T.T_D2 " +
                "ORDER BY T.T_D0, T.T_D1 LIMIT ?");
        // ORDER BY on indexed key descending, JOIN on indexed key from one table and
        // unindexed key from the other table -> no ORDER BY node
        validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D0 = T.T_D2 " +
                "ORDER BY T.T_D0 DESC, T.T_D1 DESC LIMIT ?");

        // filter on indexed column on one table, prefix join constraint,
        // ORDER BY looking for 1 recovered spoiler -> no ORDER BY node
        validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D0 = T.T_D2 AND Tmanykeys.T_D0 = ?  " +
                "ORDER BY Tmanykeys.T_D1 LIMIT ?");
        // This query requires additional recognition of transitive equality to eliminate the ORDER BY.
        // See ENG-4728.
        //*See ENG-4728.*/ validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D0 = T.T_D2 AND T.T_D2 = ?  " +
        //*See ENG-4728.*/        "ORDER BY Tmanykeys.T_D1 LIMIT ?");
        // ORDER BY is not recovered, but index is chosen for sorting purpose --> no ORDER BY node
        validateOptimalPlan("SELECT * FROM T, Tmanykeys WHERE Tmanykeys.T_D1 = T.T_D2 AND T.T_D0 = ?  " +
                "ORDER BY Tmanykeys.T_D0 LIMIT ?");

        // test NLJ --> need ORDER BY node
        validateBruteForcePlan("SELECT * FROM Tnokey, Tnokey2 WHERE Tnokey.T_D0 = Tnokey2.T_D0 " +
                "ORDER BY Tnokey.T_D1 LIMIT ?");

        // test nested NLIJ
        validateIndexedBruteForcePlan("SELECT * FROM T, T2, Tmanykeys " +
                "WHERE T.T_D0 = T2.T_D0 AND T2.T_D1 = Tmanykeys.T_D0 ORDER BY Tmanykeys.T_D1 LIMIT ?");
        validateIndexedBruteForcePlan("SELECT * FROM T, T2, Tmanykeys " +
                "WHERE T.T_D0 = T2.T_D0 AND T2.T_D1 = Tmanykeys.T_D0 ORDER BY T.T_D1 LIMIT ?");
        validateIndexedBruteForcePlan("SELECT * FROM T, T2, Tmanykeys " +
                "WHERE T.T_D0 = T2.T_D0 AND T2.T_D1 = Tmanykeys.T_D0 ORDER BY T2.T_D1 LIMIT ?");
        validateOptimalPlan("SELECT * FROM T, T2, Tmanykeys " +
                "WHERE T.T_D0 = T2.T_D0 AND T2.T_D1 = Tmanykeys.T_D0 ORDER BY T.T_D0, T.T_D1 LIMIT ?");
    }

    /**
     * Order by clause can only operate on the display columns list when having DISTINCT or GROUP BY clause.
     * However, it can operate on other columns or expressions on the table or joined table.
     */
    public void testOrderbyColumnsNotInDisplayList() {
        compile("select T.T_D0 from T order by T.T_D1;");
        compile("select T.T_D0 from T, Tmanykeys where Tmanykeys.T_D0 = T.T_D2 order by T.T_D1;");

        // DISTINCT
        failToCompile("select DISTINCT T.T_D0 from T order by T.T_D1;", "invalid ORDER BY expression");
        // GROUP BY
        failToCompile("select T.T_D0, count(*) from T group by T.T_D0 order by T.T_D1;",
                "expression not in aggregate or GROUP BY columns: ORDER BY ");

        // Very edge case:
        // Order by GROUP BY columns or expressions which are not in display list
        compile("select count(*) from T group by T.T_D0 order by T.T_D0;");
        compile("select count(*) from T group by T.T_D0 order by ABS(T.T_D0);");

        compile("select count(*) from T group by ABS(T.T_D0) order by ABS(T.T_D0);");
    }

    public void testOrderbyWithPartialIndex() {
        // Partial index T3_PARTIAL_IDX on T3 (T_D1) WHERE T_D2 > 3 is not picked up
        validatePlan("select T3.T_D0 from T3 order by T3.T_D1;", false, true, true);
        // Partial index T3_PARTIAL_IDX on T3 (T_D1) WHERE T_D2 > 3 is used now
        validatePlan("select T3.T_D0 from T3 where T_D2 > 3 order by T3.T_D1;", true, false, false);
        // Partial index T3_PARTIAL_IDX on T3 (T_D1) WHERE T_D2 > 3 is still used but now ORDER BY is required
        validatePlan("select T3.T_D0 from T3 where T_D2 > 3 order by T3.T_D2;", true, false, true);
    }

    public void testOrderByMPOptimized() {
        {
            // P_D1_IDX index provides the right order for the coordinator. Merge Receive
            validateMergeReceive("select P_D1, P_D1 + 1 from P order by P_D1",
                    false, false, new int[] {0});
        }
        {
            // P_D32_IDX index provides the right order for the coordinator. Merge Receive
            validateMergeReceive("select P_D1 from P order by P_D3 DESC, P_D2 DESC",
                    false, false, new int[] {2, 1});
        }
        {
            // P_D1_IDX index provides the right order for the coordinator. Merge Receive
            validateMergeReceive("select P_D1 from P order by P_D1", false, false, new int[] {0});
        }
        {
            // P_D1_IDX index provides the right order for the coordinator. Merge Receive with LIMIT
            validateMergeReceive("select P_D1 from P order by P_D1 limit 3 offset 1", false, true, new int[] {0});
        }
        {
            // Partitions results are ordered by the pushed down ORDER BY.
            // Merge Receive with Order By and with LIMIT
            validateMergeReceive("select P_D1, P_D2  from P order by P_D2 limit 3", false, true, new int[] {1});
        }
        {
            // NLIJ with index outer table scan (PK). ORDER BY column T.T_D1 is not the first index column
            // Merge Receive is possible because of the trivial coordinator
            validateMergeReceive("select P_D1 from P, T where P.P_D1 = T.T_D2 and T.T_D0 = 2 order by T.T_D1",
                    false, false, new int[] {1});
        }
        {
            // NLIJ with index outer table scan (PK). ORDER BY column T.T_D0 is the first index column
            // Merge RECEIVE
            validateMergeReceive("select P_D1 from P, T where P.P_D1 = T.T_D2 and T.T_D0 = 2 order by T.T_D0 limit 3",
                    false, true, new int[] {0});
        }
        {
            // Index P_D32_10_IDX ON P (P_D3 / 10, P_D2) covers ORDER BY expressions (P_D3 / 10, P_D2)
            // Merge Receive
            validateMergeReceive("select P_D1 from P where P.P_D3 / 10 > 0 order by P_D3 / 10, P_D2",
                    false, false, new int[] {2, 1});
        }
    }

    public void testOrderByMPTrivialCoordinatorOptimized() {
        // Though the partitions's output is not properly ordered the MERGE_RECEIVE optimization
        // is still possible because of a trivial coordinator fragment allowing for pushing
        // the coordinator's ORDER BY down to partitions
        {
            // P_D2 column is not covered by P_D1_IDX index. ORDER BY
            validateMergeReceive("select P_D1 from P where P_D1 > 1 order by abs(P_D3), P_D2",
                    true, false, new int[] {2, 1});
        }
        {
            // P_D32_IDX (P_D3,P_D2) Index scan. Sort order mismatch
            validateMergeReceive("select P_D1 from P order by P_D3 ASC, P_D2 DESC",
                    true, false, new int[] {2, 1});
        }
        {
            // P_D32_IDX (P_D3,P_D2) Index scan. Sort order mismatch
            validateMergeReceive("select P_D1 from P order by P_D3 DESC, P_D2",
                    true, false, new  int[] {2, 1});
        }
        {
            // P_D32_IDX (P_D3,P_D2) Index scan. Column order in ORDER BY mismatch
            validateMergeReceive("select P_D1 from P order by P_D2, P_D3",
                    true, false, new int[] {1, 2});
        }
        {
            // P_D1 column is not covered by the P_D32_IDX
            validateMergeReceive("select P_D1 from P where P_D3 > 1 order by P_D3 DESC, P_D1",
                    true, false, new int[] {1, 0});
        }
        {
            // Partitions results are unordered. Sequential scan
            validateMergeReceive("select P_D1, P_D2 from P order by P_D2",
                    true, false, new int[] {1});
        }
        {
            // NLIJ with sequential outer table scan (T). ORDER BY
            validateMergeReceive("select P_D1 from P, T where P.P_D1 = T.T_D2 order by P.P_D1",
                    true, false, new int[] {1});
        }
        {
            // NLIJ with index (PK) outer table scan (T).
            // ORDER BY column P.P_D1 is not from a leftmost table
            validateMergeReceive("select P_D1 from P, T where P.P_D1 = T.T_D2 and T.T_D0 = 2 order by P.P_D1",
                    true, false, new int[] {2});
        }
        {
            // NLIJ with index (PK(T_D0, T_D1)) outer table scan (T).
            // ORDER BY column T.T_D1 is not the first index column
            validateMergeReceive("select P_D1 from P, T where P.P_D1 = T.T_D2 and T.T_D0 = 2 order by T.T_D1",
                    true, false, new int[] {1});
        }
        {
            // Index P_D32_10_IDX ON P (P_D3 / 10, P_D2) does not cover ORDER BY expressions (P_D3 / 5, P_D2)
            validateMergeReceive("select P_D1 from P where P.P_D3 / 10 > 0 order by P_D2 / P_D3, P_D2",
                    true, false, new int[] {1, 2, 1});
        }
    }

    public void testOrderByMPNonOptimized() {
        {
            // NLJ - a replicated table is on the "outer" side of an outer join with a partitioned table.
            // The optimization is rejected because of the coordinator NLJ node is a child of the ORDER BY node.
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D1 from T left join P on P.P_D1 = T.T_D0 order by T.T_D2 limit 3");
            assertEquals(2, frags.size());
            AbstractPlanNode pn = frags.get(0).getChild(0).getChild(0);
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }
        {
            // The Merge Receive optimization is not possible because the ORDER BY node has inline
            // OFFSET node that can't be pushed down
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select * from P order by P_D2 offset 3");
            assertEquals(2, frags.size());
            AbstractPlanNode pn = frags.get(0).getChild(0);
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }

    }

    public void testOrderByMPSubquery() {
        {
            // Select from an ordered subquery. The subquery SEND/MERGERECEIVE is always preserved
            // during the subquery post-processing that would remove the SEND/RECEIVE pair
            // from this subquery without the MERGE-RECEIVE optimization.
            // Removing the subquery MERGERECEIVE node together with its inline ORDER BY node
            // would result in the invalid plan.
            // The subquery LIMIT clause is required to suppress the
            // optimization that replaces the subquery with a simple select.
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select PT_D1 from (select P_D1 as PT_D1 from P order by P_D1 limit 30) P_T limit 4;");

            assertEquals(2, frags.size());
            AbstractPlanNode coordinatorPn = frags.get(0).getChild(0);
            assertEquals(PlanNodeType.SEQSCAN, coordinatorPn.getPlanNodeType());
            assertEquals("P_T", ((SeqScanPlanNode) coordinatorPn).getTargetTableAlias());
            // Subquery
            coordinatorPn = coordinatorPn.getChild(0);
            if (coordinatorPn instanceof ProjectionPlanNode) {
                coordinatorPn = coordinatorPn.getChild(0);
            }
            validateMergeReceive(coordinatorPn, true, new int[] {0});
            AbstractPlanNode partitionPn = frags.get(1).getChild(0);
            assertEquals(PlanNodeType.INDEXSCAN, partitionPn.getPlanNodeType());
            assertEquals("P", ((IndexScanPlanNode) partitionPn).getTargetTableAlias());
        }
        {
            // The subquery SEND/MERGERECEIVE is preserved during the subquery post-processing
            // resulting in the multi-partitioned join that gets rejected.
            // In this case, the subquery MERGERECEIVE node is technically redundant but at the moment,
            // the subquery post-processing still keeps it.
            // The subquery LIMIT clause is required to suppress the
            // optimization that replaces the subquery with a simple select.
            failToCompile(
                    "select PT_D1 from (select P_D1 as PT_D1, P_D0 as PT_D0 from P order by P_D1 limit 10) P_T, P where P.P_D0 = P_T.PT_D0;",
                    "This query is not plannable.  It has a subquery which needs cross-partition access.");

        }
        {
            // The subquery with partition column (P_D0) in the GROUP BY columns.
            // The subquery SEND/MERGERECEIVE is preserved even though without the MERGE-RECEIVE optimization
            // the SEND/RECEIVE pair would be removed
            List<AbstractPlanNode> frags =  compileToFragments(
                "select PT_D1, MP_D3 from (select P_D0  as PT_D0, P_D1 as PT_D1, max(P_D3) as MP_D3 from P group by P_D0, P_D1 order by P_D0) P_T");

            assertEquals(2, frags.size());
            AbstractPlanNode pn = frags.get(0).getChild(0);
            assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
            assertEquals("P_T", ((SeqScanPlanNode) pn).getTargetTableAlias());
            // Subquery
            pn = pn.getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateMergeReceive(pn, false, new int[] {0});
            pn = frags.get(1).getChild(0);
            assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
            assertEquals("P", ((IndexScanPlanNode) pn).getTargetTableAlias());
        }
    }

    public void testOrderByMPAggregateOptimized() {
        {
            // Select from the aggregate view
            List<AbstractPlanNode> frags =  compileToFragments("Select * from V_P1 ORDER BY V_G1, V_G2");
            AbstractPlanNode pn = frags.get(0).getChild(0).getChild(0);
            validateAggregatedMergeReceive(pn, true, false, false, false);
        }
        {
            // Merge Receive with Serial aggregation
            //            select indexed_non_partition_key(P_D1), max(col)
            //            from partitioned
            //            group by indexed_non_partition_key (P_D1)
            //            order by indexed_non_partition_key (P_D1);"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D1, max(P_D2) from P where P_D1 > 0 group by P_D1 order by P_D1");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, true, false, false, false);
            // Make sure partition index scan is a range scan
            pn = frags.get(1).getChild(0);
            IndexScanPlanNode ipn = (IndexScanPlanNode)pn;
            assertTrue(ipn.getPredicate() == null);;
        }
        {
            // Merge Receive with Serial aggregation
            //            select indexed_non_partition_key(P_D3, P_D2), col, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key (P_D3, P_D2)
            //            order by indexed_non_partition_key(P_D3, P_D2);"
            List<AbstractPlanNode> frags =  compileToFragments(
                  "select P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D2 order by P_D3, P_D2");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, true, false, false, false);
        }
        {
            // Merge Receive with Serial aggregation
            //            select indexed_non_partition_key(P_D3, P_D2), col, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key (P_D2, P_D3) permutation
            //            order by indexed_non_partition_key(P_D3, P_D2);"
            List<AbstractPlanNode> frags =  compileToFragments(
                  "select P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D2, P_D3 order by P_D3, P_D2");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, true, false, false, false);
        }
        {
            // Merge Receive with Partial aggregation
            //            select indexed_non_partition_key(P_D1), col, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key(P_D1), col
            //            order by indexed_non_partition_key(P_D1);"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D1, P_D2, max(P_D2) from P group by P_D1, P_D2 order by P_D1");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, false, true, false, false);
        }
        {
            // Merge Receive with Partial aggregation
            //            select indexed_non_partition_key(P_D3, P_D2), col, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key , col (permutation)
            //            order by indexed_non_partition_key(P_D3, P_D2);"
            List<AbstractPlanNode> frags =  compileToFragments(
                  "select P_D1, P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D2, P_D1, P_D3 order by P_D3, P_D2");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, false, true, false, false);
        }
        {
            // Merge Receive with Partial aggregation
            //            select indexed_non_partition_key(P_D3, P_D2), col, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key , col (permutation)
            //            order by part of indexed_non_partition_key (P_D3);"
            List<AbstractPlanNode> frags =  compileToFragments(
                  "select P_D1, P_D3, P_D2, max (P_D0) from p where P_D3 > 0 group by P_D2, P_D1, P_D3 order by P_D3");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, false, true, false, false);
        }
        {
            // No aggregation at coordinator
            //          select indexed_partition_key(P_D0), max(col)
            //          from partitioned
            //          group by indexed_partition_key(P_D0)
            //          order by indexed_partition_key(P_D0);"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select max(P_D2), P_D0 from P group by P_D0  order by P_D0");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, false, false, false, false);
        }
        {
            // No aggregation at coordinator
            //          select non_partition_col(P_D1), max(col)
            //          from partitioned
            //          group by non_partition_col, indexed_partition_key(P_D0)
            //          order by indexed_partition_key(P_D0);"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select max(P_D2), P_D1 from P group by P_D1, P_D0 order by P_D0");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, false, false, false, false);
        }
        {
            // Merge Receive without aggregation at coordinator because of
            // the partition column (P_D0) being part of the GROUP BY/ORDER BY columns
            //            select indexed_non_partition_key1, indexed_non_partition_key2, partition_col, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key1, indexed_non_partition_key2, partition_col
            //            order by indexed_non_partition_key1, indexed_non_partition_key2;"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D3, P_D2, P_D0, max (P_D1) from p where P_D3 > 0 group by P_D3, P_D2, P_D0 order by P_D3, P_D2");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, false, false, false, false);
        }
        {
            // Two aggregation but a single aggregate node at the coordinator.
            //            select aggr1(col), aggr2(col), indexed_non_partition_key(P_D1)
            //            from partitioned
            //            group by indexed_non_partition_key
            //            order by indexed_non_partition_key;"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "SELECT min(P_D2), max(P_D2), P_D1 from P where P_D1 > 0 group by P_D1 order by P_D1;");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            validateAggregatedMergeReceive(pn, true, false, false, false);
        }
    }

    public void testOrderByMPAggregateNonOptimized() {
        {
            // Select from a VIEW. Two aggregate nodes at the coordinator. ORDER BY
            // RETURN RESULTS TO STORED PROCEDURE
            //  ORDER BY (SORT)
            //   Hash AGGREGATION ops: SUM(V_P1_ABS.V_CNT), MAX(V_P1_ABS.V_SUM_AGE)
            //    Hash AGGREGATION ops: SUM(V_P1_ABS.V_CNT), SUM(V_P1_ABS.V_SUM_AGE)
            //     RECEIVE FROM ALL PARTITIONS
            List<AbstractPlanNode> frags =  compileToFragments("Select V_G1, sum(V_CNT), max(v_sum_age) from V_P1_ABS GROUP BY V_G1 ORDER BY V_G1");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }
        {
            // Select from a table. Two hash aggregate nodes at the coordinator. The optimization is impossible for two reasons:
            // - GROUP BY and ORDER BY columns don't overlap preventing the hash aggregate to be converted to a Serial/Partial aggr
            // - Two aggregate nodes
            // The partition output is ordered
            // by index scan (P_D1PLUS3_IDX index)
            // RETURN RESULTS TO STORED PROCEDURE
            //  Hash AGGREGATION ops
            //   ORDER BY (SORT)
            //    Hash AGGREGATION ops: MAX($$_VOLT_TEMP_TABLE_$$.column#0)
            //     RECEIVE FROM ALL PARTITIONS
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select distinct max(P_D2), P_D1 + 3 from P where P_D1 + 3 > 0 group by P_D1 order by P_D1 + 3;");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            assertEquals(PlanNodeType.HASHAGGREGATE, pn.getPlanNodeType());
            // Partition fragment
            pn = frags.get(1).getChild(0);
            assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
            IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
            assertEquals(SortDirectionType.ASC, ipn.getSortDirection());
        }
        {
            // Coordinator HASH aggregate not convertible to Serial or Partial aggregation
            //          select indexed_non_partition_key, max(col)
            //          from partitioned
            //          group by indexed_non_partition_key
            //          order by aggregate;"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D1, max(P_D2) from P group by P_D1 order by max(P_D2)");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }
        {
            // Coordinator fragment with OrderBy, Projection, and Aggregation
            // ORDER BY (SORT)
            //  PROJECTION
            //   Hash AGGREGATION ops: SUM($$_VOLT_TEMP_TABLE_$$.column#0)
            //    RECEIVE FROM ALL PARTITIONS
            // Not supported yet because we do not support inlining a projection before
            // the order by into the MergeReceive AND we don't recognize cases like this
            // where we could defer the projection to be applied after the order by node and
            // so after the mergereceive OR as a final inlined step of the mergereceive.
            //            select indexed_non_partition_key, sum(col), sum(col)+1
            //            from partitioned
            //            group by indexed_non_partition_key
            //            order by indexed_non_partition_key
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D1, sum(P_D1), sum(P_D1)+1 from P group by P_D1 order by P_D1");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }
        {
            // Serial aggregation instead of ORDERING
            //          select max(indexed_partition_key)
            //          from partitioned
            //          order by max(indexed_partition_key);"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select max(P_D1) from P order by max(P_D1)");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            assertEquals(PlanNodeType.AGGREGATE, pn.getPlanNodeType());
        }
        {
            // Sequential scan (P1). Partition results are not sorted
            //          select non_indexed_partition, max(col)
            //          from partitioned
            //          group by non_indexed_partition
            //          order by non_indexed_partition;"
            // Trivial coordinator - MERGE_RECEIVE
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P1_D0, max(P1_D2) from P1 group by P1_D0 order by P1_D0");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            assertEquals(PlanNodeType.MERGERECEIVE, pn.getPlanNodeType());
            assertNotNull(pn.getInlinePlanNode(PlanNodeType.ORDERBY));
        }
        {
            // ORDER BY column P_D1 is not covered by the P_D32_IDX index
            // and its sort direction is invalid. Partition outputs are unordered
            //            select indexed_non_partition_key1, non_indexed_non_partition_col2, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key1, non_indexed_non_partition_col2
            //            order by indexed_non_partition_key1, non_indexed_non_partition_col2;"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D3, P_D1, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D1 order by P_D1, P_D3");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            };
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }
        {
            // ORDER BY column P_D1 is not covered by the P_D32_IDX index
            // and its sort direction is invalid. Partition outputs are unordered
            //            select indexed_non_partition_key1, non_indexed_non_partition_col2, max(col)
            //            from partitioned
            //            group by indexed_non_partition_key1, non_indexed_non_partition_col2
            //            order by indexed_non_partition_key1, non_indexed_non_partition_col2;"
            List<AbstractPlanNode> frags =  compileToFragments(
                    "select P_D3, P_D1, max (P_D0) from p where P_D3 > 0 group by P_D3, P_D1 order by P_D3, P_D1");
            AbstractPlanNode pn = frags.get(0).getChild(0);
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
            assertEquals(PlanNodeType.ORDERBY, pn.getPlanNodeType());
        }
    }

    public void testOrderByFullJoin() {

        // ORDER BY with FULL join still requires an ORDER BY node even if the
        // outer table output is ordered by its index
        validatePlan("SELECT L.T_D0, L.T_D1 FROM T2 L FULL JOIN T2 R ON L.T_D2 = R.T_D2 ORDER BY 1,2",
                true, true, true);

        // Same test but with a distributed table
        validateMergeReceive("SELECT L.P_D1 FROM P L FULL JOIN P R ON L.P_D0 = R.P_D0 ORDER BY 1;",
                false, false, new int[] {1});

        // With aggregate
        validatePlan("SELECT L.T_D0, L.T_D1, SUM(L.T_D2) FROM T2 L FULL JOIN T2 R ON L.T_D2 = R.T_D2 GROUP BY L.T_D0, L.T_D1 ORDER BY 1,2",
                true, true, true, false, true);

        // Partitioned with aggregate
        validateMultiPartitionedPlan("SELECT L.P_D1, SUM(L.P_D2) FROM P L FULL JOIN P R ON L.P_D0 = R.P_D0 GROUP BY L.P_D1 ORDER BY 1;",
            true, true, false, true, false, false, false, true);

    }

    private void validateMergeReceive(String sql, boolean hasPartitionOrderBy, boolean hasLimit, int[] sortColumnIdx) {
        List<AbstractPlanNode> frags =  compileToFragments(sql);
        assertEquals(2, frags.size());
        AbstractPlanNode coordinatorPn = frags.get(0).getChild(0);
        if (coordinatorPn instanceof ProjectionPlanNode) {
            coordinatorPn = coordinatorPn.getChild(0);
        }
        validateMergeReceive(coordinatorPn, hasLimit, sortColumnIdx);
        if (hasPartitionOrderBy) {
            AbstractPlanNode partitionOrderBy = frags.get(1).getChild(0);
            assertEquals(PlanNodeType.ORDERBY, partitionOrderBy.getPlanNodeType());
        }
    }

    private void validateMergeReceive(AbstractPlanNode coordinatorPn, boolean hasLimit, int[] sortColumnIdx) {
        assertEquals(PlanNodeType.MERGERECEIVE, coordinatorPn.getPlanNodeType());
        MergeReceivePlanNode rpn = (MergeReceivePlanNode) coordinatorPn;
        assertNotNull(rpn.getInlinePlanNode(PlanNodeType.ORDERBY));
        assertEquals(hasLimit, rpn.getInlinePlanNode(PlanNodeType.LIMIT) != null);
        OrderByPlanNode opn = (OrderByPlanNode) rpn.getInlinePlanNode(PlanNodeType.ORDERBY);
        List<AbstractExpression> ses = opn.getSortExpressions();
        int idx = 0;
        List<AbstractExpression> sesTves = new ArrayList<>();
        for (AbstractExpression se : ses) {
            sesTves.addAll(se.findAllTupleValueSubexpressions());
        }
        assertEquals(sortColumnIdx.length, sesTves.size());
        for (AbstractExpression seTve : sesTves) {
            assertEquals(sortColumnIdx[idx++], ((TupleValueExpression) seTve).getColumnIndex());
        }
    }

    private void validateAggregatedMergeReceive(AbstractPlanNode pn,
            boolean hasSerialAggr, boolean hasPartialAggr, boolean hasProj, boolean hasLimit) {
        assertEquals(PlanNodeType.MERGERECEIVE, pn.getPlanNodeType());
        MergeReceivePlanNode rpn = (MergeReceivePlanNode) pn;
        assertNotNull(rpn.getInlinePlanNode(PlanNodeType.ORDERBY));
        assertEquals(hasSerialAggr, rpn.getInlinePlanNode(PlanNodeType.AGGREGATE) != null);
        assertEquals(hasPartialAggr, rpn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE) != null);
        assertEquals(hasProj, rpn.getInlinePlanNode(PlanNodeType.PROJECTION) != null);
        if (hasSerialAggr || hasPartialAggr) {
            AbstractPlanNode aggrNode = (hasSerialAggr) ?
                    rpn.getInlinePlanNode(PlanNodeType.AGGREGATE) :
                        rpn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE);
            assertEquals(hasLimit, aggrNode.getInlinePlanNode(PlanNodeType.LIMIT) != null);
        } else {
            assertEquals(hasLimit, rpn.getInlinePlanNode(PlanNodeType.LIMIT) != null);
        }
    }

}
