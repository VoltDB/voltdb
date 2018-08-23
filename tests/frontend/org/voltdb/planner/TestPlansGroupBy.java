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

import java.util.ArrayList;
import java.util.List;

import javassist.expr.Expr;
import org.hsqldb_voltpatches.Expression;
import org.voltdb.RateLimitedClientNotifier;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractReceivePlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanMatcher;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class TestPlansGroupBy extends PlannerTestCase {

    // Set this to true to print the JSON string for the plan.
    private static boolean PRINT_JSON_PLAN = true;
    // Test normal sized temp tables.
    private static boolean TEST_NORMAL_SIZE_QUERIES = false;
    // Test large sized temp tables.
    private static boolean TEST_LARGE_QUERIES = true;

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansGroupBy.class.getResource("testplans-groupby-ddl.sql"),
                "testplansgroupby", false);
        planForLargeQueries(false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * There is no group by here.  So, we have
     * just one big group, and the rows can happen
     * in any order in the one group.  We compute an aggregate
     * in the distributed fragment, and another,
     * possibly different, in
     * the coordinator fragment.  Min, Max and Sum are
     * associative, so we don't care how they are
     * distributed, and the aggregate is the same
     * in both fragments.  Count is count in the distributed
     * fragment and sum in the coordinator fragment.
     *
     * This tests both large temp table and normal temp
     * table queries.
     */
    public void testInlineSerialAgg_noGroupBy() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            // Try the normal sized queries.
            planForLargeQueries(false);
            basicTestInlineSerialAgg_noGroupBy();
        }

        if (TEST_LARGE_QUERIES) {
            // Try large temp table queries.
            planForLargeQueries(true);
            basicTestInlineSerialAgg_noGroupBy();
        }
    }

    private void basicTestInlineSerialAgg_noGroupBy() {
        //
        // Note: All these queries create a two-fragment plan
        //       with a single scan node in the distributed fragment,
        //       and no join nodes.  Each fragment has an aggregate
        //       node.  The check routine will check that the
        //       scan node is what we expect and both aggregates
        //       are the ones we expect.
        //
        //       All these plans are the same, whether we use
        //       large temp tables or not.  Since there are no
        //       group by keys we can always do serial aggregation.
        //
        checkSimpleTableInlineAgg("SELECT SUM(A1) from T1",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM));

        checkSimpleTableInlineAgg("SELECT MIN(A1) from T1",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MIN),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MIN));
        checkSimpleTableInlineAgg("SELECT MAX(A1) from T1",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX));

        checkSimpleTableInlineAgg("SELECT SUM(A1), COUNT(A1) from T1",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM,
                                                           ExpressionType.AGGREGATE_COUNT));

        // There is no index defined on column B3
        checkSimpleTableInlineAgg("SELECT SUM(A3) from T3 WHERE B3 > 3",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM));
        checkSimpleTableInlineAgg("SELECT MIN(A3) from T3 WHERE B3 > 3",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MIN),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MIN));
        checkSimpleTableInlineAgg("SELECT MAX(A3) from T3 WHERE B3 > 3",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX));

        checkSimpleTableInlineAgg("SELECT COUNT(A3) from T3 WHERE B3 > 3",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_COUNT));

        // Index scan
        checkSimpleTableInlineAgg("SELECT SUM(A3) from T3 WHERE PKEY > 3",
                                  PlanNodeType.INDEXSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM));
        checkSimpleTableInlineAgg("SELECT MIN(A3) from T3 WHERE PKEY > 3",
                                  PlanNodeType.INDEXSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MIN),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MIN));
        checkSimpleTableInlineAgg("SELECT MAX(A3) from T3 WHERE PKEY > 3",
                                  PlanNodeType.INDEXSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX));
        checkSimpleTableInlineAgg("SELECT COUNT(A3) from T3 WHERE PKEY > 3",
                                  PlanNodeType.INDEXSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_COUNT));

        // Special
        // AVG is optimized with SUM / COUNT, generating extra projection node
        // In future, inline projection for aggregation.
        checkSimpleTableInlineAgg("SELECT AVG(A1) from T1",
                                  PlanNodeType.SEQSCAN,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM,
                                                           ExpressionType.AGGREGATE_SUM),
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_SUM,
                                                           ExpressionType.AGGREGATE_COUNT));
    }

    private void checkSimpleTableInlineAgg(String sql,
                                           PlanNodeType scanType,
                                           PlanMatcher coordAggNode,
                                           PlanMatcher distAggNode) {
        validatePlan(sql,
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              // Sometimes we see a projection node here,
                              // especially when computing average.
                              new OptionalPlanNode(PlanNodeType.PROJECTION),
                              coordAggNode,
                              AbstractReceivePlanMatcher),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(scanType,
                                                      PlanNodeType.PROJECTION,
                                                      distAggNode)));
    }

    /**
     * VoltDB has two optimizations to use the ordered output of an index scan to
     * avoid a (full) hash aggregation. In one case, this takes advantage of an
     * existing index scan already in the plan -- this case applies generally to
     * partial indexes (with WHERE clauses) and full indexes. In another case, the
     * index scan is introduced as a replacement for the sequential scan.
     * For simplicity, this case does not consider partial indexes -- it would have
     * to validate that the query conditions imply the predicate of the index.
     * This could be implemented some day.
     *
     * This tests large and normal sized temp table queries.
     */
    public void testAggregateOptimizationWithIndex() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestAggregateOptimizationWithIndex();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestAggregateOptimizationWithIndex();
        }
    }

    private void basicTestAggregateOptimizationWithIndex() {
        AbstractPlanNode p;
        List<AbstractPlanNode> pns;
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A, count(B) from R2 where B > 2 group by A;",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_COUNT),
                                  PlanNodeType.ORDERBY,                           // for serial aggregation.
                                  new PlanWithInlineNodes(PlanNodeType.INDEXSCAN, // Primary Key.
                                                          PlanNodeType.PROJECTION)));
        } else {
            validatePlan("SELECT A, count(B) from R2 where B > 2 group by A;",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                          PlanNodeType.HASHAGGREGATE,
                                                          PlanNodeType.PROJECTION)));
        }
        // matching the partial index where clause
        validatePlan("SELECT A, count(B) from R2 where B > 3 group by A;",
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(new IndexScanPlanMatcher("PARTIAL_IDX_R2"),
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));

        validatePlan("SELECT A, count(B) from R2 where A > 5 and B > 3 group by A;",
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(new IndexScanPlanMatcher("PARTIAL_IDX_R2"),
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));

        validatePlan("SELECT A, count(B) from R2 where B > 3 group by A order by A;",
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(new IndexScanPlanMatcher("PARTIAL_IDX_R2"),
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));

        // using the partial index with partial aggregation
        if (isPlanningForLargeQueries()) {
            // We can't use the index at all for ordering, because
            // the index does not contain column C.  But we can
            // use it to filter B > 3.  So, we need an index scan
            // followed by an order by.
            validatePlan("SELECT C, A, MAX(B) FROM R2 WHERE A > 0 and B > 3 GROUP BY C, A",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                           ExpressionType.AGGREGATE_MAX),
                                  PlanNodeType.ORDERBY,
                                  // Match an index scan on "PARTIAL_IDX_R2".
                                  new PlanWithInlineNodes(new IndexScanPlanMatcher("PARTIAL_IDX_R2"),
                                                          PlanNodeType.PROJECTION)));
        } else {
            validatePlan("SELECT C, A, MAX(B) FROM R2 WHERE A > 0 and B > 3 GROUP BY C, A",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(new IndexScanPlanMatcher("PARTIAL_IDX_R2"),
                                                          PlanNodeType.PARTIALAGGREGATE,
                                                          PlanNodeType.PROJECTION)));
        }
        if (isPlanningForLargeQueries()) {
            validatePlan(
                    "SELECT MAX(F_VAL2), F_D1, F_VAL1 FROM F WHERE F_D1 > 0 GROUP BY F_D1, F_VAL1 ORDER BY F_D1, MAX(F_VAL2)",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY,   // Statement Level Orderby.
                                                     // This has to be a parent of the
                                                     // AGGREGATE node.
                             PlanNodeType.AGGREGATE, // Coordinator MAX
                             new PlanWithInlineNodes(PlanNodeType.MERGERECEIVE,
                                                     PlanNodeType.ORDERBY)),
                    fragSpec(PlanNodeType.SEND,
                             new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                      ExpressionType.AGGREGATE_MAX),
                             PlanNodeType.ORDERBY,
                             new PlanWithInlineNodes(new IndexScanPlanMatcher("COL_F_TREE1"),
                                                     PlanNodeType.PROJECTION)));
        } else {
            // Partition IndexScan with HASH aggregate is optimized to use Partial aggregate -
            // index (F_D1) covers part of the GROUP BY columns
            validatePlan(
                    "SELECT F_D1, F_VAL1, MAX(F_VAL2) FROM F WHERE F_D1 > 0 GROUP BY F_D1, F_VAL1 ORDER BY F_D1, MAX(F_VAL2)",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY,
                             PlanNodeType.HASHAGGREGATE,
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                     PlanNodeType.PARTIALAGGREGATE,
                                                     PlanNodeType.PROJECTION)));
        }

        // IndexScan with HASH aggregate is optimized to use Serial aggregate -
        // index (F_VAL1, F_VAL2) covers all of the GROUP BY columns
        validatePlan("SELECT F_VAL1, F_VAL2, MAX(F_VAL3) FROM RF WHERE F_VAL1 > 0 GROUP BY F_VAL2, F_VAL1",
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));

        // IndexScan with HASH aggregate remains not optimized -
        // The first column index (F_VAL1, F_VAL2) is not part of the GROUP BY
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT F_VAL2, MAX(F_VAL2) FROM RF WHERE F_VAL1 > 0 GROUP BY F_VAL2",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.AGGREGATE,  // need serial aggregation
                                  PlanNodeType.ORDERBY,    // need orderby because COL_RF_TREE2
                                                           // indexes on F_VAL1 first.
                                  new PlanWithInlineNodes(new IndexScanPlanMatcher("COL_RF_TREE2"),
                                                          PlanNodeType.PROJECTION)));
        } else {
            validatePlan("SELECT F_VAL2, MAX(F_VAL2) FROM RF WHERE F_VAL1 > 0 GROUP BY F_VAL2",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(new IndexScanPlanMatcher("COL_RF_TREE2"),
                                                          PlanNodeType.HASHAGGREGATE,
                                                          PlanNodeType.PROJECTION)));
        }
        // Partition IndexScan with HASH aggregate remains unoptimized -
        // index (F_VAL1, F_VAL2) does not cover any of the GROUP BY columns
        if ( ! isPlanningForLargeQueries() ) {
            validatePlan("SELECT MAX(F_VAL2) FROM F WHERE F_VAL1 > 0 GROUP BY F_D1",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.HASHAGGREGATE,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                          PlanNodeType.HASHAGGREGATE,
                                                          PlanNodeType.PROJECTION)));
        }
        else {
            validatePlan("SELECT MAX(F_VAL2) FROM F WHERE F_VAL1 > 0 GROUP BY F_D1",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.AGGREGATE,
                                  new PlanWithInlineNodes(PlanNodeType.MERGERECEIVE,
                                                          PlanNodeType.ORDERBY)),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.AGGREGATE,
                                  PlanNodeType.ORDERBY,
                                  new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                                          PlanNodeType.PROJECTION)));
        }

        if ( isPlanningForLargeQueries() ) {
            validatePlan("SELECT F_VAL3, MAX(F_VAL2) FROM RF WHERE F_VAL3 = 0 GROUP BY F_VAL3",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.AGGREGATE,
                                  PlanNodeType.ORDERBY,
                                  new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                                          PlanNodeType.PROJECTION)));
        }
        else {
            validatePlan("SELECT F_VAL3, MAX(F_VAL2) FROM RF WHERE F_VAL3 = 0 GROUP BY F_VAL3",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                          PlanNodeType.HASHAGGREGATE,
                                                          PlanNodeType.PROJECTION)));
        }

        if ( isPlanningForLargeQueries() ) {
            // where clause not matching
            validatePlan("SELECT A, count(B) from R2 where B > 2 group by A order by A;",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new OptionalPlanNode(PlanNodeType.PROJECTION),
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.AGGREGATE,
                                  PlanNodeType.ORDERBY,
                                  new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                                          PlanNodeType.PROJECTION)));
        }
        else {
            // where clause not matching
            validatePlan("SELECT A, count(B) from R2 where B > 2 group by A order by A;",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new OptionalPlanNode(PlanNodeType.PROJECTION),
                                  PlanNodeType.ORDERBY,
                                  new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                          PlanNodeType.HASHAGGREGATE,
                                                          PlanNodeType.PROJECTION)));
        }
    }

    /**
     * Check that we can use a tablecount plan node.
     * this should work equally for large and normal plans.
     */
    public void testCountStar() {
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestCountStar();
        }
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestCountStar();
        }
    }

    private void basicTestCountStar() {
        validatePlan("SELECT count(*) FROM T1",
                PRINT_JSON_PLAN,
                fragSpec(PlanNodeType.SEND,
                        new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                ExpressionType.AGGREGATE_SUM),
                        PlanNodeType.RECEIVE),
                fragSpec(PlanNodeType.SEND,
                        PlanNodeType.TABLECOUNT));
    }

    public void testCountDistinct() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestCountDistinct();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestCountDistinct();
        }
    }

    private void basicTestCountDistinct() {
        String sql;

        // push down distinct because of group by partition column
        sql = "SELECT A4, count(distinct B4) FROM T4 GROUP BY A4";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             AbstractScanPlanNodeMatcher));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                    PlanNodeType.HASHAGGREGATE,
                                    PlanNodeType.PROJECTION)));
        }

        // group by multiple columns
        sql = "SELECT C4, A4, count(distinct B4) FROM T4 GROUP BY C4, A4";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                    ExpressionType.AGGREGATE_COUNT),
                            PlanNodeType.ORDERBY,
                            AbstractScanPlanNodeMatcher));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                    PlanNodeType.PROJECTION,
                                    new AggregateNodeMatcher(PlanNodeType.HASHAGGREGATE,
                                            ExpressionType.AGGREGATE_COUNT))));
        }

        // not push down distinct
        sql = "SELECT ABS(A4), count(distinct B4) FROM T4 GROUP BY ABS(A4)";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY, // pushed down from the coordinator.
                             new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                                     // No HashAggregate node here.
                                                     PlanNodeType.PROJECTION)));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.HASHAGGREGATE,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                    // No HashAggregate node here.
                                    PlanNodeType.PROJECTION)));
        }
        // test not group by partition column with index available
        sql = "SELECT A.NUM, COUNT(DISTINCT A.ID ) AS Q58 FROM P2 A GROUP BY A.NUM; ";
        if (isPlanningForLargeQueries()) {
            // I think this is ok, though it looks kind of weird.
            // Read from the bottom.
            // 1. We scan the primary key index, but just for determinism.
            // 2. We sort by the group by column.  Since we are doing
            //    count(distinct a.id), we know that two rows in
            //    group num = ? with the same value for a.id are on the
            //    same partition.  So we can push the aggregate down.  But
            //    we have to aggregate again on the coordinator, summing the
            //    counts this time.
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                     new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM)),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            allOf(PlanNodeType.AGGREGATE,
                                    new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT)),
                            PlanNodeType.ORDERBY,
                            allOf(new IndexScanPlanMatcher("VOLTDB_AUTOGEN_CONSTRAINT_IDX_P2_PK_TREE"),
                                    new ExplainStringMatcher("for deterministic order only"))));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.HASHAGGREGATE,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            allOf(new IndexScanPlanMatcher("VOLTDB_AUTOGEN_CONSTRAINT_IDX_P2_PK_TREE"),
                                    new ExplainStringMatcher("for deterministic order only"))));
        }
    }

    public void testDistinctA1() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestDistinctA1();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestDistinctA1();
        }
    }

    private void basicTestDistinctA1() {
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT DISTINCT A1 FROM T1",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,  // Aggregate for distinct.
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,  // Aggregate for distinct.
                             PlanNodeType.ORDERBY,
                             new IndexScanPlanMatcher("VOLTDB_AUTOGEN_IDX_PK_T1_PKEY")));
        } else {
            validatePlan("SELECT DISTINCT A1 FROM T1",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.HASHAGGREGATE,
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             allOf(new IndexScanPlanMatcher("VOLTDB_AUTOGEN_IDX_PK_T1_PKEY"),
                                   new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                           PlanNodeType.PROJECTION,
                                                           PlanNodeType.HASHAGGREGATE))));
        }
    }

    public void testDistinctA1_Subquery() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestDistinctA1_Subquery();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestDistinctA1_Subquery();
        }
    }

    private void basicTestDistinctA1_Subquery() {
        AbstractPlanNode p;
        List<AbstractPlanNode> pns;

        // Distinct rewrote with group by
        if (isPlanningForLargeQueries()) {
            validatePlan("select * from (SELECT DISTINCT A1 FROM T1) temp",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.SEQSCAN,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             AbstractScanPlanNodeMatcher));
        } else {
            validatePlan("select * from (SELECT DISTINCT A1 FROM T1) temp",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.SEQSCAN,
                            PlanNodeType.HASHAGGREGATE,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                    PlanNodeType.HASHAGGREGATE,
                                    PlanNodeType.PROJECTION)));
        }
    }

    public void testGroupByA1() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByA1();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByA1();
        }
    }

    private void basicTestGroupByA1() {
        AbstractPlanNode p;
        AggregatePlanNode aggNode;
        List<AbstractPlanNode> pns;

        // T1 has a primary key, but it's not useful
        // to us here.  So we just aggregate.  For large
        // temp tables we need to sort by A1.  Note that
        // the partition key is PKEY, not A1, so this
        // needs coordinator node aggregation.  Since
        // the distributed node produces output in
        // A1 order, we can merge receive from there.
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A1 from T1 group by A1",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.AGGREGATE,
                                  MergeReceivePlanMatcher),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.AGGREGATE,
                                  PlanNodeType.ORDERBY,
                                  AbstractScanPlanNodeMatcher));
        } else {
            validatePlan("SELECT A1 from T1 group by A1",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.HASHAGGREGATE,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                                          PlanNodeType.PROJECTION,
                                                          PlanNodeType.HASHAGGREGATE)));
        }

        // Having
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A1, count(*) from T1 group by A1 Having count(*) > 3",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(new AggregateNodeMatcher(),
                                    new NodeTestMatcher("has Post Predicate",
                                            (AbstractPlanNode n) -> {
                                                return ((AggregatePlanNode)n).getPostPredicate() != null;
                                            })),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            new AggregateNodeMatcher(PlanNodeType.AGGREGATE,
                                                     ExpressionType.AGGREGATE_COUNT_STAR),
                            PlanNodeType.ORDERBY,
                            PlanNodeType.INDEXSCAN));
        } else {
            validatePlan("SELECT A1, count(*) from T1 group by A1 Having count(*) > 3",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(new AggregateNodeMatcher(),
                                    new NodeTestMatcher("has Post Predicate",
                                            (AbstractPlanNode n) -> {
                                                return ((AggregatePlanNode) n).getPostPredicate() != null;
                                            })),
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    new AggregateNodeMatcher(PlanNodeType.HASHAGGREGATE,
                                            ExpressionType.AGGREGATE_COUNT_STAR))));
        }

        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A1, count(*) from T1 group by A1 Having count(*) > 3",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(new AggregateNodeMatcher(),
                                    new NodeTestMatcher("has Post Predicate",
                                            (AbstractPlanNode n) -> {
                                                // We know n is an aggregate plan
                                                // node by the first test in allOf.
                                                return ((AggregatePlanNode)n).getPostPredicate() != null;
                                            })),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             new NodeTestMatcher("agg node with no post predicate",
                                                 n -> {
                                                    return n.getPlanNodeType() == PlanNodeType.AGGREGATE
                                                            && ((AggregatePlanNode)n).getPostPredicate() == null;
                                                 }),
                             PlanNodeType.ORDERBY,
                             AbstractScanPlanNodeMatcher));

        } else {
            validatePlan("SELECT A1, count(*) from T1 group by A1 Having count(*) > 3",
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(new AggregateNodeMatcher(),
                                    new NodeTestMatcher("has Post Predicate",
                                            (AbstractPlanNode n) -> {
                                                // We know n is an aggregate plan
                                                // node by the first test in allOf.
                                                return ((AggregatePlanNode) n).getPostPredicate() != null;
                                            })),
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(AbstractScanPlanNodeMatcher, // abstract plan node
                                    PlanNodeType.PROJECTION,     // With an inline projection node
                                    // and an inline agg node
                                    //    with no post predicate.
                                    new NodeTestMatcher("has inline agg node",
                                            n -> {
                                                // There is no index, so we need
                                                // an inline hash aggregate.
                                                // There is no post predicate here,
                                                // since this is not yet post aggregation.
                                                return (n.getPlanNodeType() == PlanNodeType.HASHAGGREGATE)
                                                        && ((AggregatePlanNode) n).getPostPredicate() == null;
                                            }))));
        }
    }

    private void checkGroupByPartitionKey(String SQL,
             final boolean topAgg, final boolean having) {
        if (isPlanningForLargeQueries()) {
            validatePlan(SQL,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            new MatchIf(topAgg, // If topAgg is true
                                    allOf(new AggregateNodeMatcher(), // Match an aggregate node,
                                            new NodeTestMatcher("has aggregate",
                                                    n -> {
                                                        if (having) {
                                                            // Also, if having is true
                                                            // then require a post predicate.
                                                            return ((AggregatePlanNode) n).getPostPredicate() != null;
                                                        }
                                                        return true;
                                                    }))),
                            topAgg ? MergeReceivePlanMatcher : PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            allOf(AbstractScanPlanNodeMatcher,
                                    new NodeTestMatcher("Aggregate with possible post predicate",
                                            n -> {
                                                // What kind of aggregate do we want?
                                                PlanNodeType aggType = PlanNodeType.AGGREGATE;
                                                // Get the aggregate node.
                                                AbstractPlanNode aggNode = n.getInlinePlanNode(aggType);
                                                if (aggNode == null) {
                                                    return false;
                                                }
                                                if (having && !topAgg) {
                                                    return ((AggregatePlanNode) aggNode).getPostPredicate() != null;
                                                }
                                                return true;
                                            }))));
        } else {
            validatePlan(SQL,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            new MatchIf(topAgg, // If topAgg is true
                                    allOf(new AggregateNodeMatcher(), // Match an aggregate node,
                                            new NodeTestMatcher("has aggregate",
                                                    n -> {
                                                        if (having) {
                                                            // Also, if having is true
                                                            // then require a post predicate.
                                                            return ((AggregatePlanNode) n).getPostPredicate() != null;
                                                        }
                                                        return true;
                                                    }))),
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            allOf(AbstractScanPlanNodeMatcher,
                                    new NodeTestMatcher("Aggregate with possible post predicate",
                                            n -> {
                                                // What kind of aggregate do we want?
                                                PlanNodeType aggType = PlanNodeType.HASHAGGREGATE;
                                                if (n instanceof IndexScanPlanNode &&
                                                        ((IndexScanPlanNode) n).isForGroupingOnly()) {
                                                    aggType = PlanNodeType.AGGREGATE;
                                                }
                                                // Get the aggregate node.
                                                AbstractPlanNode aggNode = n.getInlinePlanNode(aggType);
                                                if (aggNode == null) {
                                                    return false;
                                                }
                                                if (having && !topAgg) {
                                                    return ((AggregatePlanNode) aggNode).getPostPredicate() != null;
                                                }
                                                return true;
                                            }))));
        }
    }

    public void testGroupByPartitionKey() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByPartitionKey();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByPartitionKey();
        }
    }

    private void basicTestGroupByPartitionKey() {
        List<AbstractPlanNode> pns;

        // Primary key is equal to partition key
        // "its primary key index (for optimized grouping only)"
        // Not sure why not use serial aggregate instead
        checkGroupByPartitionKey("SELECT PKEY, COUNT(*) from T1 group by PKEY", false, false);

        // Test Having expression
        checkGroupByPartitionKey("SELECT PKEY, COUNT(*) from T1 group by PKEY Having count(*) > 3",
                                 false, true);

        // Primary key is not equal to partition key
        checkGroupByPartitionKey("SELECT A3, COUNT(*) from T3 group by A3", false, false);

        // Test Having expression
        checkGroupByPartitionKey("SELECT A3, COUNT(*) from T3 group by A3 Having count(*) > 3", false, true);


        // Group by partition key and others
        checkGroupByPartitionKey("SELECT B3, A3, COUNT(*) from T3 group by B3, A3", false, false);

        // Test Having expression
        checkGroupByPartitionKey("SELECT B3, A3, COUNT(*) from T3 group by B3, A3 Having count(*) > 3", false, true);
    }

    public void testGroupByPartitionKey_Negative() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByPartitionKey_Negative();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByPartitionKey_Negative();
        }
    }

    private void basicTestGroupByPartitionKey_Negative() {
        String sql;
        List<AbstractPlanNode> pns;

        sql = "SELECT ABS(PKEY), COUNT(*) from T1 group by ABS(PKEY)";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             new IndexScanPlanMatcher("VOLTDB_AUTOGEN_IDX_PK_T1_PKEY")));
        }
        else {
            checkGroupByPartitionKey(sql, true, false);
        }

        sql = "SELECT ABS(PKEY), COUNT(*) from T1 group by ABS(PKEY) Having count(*) > 3";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            PlanNodeType.ORDERBY,
                            new IndexScanPlanMatcher("VOLTDB_AUTOGEN_IDX_PK_T1_PKEY")));
        }
        else {
            checkGroupByPartitionKey(sql, true, true);
        }
    }

    // Group by with index
    private void checkGroupByOnlyPlanOneFragment(String SQL, PlanNodeType aggregate) {
        validatePlan(SQL,
                PRINT_JSON_PLAN,
                fragSpec(PlanNodeType.SEND,
                        new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                PlanNodeType.PROJECTION,
                                aggregate)));
    }

    public void testGroupByOnly() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByOnly();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByOnly();
        }
    }
    private void basicTestGroupByOnly() {
        System.out.println("Starting testGroupByOnly");
        String sql;
        /**
         * Serial Aggregate cases
         */
        // Replicated Table

        // only GROUP BY cols in SELECT clause
        sql = "SELECT F_D1 FROM RF GROUP BY F_D1";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        // SELECT cols in GROUP BY and other aggregate cols
        sql = "SELECT F_D1, COUNT(*) FROM RF GROUP BY F_D1";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        // aggregate cols are part of keys of used index
        sql = "SELECT F_VAL1, SUM(F_VAL2) FROM RF GROUP BY F_VAL1";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        // expr index, full indexed case
        sql = "SELECT F_D1 + F_D2, COUNT(*) FROM RF GROUP BY F_D1 + F_D2";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        // function index, prefix indexed case
        sql = "SELECT ABS(F_D1), COUNT(*) FROM RF GROUP BY ABS(F_D1)";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        // order of GROUP BY cols is different of them in index definition
        // index on (ABS(F_D1), F_D2 - F_D3), GROUP BY on (F_D2 - F_D3, ABS(F_D1))
        sql = "SELECT F_D2 - F_D3, ABS(F_D1), COUNT(*) FROM RF GROUP BY F_D2 - F_D3, ABS(F_D1)";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        sql = "SELECT F_VAL1, F_VAL2, COUNT(*) FROM RF GROUP BY F_VAL2, F_VAL1";
        checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.AGGREGATE);

        // Partitioned Table
        // index scan for group by only, no need using hash aggregate
        sql = "SELECT F_D1 FROM F GROUP BY F_D1";
        checkGroupByOnlyPlanTwoFragSerialAgg(sql);

        sql = "SELECT F_D1, COUNT(*) FROM F GROUP BY F_D1";
        checkGroupByOnlyPlanTwoFragSerialAgg(sql);

        sql = "SELECT F_VAL1, SUM(F_VAL2) FROM F GROUP BY F_VAL1";
        checkGroupByOnlyPlanTwoFragSerialAgg(sql);

        sql = "SELECT F_D1 + F_D2, COUNT(*) FROM F GROUP BY F_D1 + F_D2";
        checkGroupByOnlyPlanTwoFragSerialAgg(sql);

        sql = "SELECT ABS(F_D1), COUNT(*) FROM F GROUP BY ABS(F_D1)";
        checkGroupByOnlyPlanTwoFragSerialAgg(sql);

        sql = "SELECT F_D2 - F_D3, ABS(F_D1), COUNT(*) FROM F GROUP BY F_D2 - F_D3, ABS(F_D1)";
        checkGroupByOnlyPlanTwoFragSerialAgg(sql);

        /**
         * Hash Aggregate cases
         */
        // unoptimized case (only use second col of the index), but will be replaced in
        // SeqScanToIndexScan optimization for deterministic reason
        // use EXPR_RF_TREE1 not EXPR_RF_TREE2
        sql = "SELECT F_D2 - F_D3, COUNT(*) FROM RF GROUP BY F_D2 - F_D3";
        if (isPlanningForLargeQueries()) {
            checkGroupByOnlyPlanOneFragmentLarge(sql, "EXPR_RF_TREE1");
        }
        else {
            checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.HASHAGGREGATE);
        }

        // unoptimized case: index is not scannable
        sql = "SELECT F_VAL3, COUNT(*) FROM RF GROUP BY F_VAL3";
        if (isPlanningForLargeQueries()) {
            checkGroupByOnlyPlanOneFragmentLarge(sql, "EXPR_RF_TREE1");
        }
        else {
            checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.HASHAGGREGATE);
        }

        // unoptimized case: F_D2 is not prefix indexable
        sql = "SELECT F_D2, COUNT(*) FROM RF GROUP BY F_D2";
        if (isPlanningForLargeQueries()) {
            checkGroupByOnlyPlanOneFragmentLarge(sql, "EXPR_RF_TREE1");
        }
        else {
            checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.HASHAGGREGATE);
        }

        // unoptimized case (only uses second col of the index), will not be replaced in
        // SeqScanToIndexScan for determinism because of non-deterministic receive.
        // Use primary key index
        sql = "SELECT F_D2 - F_D3, COUNT(*) FROM F GROUP BY F_D2 - F_D3";
        checkGroupByOnlyPlanTwoFragHashAgg(sql);

        // unoptimized case (only uses second col of the index), will be replaced in
        // SeqScanToIndexScan for determinism.
        // use EXPR_F_TREE1 not EXPR_F_TREE2
        sql = "SELECT F_D2 - F_D3, COUNT(*) FROM RF GROUP BY F_D2 - F_D3";
        if (isPlanningForLargeQueries()) {
            checkGroupByOnlyPlanOneFragmentLarge(sql, "EXPR_RF_TREE1");
        }
        else {
            checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.HASHAGGREGATE);
        }

        /**
         * Partial Aggregate cases
         */
        // unoptimized case: no prefix index found for (F_D1, F_D2)
        sql = "SELECT F_D1, F_D2, COUNT(*) FROM RF GROUP BY F_D1, F_D2";
        if (isPlanningForLargeQueries()) {
            checkGroupByOnlyPlanOneFragmentLarge(sql, "COL_RF_TREE1");
        }
        else {
            checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.PARTIALAGGREGATE);
        }

        sql = "SELECT ABS(F_D1), F_D3, COUNT(*) FROM RF GROUP BY ABS(F_D1), F_D3";
        if (isPlanningForLargeQueries()) {
            checkGroupByOnlyPlanOneFragmentLarge(sql, "EXPR_RF_TREE2");
        }
        else {
            checkGroupByOnlyPlanOneFragment(sql, PlanNodeType.PARTIALAGGREGATE);
        }

        // partition table
        sql = "SELECT F_D1, F_D2, COUNT(*) FROM F GROUP BY F_D1, F_D2";
        checkGroupByOnlyPlanTwoFragPartialAgg(sql);

        sql = "SELECT ABS(F_D1), F_D3, COUNT(*) FROM F GROUP BY ABS(F_D1), F_D3";
        checkGroupByOnlyPlanTwoFragPartialAgg(sql);
        /**
         * Regression case
         */
        // ENG-9990 Repeating GROUP BY partition key in SELECT corrupts output schema.
        // We only test that the output schema is right.
        validatePlan("SELECT G_PKEY, COUNT(*) C, G_PKEY FROM G GROUP BY G_PKEY",
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              allOf(PlanNodeType.RECEIVE,
                                    new OutputSchemaMatcher("C", VoltType.BIGINT, 1))),
                     fragSpec(new AnyFragment()));
    }

    private void checkGroupByOnlyPlanOneFragmentLarge(String SQL, String indexName) {
        validatePlan(SQL,
                PRINT_JSON_PLAN,
                fragSpec(PlanNodeType.SEND,
                        PlanNodeType.AGGREGATE,
                        PlanNodeType.ORDERBY,
                        new PlanWithInlineNodes(new IndexScanPlanMatcher(indexName),
                                PlanNodeType.PROJECTION)));

    }

    private void checkGroupByOnlyPlanTwoFragHashAgg(String sql) {
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             // This index scan is for determinism, and
                             // not to provide a useful order for group by.
                             // So we need the aggregate/orderby pair.
                             new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                     PlanNodeType.PROJECTION)));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.HASHAGGREGATE,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    PlanNodeType.HASHAGGREGATE)));
        }
    }

    /*
     * The "Serial" in the name means the distributed fragment
     * has an inline serial aggregation.  The coordinator node
     * can still have a hash aggregate.
     */
    private void checkGroupByOnlyPlanTwoFragSerialAgg(String sql) {
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY, // Orderby pushed down
                                                   // from coord fragment
                                                   // to force serial aggregation.
                             new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    PlanNodeType.AGGREGATE)));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.HASHAGGREGATE,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    PlanNodeType.AGGREGATE)));
        }
    }

    private void checkGroupByOnlyPlanTwoFragPartialAgg(String sql) {
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            PlanNodeType.ORDERBY,
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION)));
        } else {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.HASHAGGREGATE,
                            PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    PlanNodeType.PARTIALAGGREGATE)));
        }
    }

    private void checkPartialAggregate(String sql,
            boolean twoFragments) {
        List<AbstractPlanNode> pns = compileToFragments(sql);
        AbstractPlanNode apn;
        if (twoFragments) {
            assertEquals(2, pns.size());
            apn = pns.get(1).getChild(0);
        }
        else {
            assertEquals(1, pns.size());
            apn = pns.get(0).getChild(0);
        }
        assertTrue(apn.toExplainPlanString().toLowerCase().contains("partial"));
    }

    public void testPartialSerialAggregateOnJoin() {
        String sql;
        List<AbstractPlanNode> pns;

        sql = "SELECT G.G_D1, RF.F_D2, COUNT(*) " +
                "FROM G LEFT OUTER JOIN RF ON G.G_D2 = RF.F_D1 " +
                "GROUP BY G.G_D1, RF.F_D2";
        checkPartialAggregate(sql, true);

        // With different group by key ordered
        sql = "SELECT G.G_D1, RF.F_D2, COUNT(*) " +
                "FROM G LEFT OUTER JOIN RF ON G.G_D2 = RF.F_D1 " +
                "GROUP BY RF.F_D2, G.G_D1";
        checkPartialAggregate(sql, true);


        // three table joins with aggregate
        sql = "SELECT G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3, COUNT(*) " +
                "FROM G LEFT OUTER JOIN F ON G.G_PKEY = F.F_PKEY " +
                "     LEFT OUTER JOIN RF ON G.G_D1 = RF.F_D1 " +
                "GROUP BY G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3";
        checkPartialAggregate(sql, true);

        // With different group by key ordered
        sql = "SELECT G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3, COUNT(*) " +
                "FROM G LEFT OUTER JOIN F ON G.G_PKEY = F.F_PKEY " +
                "     LEFT OUTER JOIN RF ON G.G_D1 = RF.F_D1 " +
                "GROUP BY G.G_PKEY, RF.F_D2, G.G_D1, F.F_D3";
        checkPartialAggregate(sql, true);

        // With different group by key ordered
        sql = "SELECT G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3, COUNT(*) " +
                "FROM G LEFT OUTER JOIN F ON G.G_PKEY = F.F_PKEY " +
                "     LEFT OUTER JOIN RF ON G.G_D1 = RF.F_D1 " +
                "GROUP BY RF.F_D2, G.G_PKEY, F.F_D3, G.G_D1";
        checkPartialAggregate(sql, true);
    }


    // check group by query with limit
    // Query has group by from partition column and limit, does not have order by
    private void checkGroupByOnlyPlanWithLimit(List<AbstractPlanNode> pns,
            boolean twoFragments, boolean isHashAggregator,
            boolean isIndexScan, boolean inlineLimit) {
        // 'inlineLimit' means LIMIT gets pushed down for partition table and
        // inlined with aggregate.

        AbstractPlanNode apn = pns.get(0).getChild(0);

        if (!inlineLimit || twoFragments) {
            assertEquals(PlanNodeType.LIMIT, apn.getPlanNodeType());
            apn = apn.getChild(0);
        }

        // Group by partition column does not need top group by node.
        if (twoFragments) {
            apn = pns.get(1).getChild(0);
            if (!inlineLimit) {
                assertEquals(PlanNodeType.LIMIT, apn.getPlanNodeType());
                apn = apn.getChild(0);
            }
        }

        // For a single table aggregate, it is inline always.
        assertEquals(
                (isIndexScan ? PlanNodeType.INDEXSCAN : PlanNodeType.SEQSCAN),
                apn.getPlanNodeType());
        if (isHashAggregator) {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        }
        else {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.AGGREGATE));
            if (inlineLimit) {
                AbstractPlanNode p = apn.getInlinePlanNode(PlanNodeType.AGGREGATE);
                assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
            }
        }
    }

    // GROUP BY With LIMIT without ORDER BY
    public void testGroupByWithLimit() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByWithLimit();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByWithLimit();
        }
    }

    private void basicTestGroupByWithLimit() {
        String sql;
        List<AbstractPlanNode> pns;

        // replicated table with serial aggregation and inlined limit
        sql = "SELECT F_PKEY FROM RF GROUP BY F_PKEY LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             new PlanWithInlineNodes(
                                     allOf(PlanNodeType.INDEXSCAN,
                                           new IndexScanPlanMatcher("VOLTDB_AUTOGEN_IDX_PK_RF_F_PKEY")),
                                     PlanNodeType.PROJECTION,
                                     new PlanWithInlineNodes(PlanNodeType.AGGREGATE,
                                                             PlanNodeType.LIMIT))));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, false, false, true, true);
        }

        sql = "SELECT F_D1 FROM RF GROUP BY F_D1 LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    new PlanWithInlineNodes(PlanNodeType.AGGREGATE,
                                            PlanNodeType.LIMIT))));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, false, false, true, true);
        }

        // partitioned table with serial aggregation and inlined limit
        // group by columns contain the partition key is the only case allowed
        sql = "SELECT F_PKEY FROM F GROUP BY F_PKEY LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.LIMIT,
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             new PlanWithInlineNodes(
                                     new IndexScanPlanMatcher("VOLTDB_AUTOGEN_IDX_PK_F_F_PKEY"),
                                     PlanNodeType.PROJECTION,
                                     new PlanWithInlineNodes(PlanNodeType.AGGREGATE,
                                                             PlanNodeType.LIMIT))));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);

            // Explain plan for the above query
            /*
               RETURN RESULTS TO STORED PROCEDURE
                LIMIT 5
                 RECEIVE FROM ALL PARTITIONS

               RETURN RESULTS TO STORED PROCEDURE
                INDEX SCAN of "F" using its primary key index (for optimized grouping only)
                 inline Serial AGGREGATION ops
                  inline LIMIT 5
            */
            String expectedStr = "  inline Serial AGGREGATION ops: \n" +
                    "   inline LIMIT 5";
            String explainPlan = "";
            for (AbstractPlanNode apn : pns) {
                explainPlan += apn.toExplainPlanString();
            }
            assertTrue(explainPlan.contains(expectedStr));
        }

        sql = "SELECT A3, COUNT(*) FROM T3 GROUP BY A3 LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.LIMIT,
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             // Index scan of index named T3_TREE1
                             // with inline Projection and inline aggregate.
                             new PlanWithInlineNodes(new IndexScanPlanMatcher("T3_TREE1"),
                                                     PlanNodeType.PROJECTION,
                                                     // Serial Aggregate with inline limit.
                                                     allOf(new PlanWithInlineNodes(PlanNodeType.AGGREGATE,
                                                                                   PlanNodeType.LIMIT),
                                                             // Which has an aggregate count(*) expression.
                                                             new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT_STAR)))));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);
        }

        sql = "SELECT A3, B3, COUNT(*) FROM T3 GROUP BY A3, B3 LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.LIMIT,
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             new PlanWithInlineNodes(new IndexScanPlanMatcher("T3_TREE1"),
                                                     PlanNodeType.PROJECTION,
                                                     allOf(new PlanWithInlineNodes(PlanNodeType.AGGREGATE,
                                                                                   PlanNodeType.LIMIT),
                                                             new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT_STAR)))));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);
        }

        sql = "SELECT A3, B3, COUNT(*) FROM T3 WHERE A3 > 1 GROUP BY B3, A3 LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.LIMIT,
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(new IndexScanPlanMatcher("T3_TREE1"),
                                    PlanNodeType.PROJECTION,
                                    allOf(new PlanWithInlineNodes(PlanNodeType.AGGREGATE,
                                                    PlanNodeType.LIMIT),
                                            new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT_STAR)))));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);
        }

        //
        // negative tests
        //
        // RF is replicated and has no useful indexes for F_VAL2.
        sql = "SELECT F_VAL2 FROM RF GROUP BY F_VAL2 LIMIT 5";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    // The distributed fragment needs an agg/order by
                    // pair.  The index is for determinism, and is not
                    // useful.
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            pns = compileToFragments(sql);
            checkGroupByOnlyPlanWithLimit(pns, false, true, true, false);
        }
        // Limit should not be pushed down for case like:
        // Group by non-partition without partition key and order by.
        // ENG-6485
    }

    public void testEdgeComplexRelatedCases() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestEdgeComplexRelatedCases();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestEdgeComplexRelatedCases();
        }
    }

    private void basicTestEdgeComplexRelatedCases() {
        String sql;
        List<AbstractPlanNode> pns;
        AbstractPlanNode p;

        sql = "select PKEY+A1 from T1 Order by PKEY+A1";
        validatePlan(sql,
                     PRINT_JSON_PLAN,
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              MergeReceivePlanMatcher),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.ORDERBY,
                              AbstractScanPlanNodeMatcher));
        /*
        pns = compileToFragments(sql);
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof MergeReceivePlanNode);
        assertNotNull(p.getChild(0).getInlinePlanNode(PlanNodeType.ORDERBY));

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof OrderByPlanNode);
        assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);
        */

        // Useless order by clause.
        sql = "SELECT count(*)  FROM P1 order by PKEY";
        validatePlan(sql,
                PRINT_JSON_PLAN,
                fragSpec(PlanNodeType.SEND,
                         new AggregateNodeMatcher(),
                         PlanNodeType.RECEIVE),
                fragSpec(PlanNodeType.SEND,
                         AbstractScanPlanNodeMatcher));
        /*
        pns = compileToFragments(sql);
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);
        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        */

        sql = "SELECT A1, count(*) as tag FROM P1 group by A1 order by tag, A1 limit 1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY,  // Statement Level Order By (SLOB).
                             allOf(PlanNodeType.AGGREGATE,
                                     new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM)),
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                   new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT_STAR)),
                             PlanNodeType.ORDERBY,
                             PlanNodeType.SEQSCAN));
        }
        else {
            pns = compileToFragments(sql);
            p = pns.get(0).getChild(0);

            // ENG-5066: now Limit is pushed under Projection
            // Limit is also inlined with Orderby node
            // ENG-12434 But maybe there is no projection node.
            if (p instanceof ProjectionPlanNode) {
                p = p.getChild(0);
            }
            assertTrue(p instanceof OrderByPlanNode);
            assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
            assertTrue(p.getChild(0) instanceof AggregatePlanNode);

            p = pns.get(1).getChild(0);
            // inline aggregate
            assertTrue(p instanceof AbstractScanPlanNode);
            assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        }

        sql = "SELECT F_D1, count(*) as tag FROM RF group by F_D1 order by tag";
        pns = compileToFragments(sql);
        p = pns.get(0).getChild(0);

        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        assertTrue(p instanceof OrderByPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));

        sql = "SELECT F_D1, count(*) FROM RF group by F_D1 order by 2";
        pns = compileToFragments(sql);
        p = pns.get(0).getChild(0);

        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        assertTrue(p instanceof OrderByPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
    }

    private void checkHasComplexAgg(List<AbstractPlanNode> pns) {
        checkHasComplexAgg(pns, false);
    }

    private void checkHasComplexAgg(List<AbstractPlanNode> pns,
            boolean projectPushdown) {
        assertTrue(pns.size() > 0);
        boolean isDistributed = pns.size() > 1;

        if (projectPushdown) {
            assertTrue(isDistributed);
        }

        AbstractPlanNode p = pns.get(0).getChild(0);
        if (p instanceof LimitPlanNode) {
            p = p.getChild(0);
        }
        if (p instanceof OrderByPlanNode) {
            p = p.getChild(0);
        }
        if (! projectPushdown) {
            assertTrue(p instanceof ProjectionPlanNode);
        }
        while ( p.getChildCount() > 0) {
            p = p.getChild(0);
            assertFalse(p instanceof ProjectionPlanNode);
        }

        if (isDistributed) {
            p = pns.get(1).getChild(0);
            int projectCount = 0;
            while ( p.getChildCount() > 0) {
                p = p.getChild(0);
                if (p instanceof ProjectionPlanNode) {
                    projectCount++;
                    assertTrue(projectPushdown);
                }
            }
            if (projectPushdown) {
                assertEquals(1, projectCount);
            }
        }
    }

    public void testComplexAggwithLimit() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestComplexAggwithLimit();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestComplexAggwithLimit();
        }
    }

    private void basicTestComplexAggwithLimit() {
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.ORDERBY,
                                                          PlanNodeType.LIMIT),
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.AGGREGATE,
                                  MergeReceivePlanMatcher),
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.ORDERBY,
                                                          PlanNodeType.LIMIT),
                                  PlanNodeType.AGGREGATE,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.SEQSCAN));
        } else {
            validatePlan("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.ORDERBY,
                                                          PlanNodeType.LIMIT),
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.HASHAGGREGATE,
                                  new AnyFragment()),
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.ORDERBY,
                                                          PlanNodeType.LIMIT),
                                  new PlanWithInlineNodes(AbstractScanPlanNodeMatcher,
                                                          PlanNodeType.PROJECTION,
                                                          PlanNodeType.HASHAGGREGATE)));
        }
    }

    public void testComplexAggwithDistinct() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestComplexAggwithDistinct();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestComplexAggwithDistinct();
        }
    }

    private void basicTestComplexAggwithDistinct() {
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.AGGREGATE, // Aggregation not pushed down with distinct.
                                  MergeReceivePlanMatcher),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  AbstractScanPlanNodeMatcher));
        } else {
            validatePlan("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.HASHAGGREGATE, // Aggregation not pushed down with distinct.
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  AbstractScanPlanNodeMatcher));
        }
    }

    public void testComplexAggwithLimitDistinct() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestComplexAggwithLimitDistinct();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestComplexAggwithLimitDistinct();
        }
    }

    private void basicTestComplexAggwithLimitDistinct() {
        if (isPlanningForLargeQueries()) {
            validatePlan("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.ORDERBY,
                                                          PlanNodeType.LIMIT),
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.AGGREGATE,
                                  MergeReceivePlanMatcher),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  AbstractScanPlanNodeMatcher));
        } else {
            validatePlan("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2",
                         PRINT_JSON_PLAN,
                         fragSpec(PlanNodeType.SEND,
                                  new PlanWithInlineNodes(PlanNodeType.ORDERBY,
                                                          PlanNodeType.LIMIT),
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.HASHAGGREGATE,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  AbstractScanPlanNodeMatcher));
        }
    }

    public void testComplexAggCase() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestComplexAggCase();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestComplexAggCase();
        }
    }

    private void basicTestComplexAggCase() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY) as A2, (SUM(PKEY) / 888) as A3, (SUM(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY), COUNT(PKEY), (AVG(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    public void testComplexAggCaseProjectPushdown() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestComplexAggCaseProjectPushdown();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestComplexAggCaseProjectPushdown();
        }
    }

    private void basicTestComplexAggCaseProjectPushdown() {
        List<AbstractPlanNode> pns;

        // This complex aggregate case will push down ORDER BY LIMIT
        // so the projection plan node should be also pushed down
        pns = compileToFragments("SELECT PKEY, sum(A1) + 1 FROM P1 GROUP BY PKEY order by 1, 2 Limit 10");
        checkHasComplexAgg(pns, true);

        pns = compileToFragments("SELECT PKEY, AVG(A1) FROM P1 GROUP BY PKEY order by 1, 2 Limit 10");
        checkHasComplexAgg(pns, true);
    }

    public void testComplexGroupBy() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestComplexGroupBy();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestComplexGroupBy();
        }
    }

    private void basicTestComplexGroupBy() {
        List<AbstractPlanNode> pns;
        String sql;

        sql = "SELECT A1, ABS(A1), ABS(A1)+1, sum(B1) FROM P1 GROUP BY A1, ABS(A1)";
        // Just print the json plan, and don't really
        // validate anything.
        printJSONPlan(PRINT_JSON_PLAN, sql);

        pns = compileToFragments(sql);
        checkHasComplexAgg(pns);

        // Check it can compile
        sql = "SELECT ABS(A1), sum(B1) FROM P1 GROUP BY ABS(A1)";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            printJSONPlan(PRINT_JSON_PLAN, sql);
            pns = compileToFragments(sql);
            AbstractPlanNode p = pns.get(0).getChild(0);
            assertTrue(p instanceof AggregatePlanNode);

            p = pns.get(1).getChild(0);
            // inline aggregate
            assertTrue(p instanceof AbstractScanPlanNode);
            assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        }

        sql = "SELECT A1+PKEY, avg(B1) as tag FROM P1 GROUP BY A1+PKEY ORDER BY ABS(tag), A1+PKEY";
        printJSONPlan(PRINT_JSON_PLAN, sql);
        pns = compileToFragments(sql);
        checkHasComplexAgg(pns);
    }

    private void checkOptimizedAgg(List<AbstractPlanNode> pns, boolean optimized) {
        AbstractPlanNode p = pns.get(0).getChild(0);
        if (optimized) {
            assertTrue(p instanceof ProjectionPlanNode);
            assertTrue(p.getChild(0) instanceof AggregatePlanNode);

            p = pns.get(1).getChild(0);
            // push down for optimization
            assertTrue(p instanceof AbstractScanPlanNode);

            assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                    p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        }
        else {
            assertEquals(1, pns.size());
            assertTrue(p instanceof AbstractScanPlanNode);
            assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                    p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        }
    }

    public void testUnOptimizedAVG() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestUnOptimizedAVG();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestUnOptimizedAVG();
        }
    }

    private void basicTestUnOptimizedAVG() {
        List<AbstractPlanNode> pns;
        String sql;

        sql = "SELECT AVG(A1) FROM R1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                    PlanNodeType.PROJECTION,
                                    PlanNodeType.AGGREGATE)));
        }
        else {
            pns = compileToFragments(sql);
            checkOptimizedAgg(pns, false);
        }

        sql = "SELECT A1, AVG(PKEY) FROM R1 GROUP BY A1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                   new AggregateNodeMatcher(ExpressionType.AGGREGATE_AVG)),
                             PlanNodeType.ORDERBY,
                             new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                     PlanNodeType.PROJECTION)));
        }
        else {
            pns = compileToFragments(sql);
            checkOptimizedAgg(pns, false);
        }

        sql = "SELECT A1, AVG(PKEY)+1 FROM R1 GROUP BY A1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.PROJECTION,
                             allOf(PlanNodeType.AGGREGATE,
                                   new AggregateNodeMatcher(ExpressionType.AGGREGATE_AVG)),
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            pns = compileToFragments(sql);
            checkHasComplexAgg(pns);
            AbstractPlanNode p = pns.get(0).getChild(0);
            assertTrue(p instanceof ProjectionPlanNode);
            p = p.getChild(0);
            assertTrue(p instanceof AbstractScanPlanNode);
            assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                    p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        }
    }

    public void testOptimizedAVG() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestOptimizedAVG();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestOptimizedAVG();
        }
    }

    private void basicTestOptimizedAVG() {
        String sql;
        List<AbstractPlanNode> pns;

        sql = "SELECT AVG(A1) FROM P1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.PROJECTION,
                             allOf(PlanNodeType.AGGREGATE,
                                     new AggregateNodeMatcher(
                                             ExpressionType.AGGREGATE_SUM,
                                             ExpressionType.AGGREGATE_SUM
                                     )),
                             PlanNodeType.RECEIVE),
                    fragSpec(PlanNodeType.SEND,
                             new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                     PlanNodeType.PROJECTION,
                                     allOf(PlanNodeType.AGGREGATE,
                                             new AggregateNodeMatcher(
                                                     ExpressionType.AGGREGATE_SUM,
                                                     ExpressionType.AGGREGATE_COUNT)))));
        }
        else {
            printJSONPlan(PRINT_JSON_PLAN, sql);
            pns = compileToFragments(sql);
            checkHasComplexAgg(pns);
            checkOptimizedAgg(pns, true);
        }

        sql = "SELECT A1, AVG(PKEY) FROM P1 GROUP BY A1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.PROJECTION,      // Compute the quotient.
                             allOf(PlanNodeType.AGGREGATE, // Compute the total sum and count.
                                     new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM,
                                                              ExpressionType.AGGREGATE_SUM)),
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                   new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT, // Compute the group counts.
                                                            ExpressionType.AGGREGATE_SUM)), // Compute the group sums.
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            printJSONPlan(PRINT_JSON_PLAN, sql);
            pns = compileToFragments(sql);
            checkHasComplexAgg(pns);
            // Test avg pushed down by replacing it with sum, count
            checkOptimizedAgg(pns, true);
        }

        sql = "SELECT A1, AVG(PKEY)+1 FROM P1 GROUP BY A1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql, PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.PROJECTION,  // Compute the quotient.
                             allOf(PlanNodeType.AGGREGATE,
                                     new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM,
                                                              ExpressionType.AGGREGATE_SUM)),
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                     new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM,
                                                              ExpressionType.AGGREGATE_COUNT)),
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            printJSONPlan(PRINT_JSON_PLAN, sql);
            pns = compileToFragments(sql);
            checkHasComplexAgg(pns);
            // Test avg pushed down by replacing it with sum, count
            checkOptimizedAgg(pns, true);
        }
    }

    public void testGroupByColsNotInDisplayCols() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByColsNotInDisplayCols();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByColsNotInDisplayCols();
        }
    }

    private void basicTestGroupByColsNotInDisplayCols() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT sum(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT sum(PKEY), sum(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    private void checkGroupByAliasFeature(String sql1, String sql2, boolean exact) {
        List<AbstractPlanNode> pns;
        String explainStr1;
        String explainStr2;

        pns = compileToFragments(sql1);
        explainStr1 = buildExplainPlan(pns);
        pns = compileToFragments(sql2);
        explainStr2 = buildExplainPlan(pns);
        if (! exact) {
            explainStr1 = explainStr1.replaceAll("\\$\\$_VOLT_TEMP_TABLE_\\$\\$.column#[\\d]",
                    "TEMP_TABLE.column#[Index]");
            explainStr2 = explainStr2.replaceAll("\\$\\$_VOLT_TEMP_TABLE_\\$\\$\\.column#[\\d]",
                    "TEMP_TABLE.column#[Index]");
            assertEquals(explainStr1, explainStr2);
        }
        assertEquals(explainStr1, explainStr2);
    }

    public void testGroupByBooleanConstants() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByBooleanConstants();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByBooleanConstants();
        }
    }

    private void basicTestGroupByBooleanConstants() {
        String[] conditions = {"1=1", "1=0", "TRUE", "FALSE", "1>2"};
        for (String condition : conditions) {
            failToCompile("SELECT count(P1.PKEY) FROM P1 GROUP BY " + condition,
                    "A GROUP BY clause does not allow a BOOLEAN expression.");
        }
    }

    public void testGroupByAliasENG9872() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByAliasENG9872();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByAliasENG9872();
        }
    }

    private void basicTestGroupByAliasENG9872() {
        String sql;
        // If we have an alias in a group by clause, and
        // the alias is to an aggregate, we need to reject
        // this.
        sql = "SELECT 2*count(P1.PKEY) AS AAA FROM P1 GROUP BY AAA";
        failToCompile(sql, "invalid GROUP BY expression:  COUNT()");
        // Ambiguity.
        sql = "SELECT P1.PKEY AS AAA, P1.PKEY AS AAA FROM P1 GROUP BY AAA";
        failToCompile(sql, "Group by expression \"AAA\" is ambiguous");
        // More ambiguity.  Also, the count aggregate is used
        // in the group by, but we see the ambiguity first.
        sql = "SELECT 2*count(P1.PKEY) AS AAA, P1.PKEY AS AAA FROM P1 GROUP BY AAA";
        failToCompile(sql, "Group by expression \"AAA\" is ambiguous");
        // This used to fail because we ignored select lists
        // which had no aggregates.  Now we look at all of them.
        compile("SELECT P1.PKEY AS AAA FROM P1 GROUP BY AAA");
    }

    public void testGroupByAliasNegativeCases() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByAliasNegativeCases();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByAliasNegativeCases();
        }
    }

    private void basicTestGroupByAliasNegativeCases() {
        List<AbstractPlanNode> pns;

        // Group by aggregate expression
        try {
            pns = compileInvalidToFragments(
                    "SELECT abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY count(*)");
            fail("Did not expect invalid GROUP BY query to succeed.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("invalid GROUP BY expression:  COUNT()"));
        }

        try {
            pns = compileInvalidToFragments(
                    "SELECT abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY ct");
            fail("Did not expect invalid GROUP BY alias query to succeed.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("invalid GROUP BY expression:  COUNT()"));
        }

        try {
            pns = compileInvalidToFragments(
                    "SELECT abs(PKEY) as sp, (count(*) +1 ) as ct FROM P1 GROUP BY ct");
            fail("Did not expect invalid GROUP BY expression alias query to succeed.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("invalid GROUP BY expression:  COUNT()"));
        }

        // Group by alias and expression
        try {
            pns = compileInvalidToFragments(
                    "SELECT abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY sp + 1");
            fail("Did not expect invalid GROUP BY alias expression query to succeed.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "object not found: SP"));
        }

        // Having
        try {
            pns = compileInvalidToFragments(
                    "SELECT ABS(A1), count(*) as ct FROM P1 GROUP BY ABS(A1) having ct > 3");
            fail("Did not expect invalid HAVING condition on alias query to succeed.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "object not found: CT"));
        }

        // Group by column.alias
        try {
            pns = compileInvalidToFragments(
                    "SELECT abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY P1.sp");
            fail("Did not expect invalid GROUP BY qualified alias query to succeed.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "object not found: P1.SP"));
        }

        String sql;
        sql = "SELECT ABS(A1) AS A1, count(*) as ct FROM P1 GROUP BY A1";
        //
        // ambiguous group by query because of A1 is a column name and a select alias
        //
        pns = compileToFragments(sql);
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.PROJECTION,
                             PlanNodeType.AGGREGATE,
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                   new AggregateNodeMatcher(ExpressionType.AGGREGATE_COUNT_STAR)),
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            AbstractPlanNode p = pns.get(1).getChild(0);
            assertTrue(p instanceof AbstractScanPlanNode);
            AggregatePlanNode agg = AggregatePlanNode.getInlineAggregationNode(p);
            assertNotNull(agg);
            // group by column, instead of the ABS(A1) expression
            assertEquals(agg.getGroupByExpressions().get(0).getExpressionType(), ExpressionType.VALUE_TUPLE);
        }
    }

    public void testGroupByAlias() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestGroupByAlias();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestGroupByAlias();
        }
    }

    private void basicTestGroupByAlias() {
        String sql1, sql2;

        // group by alias for expression
        sql1 = "SELECT abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY sp";
        sql2 = "SELECT abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY abs(PKEY)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // group by multiple alias (expression or column)
        sql1 = "SELECT A1 as A, abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY A, sp";
        sql2 = "SELECT A1 as A, abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY A, abs(PKEY)";
        checkQueriesPlansAreTheSame(sql1, sql2);
        sql2 = "SELECT A1 as A, abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY A1, sp";
        checkQueriesPlansAreTheSame(sql1, sql2);
        sql2 = "SELECT A1 as A, abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY A1, abs(PKEY)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // group by and select in different orders
        sql2 = "SELECT abs(PKEY) as sp, A1 as A, count(*) as ct FROM P1 GROUP BY A, abs(PKEY)";
        checkGroupByAliasFeature(sql1, sql2, false);

        sql2 = "SELECT abs(PKEY) as sp, count(*) as ct, A1 as A FROM P1 GROUP BY A, abs(PKEY)";
        checkGroupByAliasFeature(sql1, sql2, false);

        sql2 = "SELECT count(*) as ct, abs(PKEY) as sp, A1 as A FROM P1 GROUP BY A, abs(PKEY)";
        checkGroupByAliasFeature(sql1, sql2, false);

        sql2 = "SELECT A1 as A, count(*) as ct, abs(PKEY) as sp FROM P1 GROUP BY A, abs(PKEY)";
        checkGroupByAliasFeature(sql1, sql2, false);

        sql2 = "SELECT A1 as A, count(*) as ct, abs(PKEY) as sp FROM P1 GROUP BY abs(PKEY), A";
        checkGroupByAliasFeature(sql1, sql2, false);

        // group by alias with selected constants
        sql1 = "SELECT 1, abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY sp";
        sql2 = "SELECT 1, abs(PKEY) as sp, count(*) as ct FROM P1 GROUP BY abs(PKEY)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // group by alias on joined results
        sql1 = "SELECT abs(P1.PKEY) as sp, count(*) as ct FROM P1, R1 WHERE P1.A1 = R1.A1 GROUP BY sp";
        sql2 = "SELECT abs(P1.PKEY) as sp, count(*) as ct FROM P1, R1 WHERE P1.A1 = R1.A1 GROUP BY abs(P1.PKEY)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // group by expression with constants parameter
        sql1 = "SELECT abs(P1.PKEY + 1) as sp, count(*) as ct FROM P1 GROUP BY sp";
        sql2 = "SELECT abs(P1.PKEY + 1) as sp, count(*) as ct FROM P1 GROUP BY abs(P1.PKEY + 1)";
        checkQueriesPlansAreTheSame(sql1, sql2);

        // group by constants with alias
        sql1 = "SELECT 5 as tag, count(*) as ct FROM P1 GROUP BY tag";
        sql2 = "SELECT 5 as tag, count(*) as ct FROM P1 GROUP BY 5";
        checkQueriesPlansAreTheSame(sql1, sql2);
    }

    private void checkMVNoFix_NoAgg_NormalQueries(
            String sql) {
        // the first '-1' indicates that there is no top aggregation node.
        checkMVReaggreateFeature(sql, false,
                -1, -1,
                -1, -1,
                false, false);
    }

    private void checkMVNoFix_NoAgg_NormalQueries_MergeReceive(
            String sql, SortDirectionType sortDirection) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);
        checkMVReaggregateFeatureMergeReceive(pns, false,
                -1, -1,
                false, false, sortDirection);
    }

    private void checkMVNoFix_NoAgg(
            String sql, int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            boolean aggPushdown, boolean aggInline) {

        checkMVReaggreateFeature(sql, false, numGroupByOfTopAggNode, numAggsOfTopAggNode,
                -1, -1, aggPushdown, aggInline);

    }

    public void testNoFix_MVBasedQuery() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestNoFix_MVBasedQuery();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestNoFix_MVBasedQuery();
        }
    }

    private void basicTestNoFix_MVBasedQuery() {
        String sql;
        List<AbstractPlanNode> pns;

        // (1) Table V_P1_NO_FIX_NEEDED:

        // Normal select queries
        checkMVNoFix_NoAgg_NormalQueries("SELECT * FROM V_P1_NO_FIX_NEEDED");

        sql = "SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED ORDER BY V_A1";
        checkMVNoFix_NoAgg_NormalQueries_MergeReceive(sql, SortDirectionType.ASC);

        sql = "SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED LIMIT 1";
        checkMVNoFix_NoAgg_NormalQueries(sql);

        // Distributed distinct select query
        sql = "SELECT DISTINCT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             PlanNodeType.ORDERBY,
                             PlanNodeType.INDEXSCAN));
        }
        else {
            checkMVNoFix_NoAgg(sql, 1, 0, true, true);
        }

        // Distributed group by query
        sql = "SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED GROUP by V_SUM_C1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            PlanNodeType.ORDERBY,
                            PlanNodeType.INDEXSCAN));
        }
        else {
            checkMVNoFix_NoAgg(sql, 1, 0, true, true);
        }

        sql = "SELECT V_SUM_C1, sum(V_CNT) FROM V_P1_NO_FIX_NEEDED " +
                "GROUP by V_SUM_C1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            PlanNodeType.ORDERBY,
                            PlanNodeType.INDEXSCAN));
        }
        else {
            checkMVNoFix_NoAgg(sql, 1, 1, true, true);
        }

        // (2) Table V_P1 and V_P1_NEW:
        pns = compileToFragments("SELECT SUM(V_SUM_C1) FROM V_P1");
        checkMVReaggregateFeature(pns, false, 0, 1, -1, -1, true, true);

        pns = compileToFragments("SELECT MIN(V_MIN_C1) FROM V_P1_NEW");
        checkMVReaggregateFeature(pns, false, 0, 1, -1, -1, true, true);

        pns = compileToFragments("SELECT MAX(V_MAX_D1) FROM V_P1_NEW");
        checkMVReaggregateFeature(pns, false, 0, 1, -1, -1, true, true);

        sql = "SELECT MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.PROJECTION,
                            allOf(PlanNodeType.AGGREGATE,
                                  new AggregateNodeMatcher(ExpressionType.AGGREGATE_MAX)),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.ORDERBY, // Pushed down from the coordinator.
                            new PlanWithInlineNodes(
                                    new IndexScanPlanMatcher("MATVIEW_PK_INDEX"),
                                    PlanNodeType.PROJECTION,
                                    allOf(PlanNodeType.AGGREGATE,
                                            new AggregateNodeMatcher(ExpressionType.AGGREGATE_MAX)))));
        }
        else {
            checkMVNoFix_NoAgg(sql, 1, 1, true, true);
        }

        sql = "SELECT V_A1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(PlanNodeType.AGGREGATE,
                                    new AggregateNodeMatcher(ExpressionType.AGGREGATE_MAX)),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.ORDERBY, // Pushed down from the coordinator.
                            new PlanWithInlineNodes(
                                    new IndexScanPlanMatcher("MATVIEW_PK_INDEX"),
                                    PlanNodeType.PROJECTION,
                                    allOf(PlanNodeType.AGGREGATE,
                                            new AggregateNodeMatcher(ExpressionType.AGGREGATE_MAX)))));
        }
        else {
            checkMVNoFix_NoAgg(sql, 1, 1, true, true);
        }

        sql = "SELECT V_A1,V_B1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1, V_B1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(PlanNodeType.AGGREGATE,
                                    new AggregateNodeMatcher(ExpressionType.AGGREGATE_MAX)),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.ORDERBY, // Pushed down from the coordinator.
                            new PlanWithInlineNodes(
                                    new IndexScanPlanMatcher("MATVIEW_PK_INDEX"),
                                    PlanNodeType.PROJECTION,
                                    allOf(PlanNodeType.AGGREGATE,
                                            new AggregateNodeMatcher(ExpressionType.AGGREGATE_MAX)))));
        }
        else {
            checkMVNoFix_NoAgg(sql, 2, 1, true, true);
        }

        // (3) Join Query
        // Voter example query in 'Results' stored procedure.
        sql = "   SELECT a.contestant_name   AS contestant_name"
                + "        , a.contestant_number AS contestant_number"
                + "        , SUM(b.num_votes)    AS total_votes"
                + "     FROM v_votes_by_contestant_number_state AS b"
                + "        , contestants AS a"
                + "    WHERE a.contestant_number = b.contestant_number"
                + " GROUP BY a.contestant_name"
                + "        , a.contestant_number"
                + " ORDER BY total_votes DESC"
                + "        , contestant_number ASC"
                + "        , contestant_name ASC;";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY,
                            allOf(PlanNodeType.AGGREGATE,
                                    new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM)),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             allOf(PlanNodeType.AGGREGATE,
                                     new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM)),
                             PlanNodeType.ORDERBY,
                             // NESTLOOPINDEX has an index scan inline node.
                             new PlanWithInlineNodes(PlanNodeType.NESTLOOPINDEX,
                                 new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                         PlanNodeType.PROJECTION)),
                             PlanNodeType.SEQSCAN));
        }
        else {
            checkMVNoFix_NoAgg(sql, 2, 1, true, true);
        }


        sql = "select sum(v_p1.v_cnt) " +
                "from v_p1 INNER JOIN v_r1 using(v_a1)";
        checkMVNoFix_NoAgg(sql, 0, 1, true, true);

        sql = "select v_p1.v_b1, sum(v_p1.v_sum_d1) " +
                "from v_p1 INNER JOIN v_r1 on v_p1.v_a1 > v_r1.v_a1 " +
                "group by v_p1.v_b1;";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            allOf(PlanNodeType.AGGREGATE,
                                    new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM)),
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            allOf(PlanNodeType.AGGREGATE,
                                    new AggregateNodeMatcher(ExpressionType.AGGREGATE_SUM)),
                            PlanNodeType.ORDERBY,
                            // NESTLOOPINDEX has an index scan inline node.
                            new PlanWithInlineNodes(PlanNodeType.NESTLOOPINDEX,
                                    new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                            PlanNodeType.PROJECTION)),
                            new IndexScanPlanMatcher("MATVIEW_PK_INDEX")));
        }
        else {
            checkMVNoFix_NoAgg(sql, 1, 1, true, true);
        }

        sql = "select MAX(v_r1.v_a1) " +
                "from v_p1 INNER JOIN v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "INNER JOIN r1v on v_p1.v_a1 = r1v.v_a1 ";
        checkMVNoFix_NoAgg(sql, 0, 1, true, true);
    }

    public void testMVBasedQuery_EdgeCases() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestMVBasedQuery_EdgeCases();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestMVBasedQuery_EdgeCases();
        }
    }

    private void basicTestMVBasedQuery_EdgeCases() {
        String sql;
        List<AbstractPlanNode> pns;

        // No aggregation will be pushed down.
        sql = "SELECT count(*) FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 0, 1, 2, 0);

        sql = "SELECT SUM(v_a1) FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 0, 1, 2, 0);

        sql = "SELECT count(v_a1) FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 0, 1, 2, 0);

        sql = "SELECT max(v_a1) FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 0, 1, 2, 0);

        // ENG-5386 opposite cases.
        sql = "SELECT SUM(V_SUM_C1+1) FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 0, 1, 2, 1);

        sql = "SELECT SUM(V_SUM_C1) FROM V_P1 WHERE V_SUM_C1 > 3";
        checkMVFix_TopAgg_ReAgg(sql, 0, 1, 2, 1);

        sql = "SELECT V_SUM_C1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_SUM_C1";
        checkMVFix_TopAgg_ReAgg(sql, 1, 1, 2, 2);

        // ENG-5669 HAVING edge cases.
        sql = "SELECT SUM(V_SUM_C1) FROM V_P1 HAVING MAX(V_SUM_D1) > 3";
        checkMVFix_TopAgg_ReAgg(sql, 0, 2, 2, 2);

        sql = "SELECT SUM(V_SUM_C1) FROM V_P1 HAVING SUM(V_SUM_D1) > 3";
        pns = compileToFragments(sql);
        checkMVReaggregateFeature(pns, false, 0, 2, -1, -1, true, true);

        // distinct on the v_a1 (part of the group by columns in the view)
        // aggregate pushed down for optimization
        sql = "SELECT distinct v_a1 FROM V_P1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.AGGREGATE,
                             MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                             PlanNodeType.ORDERBY, // Pushed down from coordinator.
                             new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                     PlanNodeType.PROJECTION,
                                     PlanNodeType.AGGREGATE)));
        }
        else {
            checkMVFix_TopAgg_ReAgg(sql, 1, 0, 1, 0);
        }

        sql = "SELECT v_a1 FROM V_P1 group by v_a1";
        if (isPlanningForLargeQueries()) {
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.AGGREGATE,
                            MergeReceivePlanMatcher),
                    fragSpec(PlanNodeType.SEND,
                            PlanNodeType.ORDERBY, // Pushed down from coordinator.
                            new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                    PlanNodeType.PROJECTION,
                                    PlanNodeType.AGGREGATE)));
        }
        else {
            checkMVFix_TopAgg_ReAgg(sql, 1, 0, 1, 0);
        }

        sql = "SELECT distinct v_cnt FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);

        sql = "SELECT v_cnt FROM V_P1 group by v_cnt";
        checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);
    }

    public void testMVBasedQuery_NoAggQuery() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestMVBasedQuery_NoAggQuery();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestMVBasedQuery_NoAggQuery();
        }
    }

    private void basicTestMVBasedQuery_NoAggQuery() {
        String sql;
        //        CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //        AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //        FROM P1  GROUP BY A1, B1;

        String[] tbs = {"V_P1", "V_P1_ABS"};
        for (String tb: tbs) {
            sql = "SELECT * FROM " + tb;
            checkMVFix_reAgg(sql, 2, 3);

            sql = "SELECT * FROM " + tb + " order by V_A1 DESC";
            checkMVFix_reAgg_MergeReceive(sql, 2, 3, SortDirectionType.DESC);

            sql = "SELECT * FROM " + tb + " order by V_A1, V_B1";
            checkMVFix_reAgg_MergeReceive(sql, 2, 3, SortDirectionType.ASC);

            sql = "SELECT * FROM " + tb + " order by V_SUM_D1";
            checkMVFix_reAgg(sql, 2, 3);

            sql = "SELECT * FROM " + tb + " limit 1";
            checkMVFix_reAgg(sql, 2, 3);

            sql = "SELECT * FROM " + tb + " order by V_A1, V_B1 limit 1";
            checkMVFix_reAgg_MergeReceive(sql, 2, 3, SortDirectionType.ASC);

            sql = "SELECT v_sum_c1 FROM " + tb;
            checkMVFix_reAgg(sql, 2, 1);

            sql = "SELECT v_sum_c1 FROM " + tb + " order by v_sum_c1";
            checkMVFix_reAgg(sql, 2, 1);

            sql = "SELECT v_sum_c1 FROM " + tb + " order by v_sum_d1";
            checkMVFix_reAgg(sql, 2, 2);

            sql = "SELECT v_sum_c1 FROM " + tb + " limit 1";
            checkMVFix_reAgg(sql, 2, 1);

            sql = "SELECT v_sum_c1 FROM " + tb + " order by v_sum_c1 limit 1";
            checkMVFix_reAgg(sql, 2, 1);
        }
    }

    public void testMVBasedQuery_AggQuery() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestMVBasedQuery_AggQuery();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestMVBasedQuery_AggQuery();
        }
    }

    private void basicTestMVBasedQuery_AggQuery() {
        String sql;
        //      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //      FROM P1  GROUP BY A1, B1;

        String[] tbs = {"V_P1", "V_P1_ABS"};

        for (String tb: tbs) {
            // Test set (1): group by
            sql = "SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);

            // because we have order by.
            sql = "SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 " +
                    " ORDER BY V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);

            sql = "SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 " +
                    " ORDER BY V_SUM_C1 LIMIT 5";
            checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);

            sql = "SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 LIMIT 5";
            checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);

            // Test set (2):
            sql = "SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 1, 1, 2, 2);

            sql = "SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1 " +
                    " ORDER BY V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 1, 1, 2, 2);

            sql = "SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1 " +
                    " ORDER BY V_SUM_C1 limit 2";
            checkMVFix_TopAgg_ReAgg(sql, 1, 1, 2, 2);

            // Distinct: No aggregation push down.
            sql = "SELECT V_SUM_C1, sum(distinct V_CNT) " +
                    "FROM " + tb + " GROUP by V_SUM_C1 " +
                    " ORDER BY V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 1, 1, 2, 2);

            // Test set (3)
            sql = "SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 3, 1, 2, 2);

            sql = "SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 " +
                    " ORDER BY V_A1,V_B1, V_SUM_C1";
            checkMVFix_TopAgg_ReAgg(sql, 3, 1, 2, 2);

            sql = "SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 " +
                    " ORDER BY V_A1,V_B1, V_SUM_C1 LIMIT 5";
            checkMVFix_TopAgg_ReAgg(sql, 3, 1, 2, 2);

            sql = "SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 " +
                    " ORDER BY V_A1, V_SUM_C1 LIMIT 5";
            checkMVFix_TopAgg_ReAgg(sql, 3, 1, 2, 2);

            // Distinct: No aggregation push down.
            sql = "SELECT V_A1,V_B1, V_SUM_C1, sum( distinct V_SUM_D1) FROM " +
                    tb + " GROUP BY V_A1,V_B1, V_SUM_C1 " +
                    " ORDER BY V_A1, V_SUM_C1 LIMIT 5";
            checkMVFix_TopAgg_ReAgg(sql, 3, 1, 2, 2);
        }
    }

    private void checkMVFixWithWhere(String sql, String aggFilter, String scanFilter) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);
        //* enable to debug */ printExplainPlan(pns);
        checkMVFixWithWhere(pns,
                ( (aggFilter == null) ? null: new String[] {aggFilter}),
                ( (scanFilter == null) ? null: new String[] {scanFilter}));
    }

    private void checkMVFixWithWhere(String sql, Object aggFilters[]) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);
        //* enable to debug */ printExplainPlan(pns);
        checkMVFixWithWhere(pns, aggFilters, null);
    }

    private void checkMVFixWithWhere(List<AbstractPlanNode> pns,
            Object aggFilters, Object scanFilters) {
        AbstractPlanNode p = pns.get(0);

        List<AbstractPlanNode> nodes = p.findAllNodesOfClass(AbstractReceivePlanNode.class);
        assertEquals(1, nodes.size());
        p = nodes.get(0);

        // Find re-aggregation node.
        assertTrue(p instanceof ReceivePlanNode);
        assertTrue(p.getParent(0) instanceof HashAggregatePlanNode);
        HashAggregatePlanNode reAggNode = (HashAggregatePlanNode) p.getParent(0);
        String reAggNodeStr = reAggNode.toExplainPlanString().toLowerCase();

        // Find scan node.
        p = pns.get(1);
        assertEquals(1, p.getScanNodeList().size());
        p = p.getScanNodeList().get(0);
        String scanNodeStr = p.toExplainPlanString().toLowerCase();

        if (aggFilters != null) {
            String[] aggFilterStrings = null;
            if (aggFilters instanceof String) {
                aggFilterStrings = new String[] { (String) aggFilters };
            }
            else {
                aggFilterStrings = (String[]) aggFilters;
            }
            for (String aggFilter : aggFilterStrings) {
                //* enable to debug */ System.out.println(reAggNodeStr.contains(aggFilter.toLowerCase()));
                assertTrue(reAggNodeStr.contains(aggFilter.toLowerCase()));
                //* enable to debug */ System.out.println(scanNodeStr.contains(aggFilter.toLowerCase()));
                assertFalse(scanNodeStr.contains(aggFilter.toLowerCase()));
            }
        }
        else {
            assertNull(reAggNode.getPostPredicate());
        }

        if (scanFilters != null) {
            String[] scanFilterStrings = null;
            if (scanFilters instanceof String) {
                scanFilterStrings = new String[] { (String) scanFilters };
            }
            else {
                scanFilterStrings = (String[]) scanFilters;
            }
            for (String scanFilter : scanFilterStrings) {
                //* enable to debug */ System.out.println(reAggNodeStr.contains(scanFilter.toLowerCase()));
                assertFalse(reAggNodeStr.contains(scanFilter.toLowerCase()));
                //* enable to debug */ System.out.println(scanNodeStr.contains(scanFilter.toLowerCase()));
                assertTrue(scanNodeStr.contains(scanFilter.toLowerCase()));
            }
        }
    }

    public void testMVBasedQuery_Where() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestMVBasedQuery_Where();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestMVBasedQuery_Where();
        }
    }

    private void basicTestMVBasedQuery_Where() {
        String sql;
        //      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //      FROM P1  GROUP BY A1, B1;
        // Test
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        sql = "SELECT * FROM V_P1 where v_cnt = 1";
        checkMVFixWithWhere(sql, "v_cnt = 1", null);
        sql = "SELECT * FROM V_P1 where v_a1 = 9";
        checkMVFixWithWhere(sql, null, "v_a1 = 9");
        sql = "SELECT * FROM V_P1 where v_a1 = 9 AND v_cnt = 1";
        checkMVFixWithWhere(sql, "v_cnt = 1", "v_a1 = 9");
        sql = "SELECT * FROM V_P1 where v_a1 = 9 OR v_cnt = 1";
        checkMVFixWithWhere(sql, new String[] {"v_a1 = 9) OR ", "v_cnt = 1)"});
        sql = "SELECT * FROM V_P1 where v_a1 = v_cnt + 1";
        checkMVFixWithWhere(sql, new String[] {"v_a1 = (", "v_cnt + 1)"});
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    private void checkMVFixWithJoin_ReAgg(String sql,
            int numGroupByOfReaggNode,
            int numAggsOfReaggNode,
            Object aggFilter,
            String scanFilter) {
        checkMVFixWithJoin(sql, -1, -1,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                aggFilter, scanFilter);
    }

    private void checkMVFixWithJoinLarge(String sqlPattern) {
        String[] joinTypes = {"inner join", "left join", "right join"};
        for (String joinType : joinTypes) {
            String sql = sqlPattern.replace("@joinType", joinType);
            validatePlan(sql,
                    PRINT_JSON_PLAN,
                    fragSpec(PlanNodeType.SEND),
                    fragSpec(PlanNodeType.SEND));
        }
    }

    private void checkMVFixWithJoin(String sql,
            int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupByOfReaggNode, int numAggsOfReaggNode,
            Object aggFilters, Object scanFilters) {
        String[] joinType = {"inner join", "left join", "right join"};

        for (int i = 0; i < joinType.length; i++) {
            List<AbstractPlanNode> pns;
            String newsql = sql.replace("@joinType", joinType[i]);
            pns = compileToFragments(newsql);
            //* enable to debug */ System.err.println("Query:" + newsql);
            // No join node under receive node.
            checkMVReaggregateFeature(pns, true,
                    numGroupByOfTopAggNode, numAggsOfTopAggNode,
                    numGroupByOfReaggNode, numAggsOfReaggNode, false, false);

            checkMVFixWithWhere(pns, aggFilters, scanFilters);
        }
    }

    /**
     * No tested for Outer join, no 'using' unclear column reference tested.
     * Non-aggregation queries.
     */
    public void testMVBasedQuery_Join_NoAgg() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestMVBasedQuery_Join_NoAgg();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestMVBasedQuery_Join_NoAgg();
        }
    }

    private void basicTestMVBasedQuery_Join_NoAgg() {
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        String sql = "";

        // Two tables joins.
        sql = "select v_a1 from v_p1 @joinType v_r1 using(v_a1)";
        checkMVFixWithJoin_ReAgg(sql, 2, 0, null, null);

        sql = "select v_a1 from v_p1 @joinType v_r1 using(v_a1) " +
                "where v_a1 > 1 and v_p1.v_cnt > 2 and v_r1.v_b1 < 3 ";
        checkMVFixWithJoin_ReAgg(sql, 2, 1, "v_cnt > 2", null /* "v_a1 > 1" is optional */);

        sql = "select v_p1.v_cnt from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 ";
        checkMVFixWithJoin_ReAgg(sql, 2, 2,
                new String[] { "v_sum_c1 < 3)", "v_cnt > 1)" }, "v_a1 > 2");

        // join on different columns.
        sql = "select v_p1.v_cnt from v_r1 @joinType v_p1 on v_r1.v_sum_c1 = v_p1.v_sum_d1 ";
        checkMVFixWithJoin_ReAgg(sql, 2, 2, null, null);


        // Three tables joins.
        sql = "select v_r1.v_a1, v_r1.v_cnt from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "@joinType r1v on v_p1.v_a1 = r1v.v_a1 ";
        checkMVFixWithJoin_ReAgg(sql, 2, 0, null, null);

        sql = "select v_r1.v_cnt, v_r1.v_a1 from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt ";
        checkMVFixWithJoin_ReAgg(sql, 2, 1, null, null);

        // join on different columns.
        sql = "select v_p1.v_cnt from v_r1 @joinType v_p1 on v_r1.v_sum_c1 = v_p1.v_sum_d1 " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_sum_c1";
        checkMVFixWithJoin_ReAgg(sql, 2, 2, null, null);

        sql = "select v_r1.v_cnt, v_r1.v_a1 from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 ";
        checkMVFixWithJoin(sql, -1, -1, 2, 2,
                new String[] {"v_cnt > 1", "v_sum_c1 < 3"}, "v_a1 > 2");

        sql = "select v_r1.v_cnt, v_r1.v_a1 from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and " +
                "v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 and r1v.v_sum_c1 > 6";
        checkMVFixWithJoin(sql, -1, -1, 2, 2,
                new String[] {"v_cnt > 1", "v_sum_c1 < 3"}, "v_a1 > 2");
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    /**
     * No tested for Outer join, no 'using' unclear column reference tested.
     * Aggregation queries.
     */
    public void testMVBasedQuery_Join_Agg() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestMVBasedQuery_Join_Agg();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestMVBasedQuery_Join_Agg();
        }
    }

    private void basicTestMVBasedQuery_Join_Agg() {
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        String sql = "";

        // Two tables joins.
        sql = "select sum(v_a1) from v_p1 @joinType v_r1 using(v_a1)";
        checkMVFixWithJoin(sql, 0, 1, 2, 0, null, null);

        sql = "select sum(v_p1.v_a1) from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1";
        checkMVFixWithJoin(sql, 0, 1, 2, 0, null, null);

        sql = "select sum(v_r1.v_a1) from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1";
        checkMVFixWithJoin(sql, 0, 1, 2, 0, null, null);

        sql = "select v_p1.v_b1, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) group by v_p1.v_b1;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 1, 1, 2, 0, null, null);
        }

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) " +
                "group by v_p1.v_b1, v_p1.v_cnt;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 1, 2, 1, null, null);
        }

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_p1.v_a1) from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "where v_p1.v_a1 > 1 AND v_p1.v_cnt < 8 group by v_p1.v_b1, v_p1.v_cnt;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 1, 2, 1, "v_cnt < 8", "v_a1 > 1");
        }

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_p1.v_a1), max(v_p1.v_sum_c1) from v_p1 @joinType v_r1 " +
                "on v_p1.v_a1 = v_r1.v_a1 " +
                "where v_p1.v_a1 > 1 AND v_p1.v_cnt < 8 group by v_p1.v_b1, v_p1.v_cnt;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 2, 2, 2, "v_cnt < 8", "v_a1 > 1");
        }

        sql = "select v_r1.v_b1, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) group by v_r1.v_b1;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 1, 1, 2, 0, null, null);
        }

        sql = "select v_r1.v_b1, v_r1.v_cnt, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) " +
                "group by v_r1.v_b1, v_r1.v_cnt;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 1, 2, 0, null, null);
        }

        sql = "select v_r1.v_b1, v_p1.v_cnt, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) " +
                "group by v_r1.v_b1, v_p1.v_cnt;";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 1, 2, 1, null, null);
        }


        // Three tables joins.
        sql = "select MAX(v_p1.v_a1) from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "@joinType r1v on v_p1.v_a1 = r1v.v_a1 ";
        checkMVFixWithJoin(sql, 0, 1, 2, 0, null, null);

        sql = "select MIN(v_p1.v_cnt) from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt ";
        checkMVFixWithJoin(sql, 0, 1, 2, 1, null, null);

        sql = "select MIN(v_p1.v_cnt) from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "where v_p1.v_cnt > 1 AND v_p1.v_a1 < 5 AND v_r1.v_b1 > 9";
        checkMVFixWithJoin(sql, 0, 1, 2, 1, "v_cnt > 1", "v_a1 < 5");


        sql = "select v_p1.v_cnt, v_p1.v_b1, SUM(v_p1.v_sum_d1) " +
                "from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt @joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "group by v_p1.v_cnt, v_p1.v_b1";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 1, 2, 2, null, null);
        }

        sql = "select v_p1.v_cnt, v_p1.v_b1, SUM(v_p1.v_sum_d1), MAX(v_r1.v_a1)  " +
                "from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt @joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "group by v_p1.v_cnt, v_p1.v_b1";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 2, 2, 2, null, null);
        }

        sql = "select v_p1.v_cnt, v_p1.v_b1, SUM(v_p1.v_sum_d1) " +
                "from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt @joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 " +
                "group by v_p1.v_cnt, v_p1.v_b1 ";
        if (isPlanningForLargeQueries()) {
            checkMVFixWithJoinLarge(sql);
        }
        else {
            checkMVFixWithJoin(sql, 2, 1, 2, 3, new String[]{"v_sum_c1 < 3)", "v_cnt > 1)"}, "v_a1 > 2");
        }
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    public void testENG5385() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestENG5385();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestENG5385();
        }
    }

    private void basicTestENG5385() {
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        String sql = "";

        sql = "select v_p1.v_a1 from v_p1 left join v_r1 on v_p1.v_a1 = v_r1.v_a1 AND v_p1.v_cnt = 2 ";
        checkMVFixWithJoin_ReAgg(sql, 2, 1, "v_cnt = 2", null);

        // When ENG-5385 is fixed, use the next line to check its plan.
//        checkMVFixWithJoin_reAgg(sql, 2, 1, null, null);
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    public void testENG6962DistinctCases() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestENG6962DistinctCases();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestENG6962DistinctCases();
        }
    }

    private void basicTestENG6962DistinctCases() {
        String sql;
        String sql_rewrote;
        sql = "select distinct A1, B1 from R1";
        sql_rewrote = "select A1, B1 from R1 group by A1, B1";
        checkQueriesPlansAreTheSame(sql, sql_rewrote);

        sql = "select distinct A1+B1 from R1";
        sql_rewrote = "select A1+B1 from R1 group by A1+B1";
        checkQueriesPlansAreTheSame(sql, sql_rewrote);
    }

    public void testENG389_Having() {
        if (TEST_NORMAL_SIZE_QUERIES) {
            planForLargeQueries(false);
            basicTestENG389_Having();
        }
        if (TEST_LARGE_QUERIES) {
            planForLargeQueries(true);
            basicTestENG389_Having();
        }
    }

    private void basicTestENG389_Having() {
        String sql;

        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        //      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //      FROM P1  GROUP BY A1, B1;

        sql = "select sum(V_A1) from v_r1 having v_cnt > 3";
        failToCompile(sql, "invalid HAVING expression");

        sql= "select sum(V_A1) from v_r1 having 3 > 3";
        failToCompile(sql, "does not support HAVING clause without aggregation");

        sql = "select V_A1, count(v_cnt) from v_r1 group by v_a1 having count(v_cnt) > 1; ";
        checkHavingClause(sql, true, ".v_cnt) having (c2 > 1)");

        sql = "select sum(V_A1) from v_r1 having avg(v_cnt) > 3; ";
        checkHavingClause(sql, true, ".v_cnt) having (column#1 > 3)");

        sql = "select avg(v_cnt) from v_r1 having avg(v_cnt) > 3; ";
        checkHavingClause(sql, true, ".v_cnt) having (c1 > 3)");

        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    private void checkHavingClause(String sql,
            boolean aggInline,
            Object aggPostFilters) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);

        AbstractPlanNode p = pns.get(0);
        AggregatePlanNode aggNode;

        ArrayList<AbstractPlanNode> nodesList = p.findAllNodesOfType(PlanNodeType.AGGREGATE);
        assertEquals(1, nodesList.size());
        p = nodesList.get(0);

        boolean isInline = p.isInline();
        assertEquals(aggInline, isInline);

        assertTrue(p instanceof AggregatePlanNode);
        aggNode = (AggregatePlanNode) p;

        String aggNodeStr = aggNode.toExplainPlanString().toLowerCase();

        if (aggPostFilters != null) {
            String[] aggFilterStrings = null;
            if (aggPostFilters instanceof String) {
                aggFilterStrings = new String[] { (String) aggPostFilters };
            }
            else {
                aggFilterStrings = (String[]) aggPostFilters;
            }
            for (String aggFilter : aggFilterStrings) {
                assertTrue(aggNodeStr.contains(aggFilter.toLowerCase()));
            }
        }
        else {
            assertNull(aggNode.getPostPredicate());
        }
    }

    private void checkMVFix_reAgg_MergeReceive(
            String sql,
            int numGroupByOfReaggNode, int numAggsOfReaggNode,
            SortDirectionType sortDirection) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);
        checkMVReaggregateFeatureMergeReceive(pns, true,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                false, false, sortDirection);
}

    private void checkMVFix_reAgg(
            String sql,
            int numGroupByOfReaggNode, int numAggsOfReaggNode) {
        checkMVReaggreateFeature(sql, true,
                -1, -1,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                false, false);
    }

    private void checkMVFix_TopAgg_ReAgg(
            String sql,
            int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupByOfReaggNode, int numAggsOfReaggNode) {

        checkMVReaggreateFeature(sql, true,
                numGroupByOfTopAggNode, numAggsOfTopAggNode,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                false, false);
    }

    // topNode, reAggNode
    private void checkMVReaggreateFeature(
            String sql, boolean needFix,
            int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupByOfReaggNode, int numAggsOfReaggNode,
            boolean aggPushdown, boolean aggInline) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);
        checkMVReaggregateFeature(pns, needFix,
                numGroupByOfTopAggNode, numAggsOfTopAggNode,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                aggPushdown, aggInline);
    }

    // topNode, reAggNode
    private void checkMVReaggregateFeatureMergeReceive(List<AbstractPlanNode> pns,
            boolean needFix,
            int numGroupByOfReaggNode,
            int numAggsOfReaggNode,
            boolean aggPushdown,
            boolean aggInline,
            SortDirectionType sortDirection) {

        assertEquals(2, pns.size());
        AbstractPlanNode p = pns.get(0);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);
        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        AbstractPlanNode receiveNode = p;
        assertNotNull(receiveNode);

        AggregatePlanNode reAggNode = AggregatePlanNode.getInlineAggregationNode(receiveNode);

        if (needFix) {
            assertNotNull(reAggNode);

            assertEquals(numGroupByOfReaggNode, reAggNode.getGroupByExpressionsSize());
            assertEquals(numAggsOfReaggNode, reAggNode.getAggregateTypesSize());
        }
        else {
            assertNull(reAggNode);
        }

        p = pns.get(1);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);

        assertTrue(p instanceof IndexScanPlanNode);
        assertEquals(sortDirection, ((IndexScanPlanNode)p).getSortDirection());

    }

    // topNode, reAggNode
    private void checkMVReaggregateFeature(List<AbstractPlanNode> pns,
            boolean needFix,
            int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupByOfReaggNode, int numAggsOfReaggNode,
            boolean aggPushdown, boolean aggInline) {
        String sql;
        AbstractPlanNode p;

        assertEquals(2, pns.size());
        p = pns.get(0);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);

        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }

        if (p instanceof LimitPlanNode) {
            // No limit pushed down.
            p = p.getChild(0);
        }

        if (p instanceof OrderByPlanNode) {
            p = p.getChild(0);
        }
        HashAggregatePlanNode reAggNode = null;

        List<AbstractPlanNode> nodes = p.findAllNodesOfClass(AbstractReceivePlanNode.class);
        assertEquals(1, nodes.size());
        AbstractPlanNode receiveNode = nodes.get(0);

        // Indicates that there is no top aggregation node.
        if (numGroupByOfTopAggNode == -1) {
            if (needFix) {
                p = receiveNode.getParent(0);
                assertTrue(p instanceof HashAggregatePlanNode);
                reAggNode = (HashAggregatePlanNode) p;

                assertEquals(numGroupByOfReaggNode, reAggNode.getGroupByExpressionsSize());
                assertEquals(numAggsOfReaggNode, reAggNode.getAggregateTypesSize());

                p = p.getChild(0);
            }
            assertTrue(p instanceof ReceivePlanNode);

            p = pns.get(1);
            assertTrue(p instanceof SendPlanNode);
            p = p.getChild(0);

            assertTrue(p instanceof AbstractScanPlanNode);
            return;
        }

        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }

        //
        // Hash top aggregate node
        //
        AggregatePlanNode topAggNode = null;
        if (p instanceof AbstractJoinPlanNode) {
            // Inline aggregation with join
            topAggNode = AggregatePlanNode.getInlineAggregationNode(p);
        }
        else {
            assertTrue(p instanceof AggregatePlanNode);
            topAggNode = (AggregatePlanNode) p;
            p = p.getChild(0);
        }
        assertEquals(numGroupByOfTopAggNode, topAggNode.getGroupByExpressionsSize());
        assertEquals(numAggsOfTopAggNode, topAggNode.getAggregateTypesSize());

        if (needFix) {
            p = receiveNode.getParent(0);
            assertTrue(p instanceof HashAggregatePlanNode);
            reAggNode = (HashAggregatePlanNode) p;

            assertEquals(numGroupByOfReaggNode, reAggNode.getGroupByExpressionsSize());
            assertEquals(numAggsOfReaggNode, reAggNode.getAggregateTypesSize());

            p = p.getChild(0);
        }
        assertTrue(p instanceof ReceivePlanNode);

        // Test the second part
        p = pns.get(1);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);

        if (aggPushdown) {
            assertTrue(!needFix);
            if (aggInline) {
                assertNotNull(AggregatePlanNode.getInlineAggregationNode(p));
            }
            else {
                assertTrue(p instanceof AggregatePlanNode);
                p = p.getChild(0);
            }
        }

        if (needFix) {
            assertTrue(p instanceof AbstractScanPlanNode);
        }
        else {
            assertTrue(p instanceof AbstractScanPlanNode ||
                    p instanceof AbstractJoinPlanNode);
        }

    }
}
