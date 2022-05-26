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

import static org.voltdb.types.ExpressionType.AGGREGATE_APPROX_COUNT_DISTINCT;
import static org.voltdb.types.ExpressionType.AGGREGATE_AVG;
import static org.voltdb.types.ExpressionType.AGGREGATE_COUNT;
import static org.voltdb.types.ExpressionType.AGGREGATE_HYPERLOGLOGS_TO_CARD;
import static org.voltdb.types.ExpressionType.AGGREGATE_MAX;
import static org.voltdb.types.ExpressionType.AGGREGATE_MIN;
import static org.voltdb.types.ExpressionType.AGGREGATE_SUM;
import static org.voltdb.types.ExpressionType.AGGREGATE_VALS_TO_HYPERLOGLOG;

import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.types.ExpressionType;

/**
 * Mostly here we're concerned that an APPROX_COUNT_DISTINCT aggregate function is
 * handled correctly in both single- and multi-partition contexts.  In a single partition context,
 * we expect APPROX_COUNT_DISTINCT to be appear in the plan.  This is the simplest case.
 *
 * For multi-part plans, there are two possibilities:
 * - APPROX_COUNT_DISTINCT is accompanied by other aggregates that cannot be pushed down
 *   (e.g., count(distinct col)), in this case, we must ship all the rows to the coordinator,
 *   so we expect to just evaluate APPROX_COUNT_DISTINCT on the coordinator.
 * - APPROX_COUNT_DISTINCT appears as the only aggregate on the select list, or all the other
 *   aggregates can be pushed down.  In this case, we "split" the aggregate function to two:
 *   - ROWS_TO_HYPERLOGLOG, which produces a hyperloglog for each partition
 *   - One coordinator, HYPERLOGLOGS_TO_CARD which produces the estimate (as a double)
 * @author cwolff
 *
 */
public class TestPlansApproxCountDistinct extends PlannerTestCase {

    private static final int COORDINATOR_FRAG = 0;
    private static final int PARTITION_FRAG = 1;

    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-count-ddl.sql"),
                    "testcount", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertAggPlanNodeContainsFunctions(AggregatePlanNode node, ExpressionType[] expectedAggFns) {
        List<ExpressionType> actualAggFns = node.getAggregateTypes();

        assertEquals("Wrong number of aggregate functions in plan", expectedAggFns.length, actualAggFns.size());

        int i = 0;
        for (ExpressionType expectedAggFn : expectedAggFns) {
            assertEquals("Found unexpected agg function", expectedAggFn, actualAggFns.get(i));
            ++i;
        }
    }

    private void assertFragContainsAggWithFunctions(AbstractPlanNode frag, ExpressionType... expectedAggFns) {
        List<AbstractPlanNode> aggNodes = findAllAggPlanNodes(frag);
        assertFalse("No aggregation node in fragment!", 0 == aggNodes.size());
        assertEquals("More than one aggregation node in fragment!", 1, aggNodes.size());

        AggregatePlanNode aggNode = (AggregatePlanNode)aggNodes.get(0);
        assertAggPlanNodeContainsFunctions(aggNode, expectedAggFns);
    }

    private void assertFragContainsTwoAggsWithFunctions(AbstractPlanNode frag,
            ExpressionType[] expectedAggFnsFirst,
            ExpressionType[] expectedAggFnsSecond) {
        List<AbstractPlanNode> aggNodes = findAllAggPlanNodes(frag);
        assertEquals("Wrong number of aggregation nodes in fragment!", 2, aggNodes.size());

        assertAggPlanNodeContainsFunctions((AggregatePlanNode)aggNodes.get(0), expectedAggFnsFirst);
        assertAggPlanNodeContainsFunctions((AggregatePlanNode)aggNodes.get(1), expectedAggFnsSecond);
    }

    private void assertFragContainsNoAggPlanNodes(AbstractPlanNode node) {
        List<AbstractPlanNode> aggNodes = findAllAggPlanNodes(node);
        assertEquals("Found an aggregation node in fragment, but didn't expect to!", 0, aggNodes.size());
    }

    public void testSinglePartitionTableAgg() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments("SELECT approx_count_distinct(age) from T1");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), AGGREGATE_APPROX_COUNT_DISTINCT);

        pn = compileToFragments("select approx_count_distinct(age), sum(points) from t1");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT,
                AGGREGATE_SUM);

        pn = compileToFragments("select approx_count_distinct(age), sum(distinct points) from t1");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT,
                AGGREGATE_SUM);

    }

    public void testSinglePartitionWithGroupBy() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments(
                "SELECT id, approx_count_distinct(age) "
                + "from T1 "
                + "group by id");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), AGGREGATE_APPROX_COUNT_DISTINCT);

        pn = compileToFragments(
                "select age, approx_count_distinct(points), max(username) "
                + "from t2 "
                + "group by age");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT,
                AGGREGATE_MAX);

        pn = compileToFragments(
                "select username, approx_count_distinct(age), avg(distinct points) "
                + "from t2 "
                + "group by username");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT,
                AGGREGATE_AVG);
    }

    public void testMultiPartitionTableAgg() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments("SELECT approx_count_distinct(num) from P1");
        assertEquals(2,  pn.size());

        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), AGGREGATE_HYPERLOGLOGS_TO_CARD);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG), AGGREGATE_VALS_TO_HYPERLOGLOG);

        // Two push-down-able aggs.
        pn = compileToFragments("SELECT approx_count_distinct(num), count(ratio) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_COUNT);

        // Three push-down-able aggs.
        pn = compileToFragments("SELECT approx_count_distinct(num), min(desc), max(ratio) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_MIN, AGGREGATE_MAX);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_MIN, AGGREGATE_MAX);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        pn = compileToFragments("SELECT approx_count_distinct(num), count(distinct id) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_COUNT);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        // Also, with approx count distinct with partition key as argument.
        pn = compileToFragments("SELECT approx_count_distinct(id), count(distinct id) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_COUNT);

        // With an agg that cannot be pushed down,
        pn = compileToFragments("SELECT sum(distinct ratio), approx_count_distinct(num) from P1");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_SUM,
                AGGREGATE_APPROX_COUNT_DISTINCT);
        assertFragContainsNoAggPlanNodes(pn.get(PARTITION_FRAG));
    }

    public void testMultiPartitionWithGroupBy() throws Exception {
        List<AbstractPlanNode> pn = compileToFragments(
                "SELECT desc as modid, approx_count_distinct(num) "
                + "from P1 "
                + "group by desc");
        assertEquals(2,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG), AGGREGATE_HYPERLOGLOGS_TO_CARD);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG), AGGREGATE_VALS_TO_HYPERLOGLOG);

        // Two push-down-able aggs.
        pn = compileToFragments("SELECT desc, approx_count_distinct(num), count(ratio) "
                + "from P1 "
                + "group by desc");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_COUNT);

        // A case similar to above.
        pn = compileToFragments(
                "SELECT desc, approx_count_distinct(num), max(ratio) "
                + "from P1 "
                + "group by desc");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_MAX);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_MAX);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        pn = compileToFragments(
                "SELECT ratio, approx_count_distinct(num), count(distinct id) "
                + "from P1 "
                + "group by ratio");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_COUNT);

        // With an agg that can be pushed down, but only because its argument is a partition key.
        // Also, with approx count distinct with partition key as argument.
        pn = compileToFragments(
                "SELECT desc, approx_count_distinct(id), count(distinct id) "
                + "from P1 "
                + "group by desc");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG,
                AGGREGATE_COUNT);

        // With partition key as group by key.
        // In this case, all aggregation can be done on partitions,
        // coordinator just concatenates result
        pn = compileToFragments(
                "SELECT id, sum(distinct ratio), approx_count_distinct(num) "
                + "from P1 "
                + "group by id");
        assertFragContainsNoAggPlanNodes(pn.get(COORDINATOR_FRAG));
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_SUM,
                AGGREGATE_APPROX_COUNT_DISTINCT);
    }

    public void testWithSubqueries() throws Exception {

        // Single-partition statement with a subquery (table agg)
        List<AbstractPlanNode> pn = compileToFragments(
                "select * "
                + "from "
                + "  T1, "
                + "  (select approx_count_distinct(age) from t1) as subq");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT);

        // Single-partition statement with a subquery (with group by)
        pn = compileToFragments(
                "select * "
                + "from "
                + "  (select username, approx_count_distinct(age), avg(distinct points) "
                + "   from t2 "
                + "   group by username) as subq"
                + "  inner join t2 "
                + "  on t2.username = subq.username;");
        assertEquals(1,  pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT,
                AGGREGATE_AVG);

        // multi-partition table agg
        pn = compileToFragments(
                "select * "
                + "from "
                + "t1, "
                + "(SELECT sum(distinct ratio), approx_count_distinct(num) from P1) as subq");
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_SUM,
                AGGREGATE_APPROX_COUNT_DISTINCT);
        assertFragContainsNoAggPlanNodes(pn.get(PARTITION_FRAG));

        // single-part plan on partitioned tables, with GB in subquery
        pn = compileToFragments(
                "select * "
                + "from p1 "
                + "inner join "
                + "(SELECT id, sum(distinct ratio), approx_count_distinct(num) "
                + "from P1 "
                + "where id = 10 "
                + "group by id) as subq "
                + "on subq.id = p1.id");
        assertEquals(1, pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_SUM,
                AGGREGATE_APPROX_COUNT_DISTINCT);

        // multi-part plan on partitioned tables, with GB in subquery
        pn = compileToFragments(
                "select * "
                + "from t1 "
                + "inner join "
                + "(SELECT id, approx_count_distinct(num) "
                + "from P1 "
                + "group by id) as subq "
                + "on subq.id = t1.id");
        for (AbstractPlanNode n : pn) {
            System.out.println(n.toExplainPlanString());
        }
        assertEquals(2, pn.size());
        assertFragContainsNoAggPlanNodes(pn.get(COORDINATOR_FRAG));
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT);
    }

    public void testSubqueriesWithMultipleAggs() throws Exception {
        List<AbstractPlanNode> pn;

        // In this query, one agg plan node is distributed across fragments (p1),
        // but the other is not (t1).
        pn = compileToFragments("select approx_count_distinct(num) "
                + "from (select approx_count_distinct(points) from t1) as repl_subquery,"
                + "  p1");
        assertEquals(2, pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_HYPERLOGLOGS_TO_CARD);
        assertFragContainsTwoAggsWithFunctions(pn.get(PARTITION_FRAG),
                new ExpressionType[] {AGGREGATE_APPROX_COUNT_DISTINCT},
                new ExpressionType[] {AGGREGATE_VALS_TO_HYPERLOGLOG});

        // Like above but with some more aggregate functions
        // (which breaks the push-down-ability of distributed agg)
        pn = compileToFragments("select approx_count_distinct(num), sum(distinct num) "
                + "from (select approx_count_distinct(points) from t1) as repl_subquery,"
                + "  p1");
        assertEquals(2, pn.size());
        assertFragContainsAggWithFunctions(pn.get(COORDINATOR_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT,
                AGGREGATE_SUM);
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_APPROX_COUNT_DISTINCT);

        // As above but partitioned and replicated tables are swapped.
        pn = compileToFragments("select approx_count_distinct(points) "
                + "from (select approx_count_distinct(num) from p1) as repl_subquery,"
                + "  t1");
        assertEquals(2, pn.size());
        assertFragContainsTwoAggsWithFunctions(pn.get(COORDINATOR_FRAG),
                new ExpressionType[] {AGGREGATE_HYPERLOGLOGS_TO_CARD},
                new ExpressionType[] {AGGREGATE_APPROX_COUNT_DISTINCT});
        assertFragContainsAggWithFunctions(pn.get(PARTITION_FRAG),
                AGGREGATE_VALS_TO_HYPERLOGLOG);

        // Like above but with some more aggregate functions
        // (which breaks the push-down-ability of distributed agg)
        pn = compileToFragments("select approx_count_distinct(points) "
                + "from (select approx_count_distinct(num), sum(distinct num) from p1) as repl_subquery,"
                + "  t1");
        assertEquals(2, pn.size());
        assertFragContainsTwoAggsWithFunctions(pn.get(COORDINATOR_FRAG),
                new ExpressionType[] {AGGREGATE_APPROX_COUNT_DISTINCT, AGGREGATE_SUM},
                new ExpressionType[] {AGGREGATE_APPROX_COUNT_DISTINCT});
        assertFragContainsNoAggPlanNodes(pn.get(PARTITION_FRAG));
    }
}
