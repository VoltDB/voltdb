/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class TestPlansGroupBy extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansGroupBy.class.getResource("testplans-groupby-ddl.sql"),
                "testplansgroupby", false);
        AbstractPlanNode.enableVerboseExplainForDebugging();
        AbstractExpression.enableVerboseExplainForDebugging();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    List<AbstractPlanNode> pns = new ArrayList<AbstractPlanNode>();

    public void testInlineSerialAgg_noGroupBy() {
        checkSimpleTableInlineAgg("SELECT SUM(A1) from T1");
        checkSimpleTableInlineAgg("SELECT MIN(A1) from T1");
        checkSimpleTableInlineAgg("SELECT MAX(A1) from T1");
        checkSimpleTableInlineAgg("SELECT COUNT(A1) from T1");

        checkSimpleTableInlineAgg("SELECT SUM(A1), COUNT(A1) from T1");

        // There is no index defined on column B3
        checkSimpleTableInlineAgg("SELECT SUM(A3) from T3 WHERE B3 > 3");
        checkSimpleTableInlineAgg("SELECT MIN(A3) from T3 WHERE B3 > 3");
        checkSimpleTableInlineAgg("SELECT MAX(A3) from T3 WHERE B3 > 3");
        checkSimpleTableInlineAgg("SELECT COUNT(A3) from T3 WHERE B3 > 3");

        // Index scan
        checkSimpleTableInlineAgg("SELECT SUM(A3) from T3 WHERE PKEY > 3");
        checkSimpleTableInlineAgg("SELECT MIN(A3) from T3 WHERE PKEY > 3");
        checkSimpleTableInlineAgg("SELECT MAX(A3) from T3 WHERE PKEY > 3");
        checkSimpleTableInlineAgg("SELECT COUNT(A3) from T3 WHERE PKEY > 3");
    }

    private void checkSimpleTableInlineAgg(String sql) {
        AbstractPlanNode p;
        pns = compileToFragments(sql);
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.PROJECTION));
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
    }

    // AVG is optimized with SUM / COUNT, generating extra projection node
    // In future, inline projection for aggregation.
    public void testInlineSerialAgg_noGroupBy_special() {
      AbstractPlanNode p;
      pns = compileToFragments("SELECT AVG(A1) from T1");
      for (AbstractPlanNode apn: pns) {
          System.out.println(apn.toExplainPlanString());
      }
      p = pns.get(0).getChild(0);
      assertTrue(p instanceof ProjectionPlanNode);
      assertTrue(p.getChild(0) instanceof AggregatePlanNode);
      assertTrue(p.getChild(0).getChild(0) instanceof ReceivePlanNode);

      p = pns.get(1).getChild(0);
      assertTrue(p instanceof SeqScanPlanNode);
      assertNotNull(p.getInlinePlanNode(PlanNodeType.PROJECTION));
      assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
    }

    public void testCountA1() {
        pns = compileToFragments("SELECT count(A1) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountStar()
    {
        pns = compileToFragments("SELECT count(*) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountDistinctA1() {
        pns = compileToFragments("SELECT count(distinct A1) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testDistinctA1_Subquery() {
        AbstractPlanNode p;
        pns = compileToFragments("select * from (SELECT DISTINCT A1 FROM T1) temp");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof SeqScanPlanNode);
        assertTrue(p.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof DistinctPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof DistinctPlanNode);
        assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);
    }

    public void testDistinctA1() {
        pns = compileToFragments("SELECT DISTINCT A1 FROM T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testGroupByA1() {
        AbstractPlanNode p;
        AggregatePlanNode aggNode;
        pns = compileToFragments("SELECT A1 from T1 group by A1");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        // No index, inline hash aggregate
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        // Having
        pns = compileToFragments("SELECT A1, count(*) from T1 group by A1 Having count(*) > 3");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        aggNode = (AggregatePlanNode)p;
        assertNotNull(aggNode.getPostPredicate());
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        // No index, inline hash aggregate
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        aggNode = (AggregatePlanNode)p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertNull(aggNode.getPostPredicate());

    }

    private void checkGroupByPartitionKey(boolean topAgg, boolean having) {
        AbstractPlanNode p;
        AggregatePlanNode aggNode;

        p = pns.get(0).getChild(0);
        if (topAgg) {
            assertTrue(p instanceof AggregatePlanNode);
            if (having) {
                aggNode = (AggregatePlanNode)p;
                assertNotNull(aggNode.getPostPredicate());
            }
            p = p.getChild(0);
        }
        assertTrue(p instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        // inline aggregate
        assertTrue(p instanceof AbstractScanPlanNode);

        PlanNodeType aggType = PlanNodeType.HASHAGGREGATE;
        if (p instanceof IndexScanPlanNode &&
                ((IndexScanPlanNode)p).isForGroupingOnly() ) {
            aggType = PlanNodeType.AGGREGATE;
        }
        assertNotNull(p.getInlinePlanNode(aggType));

        if (having && !topAgg) {
            aggNode = (AggregatePlanNode)p.getInlinePlanNode(aggType);
            assertNotNull(aggNode.getPostPredicate());
        }
    }

    public void testGroupByPartitionKey() {
        // Primary key is equal to partition key
        pns = compileToFragments("SELECT PKEY, COUNT(*) from T1 group by PKEY");
        // "its primary key index (for optimized grouping only)"
        // Not sure why not use serial aggregate instead
        checkGroupByPartitionKey(false, false);

        // Test Having expression
        pns = compileToFragments("SELECT PKEY, COUNT(*) from T1 group by PKEY Having count(*) > 3");
        checkGroupByPartitionKey(false, true);

        // Primary key is not equal to partition key
        pns = compileToFragments("SELECT A3, COUNT(*) from T3 group by A3");
        checkGroupByPartitionKey(false, false);

        // Test Having expression
        pns = compileToFragments("SELECT A3, COUNT(*) from T3 group by A3 Having count(*) > 3");
        checkGroupByPartitionKey(false, true);


        // Group by partition key and others
        pns = compileToFragments("SELECT B3, A3, COUNT(*) from T3 group by B3, A3");
        checkGroupByPartitionKey(false, false);

        // Test Having expression
        pns = compileToFragments("SELECT B3, A3, COUNT(*) from T3 group by B3, A3 Having count(*) > 3");
        checkGroupByPartitionKey(false, true);
    }

    public void testGroupByPartitionKey_Negative() {
        pns = compileToFragments("SELECT ABS(PKEY), COUNT(*) from T1 group by ABS(PKEY)");
        checkGroupByPartitionKey(true, false);

        pns = compileToFragments("SELECT ABS(PKEY), COUNT(*) from T1 group by ABS(PKEY) Having count(*) > 3");
        checkGroupByPartitionKey(true, true);
    }

    // Group by with index
    private void checkGroupByOnlyPlan(boolean twoFragments, boolean isHashAggregator,
            boolean isIndexScan) {
        AbstractPlanNode apn = pns.get(0).getChild(0);
        if (twoFragments) {
            assertTrue(apn.getPlanNodeType() == PlanNodeType.HASHAGGREGATE);
            apn = pns.get(1).getChild(0);
        }

        // For a single table aggregate, it is inline always.
        assertTrue(apn.getPlanNodeType() == (isIndexScan ? PlanNodeType.INDEXSCAN : PlanNodeType.SEQSCAN));
        if (isHashAggregator) {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        } else {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.AGGREGATE));
        }
    }

    public void testGroupByOnly() {
        System.out.println("Starting testGroupByOnly");
        // Replicated Table

        // only GROUP BY cols in SELECT clause
        pns = compileToFragments("SELECT F_D1 FROM RF GROUP BY F_D1");
        checkGroupByOnlyPlan(false, false, true);

        // SELECT cols in GROUP BY and other aggregate cols
        pns = compileToFragments("SELECT F_D1, COUNT(*) FROM RF GROUP BY F_D1");
        checkGroupByOnlyPlan(false, false, true);

        // aggregate cols are part of keys of used index
        pns = compileToFragments("SELECT F_VAL1, SUM(F_VAL2) FROM RF GROUP BY F_VAL1");
        checkGroupByOnlyPlan(false, false, true);

        // expr index, full indexed case
        pns = compileToFragments("SELECT F_D1 + F_D2, COUNT(*) FROM RF GROUP BY F_D1 + F_D2");
        checkGroupByOnlyPlan(false, false, true);

        // function index, prefix indexed case
        pns = compileToFragments("SELECT ABS(F_D1), COUNT(*) FROM RF GROUP BY ABS(F_D1)");
        checkGroupByOnlyPlan(false, false, true);

        // order of GROUP BY cols is different of them in index definition
        // index on (ABS(F_D1), F_D2 - F_D3), GROUP BY on (F_D2 - F_D3, ABS(F_D1))
        pns = compileToFragments("SELECT F_D2 - F_D3, ABS(F_D1), COUNT(*) FROM RF GROUP BY F_D2 - F_D3, ABS(F_D1)");
        checkGroupByOnlyPlan(false, false, true);

        pns = compileToFragments("SELECT F_VAL1, F_VAL2, COUNT(*) FROM RF GROUP BY F_VAL2, F_VAL1");
        //*/ debug */ System.out.println("DEBUG: " + pns.get(0).toExplainPlanString());
        checkGroupByOnlyPlan(false, false, true);
        System.out.println("Finishing testGroupByOnly");

        // unoptimized case (only use second col of the index), but will be replaced in
        // SeqScanToIndexScan optimization for deterministic reason
        // use EXPR_RF_TREE1 not EXPR_RF_TREE2
        pns = compileToFragments("SELECT F_D2 - F_D3, COUNT(*) FROM RF GROUP BY F_D2 - F_D3");
        checkGroupByOnlyPlan(false, true, true);

        // unoptimized case: index is not scannable
        pns = compileToFragments("SELECT F_VAL3, COUNT(*) FROM RF GROUP BY F_VAL3");
        checkGroupByOnlyPlan(false, true, true);

        // unoptimized case: F_D2 is not prefix indexable
        pns = compileToFragments("SELECT F_D2, COUNT(*) FROM RF GROUP BY F_D2");
        checkGroupByOnlyPlan(false, true, true);

        // unoptimized case: no prefix index found for (F_D1, F_D2)
        pns = compileToFragments("SELECT F_D1, F_D2, COUNT(*) FROM RF GROUP BY F_D1, F_D2");
        checkGroupByOnlyPlan(false, true, true);

        // Partitioned Table
        pns = compileToFragments("SELECT F_D1 FROM F GROUP BY F_D1");
        // index scan for group by only, no need using hash aggregate
        checkGroupByOnlyPlan(true, false, true);

        pns = compileToFragments("SELECT F_D1, COUNT(*) FROM F GROUP BY F_D1");
        //*/ debug */ System.out.println("DEBUG: " + pns.get(0).toExplainPlanString());
        //*/ debug */ System.out.println("DEBUG: " + pns.get(1).toExplainPlanString());
        checkGroupByOnlyPlan(true, false, true);

        pns = compileToFragments("SELECT F_VAL1, SUM(F_VAL2) FROM F GROUP BY F_VAL1");
        checkGroupByOnlyPlan(true, false, true);

        pns = compileToFragments("SELECT F_D1 + F_D2, COUNT(*) FROM F GROUP BY F_D1 + F_D2");
        checkGroupByOnlyPlan(true, false, true);

        pns = compileToFragments("SELECT ABS(F_D1), COUNT(*) FROM F GROUP BY ABS(F_D1)");
        checkGroupByOnlyPlan(true, false, true);

        pns = compileToFragments("SELECT F_D2 - F_D3, ABS(F_D1), COUNT(*) FROM F GROUP BY F_D2 - F_D3, ABS(F_D1)");
        checkGroupByOnlyPlan(true, false, true);

        // unoptimized case (only uses second col of the index), will not be replaced in
        // SeqScanToIndexScan for determinism because of non-deterministic receive.
        // Use primary key index
        pns = compileToFragments("SELECT F_D2 - F_D3, COUNT(*) FROM F GROUP BY F_D2 - F_D3");
        checkGroupByOnlyPlan(true, true, true);

        // unoptimized case (only uses second col of the index), will be replaced in
        // SeqScanToIndexScan for determinism.
        // use EXPR_F_TREE1 not EXPR_F_TREE2
        pns = compileToFragments("SELECT F_D2 - F_D3, COUNT(*) FROM RF GROUP BY F_D2 - F_D3");
        //*/ debug */ System.out.println(pns.get(0).toExplainPlanString());
        System.out.println("DEBUG 2: " + pns.get(0).getChild(0).toExplainPlanString());
        checkGroupByOnlyPlan(false, true, true);
    }

    public void testEdgeComplexRelatedCases() {
        pns = compileToFragments("select PKEY+A1 from T1 Order by PKEY+A1");
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);

        // Useless order by clause.
        pns = compileToFragments("SELECT count(*)  FROM P1 order by PKEY");
        for ( AbstractPlanNode nd : pns) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);
        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);

        pns = compileToFragments("SELECT A1, count(*) as tag FROM P1 group by A1 order by tag, A1 limit 1");
        p = pns.get(0).getChild(0);

        // ENG-5066: now Limit is pushed under Projection
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof LimitPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        // inline aggregate
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        pns = compileToFragments("SELECT F_D1, count(*) as tag FROM RF group by F_D1 order by tag");
        p = pns.get(0).getChild(0);
        /*/ to debug */ System.out.println("DEBUG: " + p.toExplainPlanString());
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        p = p.getChild(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));

        pns = compileToFragments("SELECT F_D1, count(*) FROM RF group by F_D1 order by 2");
        p = pns.get(0).getChild(0);
        /*/ to debug */ System.out.println("DEBUG: " + p.toExplainPlanString());
        assertTrue(p instanceof ProjectionPlanNode);
        //assertTrue(p.getChild(0) instanceof LimitPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        p = p.getChild(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
    }

    private void checkHasComplexAgg(List<AbstractPlanNode> pns) {
        assertTrue(pns.size() > 0);
        boolean isDistributed = pns.size() > 1 ? true: false;

        for ( AbstractPlanNode nd : pns) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }

        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        while ( p.getChildCount() > 0) {
            p = p.getChild(0);
            assertFalse(p instanceof ProjectionPlanNode);
        }

        if (isDistributed) {
            p = pns.get(1).getChild(0);
            assertFalse(p instanceof ProjectionPlanNode);
        }
    }

    public void testComplexAggwithLimit() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2");
        checkHasComplexAgg(pns);

        // Test limit push down
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof LimitPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof LimitPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);

        p = p.getChild(0).getChild(0);
        // inline aggregate
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

    }

    public void testComplexAggwithDistinct() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1");
        checkHasComplexAgg(pns);

        // Test aggregation node not push down with distinct
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
    }

    public void testComplexAggwithLimitDistinct() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2");
        checkHasComplexAgg(pns);

        // Test no limit push down
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof LimitPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof OrderByPlanNode);
        assertTrue(p.getChild(0).getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
    }

    public void testComplexAggCase() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY) as A2, (SUM(PKEY) / 888) as A3, (SUM(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY), COUNT(PKEY), (AVG(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    public void testComplexGroupby() {
        pns = compileToFragments("SELECT A1, ABS(A1), ABS(A1)+1, sum(B1) FROM P1 GROUP BY A1, ABS(A1)");
        checkHasComplexAgg(pns);

        // Check it can compile
        pns = compileToFragments("SELECT ABS(A1), sum(B1) FROM P1 GROUP BY ABS(A1)");
        AbstractPlanNode p = pns.get(0).getChild(0);
        //
        assertTrue(p instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        // inline aggregate
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        pns = compileToFragments("SELECT A1+PKEY, avg(B1) as tag FROM P1 GROUP BY A1+PKEY ORDER BY ABS(tag), A1+PKEY");
        checkHasComplexAgg(pns);
    }

    private void checkOptimizedAgg (List<AbstractPlanNode> pns, boolean optimized) {
        AbstractPlanNode p = pns.get(0).getChild(0);
        if (optimized) {
            assertTrue(p instanceof ProjectionPlanNode);
            assertTrue(p.getChild(0) instanceof AggregatePlanNode);

            p = pns.get(1).getChild(0);
            // push down for optimization
            assertTrue(p instanceof AbstractScanPlanNode);

            assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                    p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        } else {
            assertTrue(pns.size() == 1);
            assertTrue(p instanceof AbstractScanPlanNode);
            assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                    p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
        }
    }

    public void testUnOptimizedAVG() {
        pns = compileToFragments("SELECT AVG(A1) FROM R1");
        checkOptimizedAgg(pns, false);

        pns = compileToFragments("SELECT A1, AVG(PKEY) FROM R1 GROUP BY A1");
        checkOptimizedAgg(pns, false);

        pns = compileToFragments("SELECT A1, AVG(PKEY)+1 FROM R1 GROUP BY A1");
        checkHasComplexAgg(pns);
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
    }

    public void testOptimizedAVG() {
        pns = compileToFragments("SELECT AVG(A1) FROM P1");
        checkHasComplexAgg(pns);
        checkOptimizedAgg(pns, true);

        pns = compileToFragments("SELECT A1, AVG(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
        // Test avg pushed down by replacing it with sum, count
        checkOptimizedAgg(pns, true);

        pns = compileToFragments("SELECT A1, AVG(PKEY)+1 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
        // Test avg pushed down by replacing it with sum, count
        checkOptimizedAgg(pns, true);
    }

    public void testGroupbyColsNotInDisplayCols() {
        pns = compileToFragments("SELECT sum(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT sum(PKEY), sum(PKEY) FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    private void checkMVNoFix_NoAgg(
            String sql,
            boolean distinctPushdown) {
        // the first '-1' indicates that there is no top aggregation node.
        checkMVReaggreateFeature(sql, false,
                -1, -1,
                -1, -1,
                distinctPushdown, true, false, false);
    }

    private void checkMVNoFix_NoAgg(
            String sql, int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            boolean distinctPushdown, boolean projectionNode,
            boolean aggPushdown, boolean aggInline) {

        checkMVReaggreateFeature(sql, false, numGroupbyOfTopAggNode, numAggsOfTopAggNode, -1, -1,
                distinctPushdown, projectionNode, aggPushdown, aggInline);

    }

    public void testNoFix_MVBasedQuery() {
        String sql = "";
        // (1) Table V_P1_NO_FIX_NEEDED:

        // Normal select queries
        checkMVNoFix_NoAgg("SELECT * FROM V_P1_NO_FIX_NEEDED", false);
        checkMVNoFix_NoAgg("SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED ORDER BY V_A1", false);
        checkMVNoFix_NoAgg("SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED LIMIT 1", false);
        checkMVNoFix_NoAgg("SELECT DISTINCT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED", true);

        // Distributed group by query
        checkMVNoFix_NoAgg("SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED GROUP by V_SUM_C1",
                1, 0, false, false, true, true);
        checkMVNoFix_NoAgg("SELECT V_SUM_C1, sum(V_CNT) FROM V_P1_NO_FIX_NEEDED " +
                "GROUP by V_SUM_C1", 1, 1, false, false, true, true);

        // (2) Table V_P1 and V_P1_NEW:
        pns = compileToFragments("SELECT SUM(V_SUM_C1) FROM V_P1");
        checkMVReaggregateFeature(false, 0, 1, -1, -1, false, false, true, true);

        pns = compileToFragments("SELECT MIN(V_MIN_C1) FROM V_P1_NEW");
        checkMVReaggregateFeature(false, 0, 1, -1, -1, false, false, true, true);

        pns = compileToFragments("SELECT MAX(V_MAX_D1) FROM V_P1_NEW");
        checkMVReaggregateFeature(false, 0, 1, -1, -1, false, false, true, true);

        checkMVNoFix_NoAgg("SELECT MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1", 1, 1, false, true, true, true);
        checkMVNoFix_NoAgg("SELECT V_A1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1", 1, 1, false, false, true, true);
        checkMVNoFix_NoAgg("SELECT V_A1,V_B1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1, V_B1", 2, 1, false, false, true, true);


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
        checkMVNoFix_NoAgg(sql, 2, 1, false, true, true, false);


        sql = "select sum(v_cnt) from v_p1 INNER JOIN v_r1 using(v_a1)";
        checkMVNoFix_NoAgg(sql, 0, 1, false, false, true, false);

        sql = "select v_p1.v_b1, sum(v_p1.v_sum_d1) from v_p1 INNER JOIN v_r1 on v_p1.v_a1 > v_r1.v_a1 " +
                "group by v_p1.v_b1;";
        checkMVNoFix_NoAgg(sql, 1, 1, false, false, true, false);

        sql = "select MAX(v_r1.v_a1) from v_p1 INNER JOIN v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "INNER JOIN r1v on v_p1.v_a1 = r1v.v_a1 ";
        checkMVNoFix_NoAgg(sql, 0, 1, false, false, true, false);
    }

    public void testMVBasedQuery_EdgeCases() {
        // No aggregation will be pushed down.
        checkMVFix_TopAgg_ReAgg("SELECT count(*) FROM V_P1", 0, 1, 2, 0);
        checkMVFix_TopAgg_ReAgg("SELECT SUM(v_a1) FROM V_P1", 0, 1, 2, 0);
        checkMVFix_TopAgg_ReAgg("SELECT count(v_a1) FROM V_P1", 0, 1, 2, 0);
        checkMVFix_TopAgg_ReAgg("SELECT max(v_a1) FROM V_P1", 0, 1, 2, 0);

        // ENG-5386 opposite cases.
        checkMVFix_TopAgg_ReAgg("SELECT SUM(V_SUM_C1+1) FROM V_P1", 0, 1, 2, 1);
        checkMVFix_TopAgg_ReAgg("SELECT SUM(V_SUM_C1) FROM V_P1 WHERE V_SUM_C1 > 3", 0, 1, 2, 1);
        checkMVFix_TopAgg_ReAgg("SELECT V_SUM_C1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_SUM_C1", 1, 1, 2, 2);

        // ENG-5669 HAVING edge cases.
        checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT SUM(V_SUM_C1) FROM V_P1 HAVING MAX(V_SUM_D1) > 3", 0, 2, 2, 2);

        pns = compileToFragments("SELECT SUM(V_SUM_C1) FROM V_P1 HAVING SUM(V_SUM_D1) > 3");
        checkMVReaggregateFeature(false, 0, 2, -1, -1, false, true, true, true);

        // No disctinct will be pushed down.
        // ENG-5364.
        // In future,  a little efficient way is to push down distinct for part of group by columns only.
        checkMVFix_reAgg("SELECT distinct v_a1 FROM V_P1", 2, 0);
        checkMVFix_reAgg("SELECT distinct v_cnt FROM V_P1", 2, 1);
    }

    public void testMVBasedQuery_NoAggQuery() {
        //        CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //        AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //        FROM P1  GROUP BY A1, B1;

        String[] tbs = {"V_P1", "V_P1_ABS"};
        for (String tb: tbs) {
            checkMVFix_reAgg("SELECT * FROM " + tb, 2, 3);
            checkMVFix_reAgg("SELECT * FROM " + tb + " order by V_A1", 2, 3);
            checkMVFix_reAgg("SELECT * FROM " + tb + " order by V_A1, V_B1", 2, 3);
            checkMVFix_reAgg("SELECT * FROM " + tb + " order by V_SUM_D1", 2, 3);
            checkMVFix_reAgg("SELECT * FROM " + tb + " limit 1", 2, 3);
            checkMVFix_reAgg("SELECT * FROM " + tb + " order by V_A1, V_B1 limit 1", 2, 3);
            checkMVFix_reAgg("SELECT v_sum_c1 FROM " + tb, 2, 1);
            checkMVFix_reAgg("SELECT v_sum_c1 FROM " + tb + " order by v_sum_c1", 2, 1);
            checkMVFix_reAgg("SELECT v_sum_c1 FROM " + tb + " order by v_sum_d1", 2, 2);
            checkMVFix_reAgg("SELECT v_sum_c1 FROM " + tb + " limit 1", 2, 1);
            checkMVFix_reAgg("SELECT v_sum_c1 FROM " + tb + " order by v_sum_c1 limit 1", 2, 1);
            // test distinct down.
            checkMVFix_reAgg("SELECT distinct v_sum_c1 FROM " + tb + " limit 1", 2, 1);
        }
    }

    public void testMVBasedQuery_AggQuery() {
        //      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //      FROM P1  GROUP BY A1, B1;

        String[] tbs = {"V_P1", "V_P1_ABS"};

        for (String tb: tbs) {
            // Test set (1): group by
            checkMVFix_TopAgg_ReAgg("SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1", 1, 0, 2, 1);

            // because we have order by.
            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 ORDER BY V_SUM_C1", 1, 0, 2, 1);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_SUM_C1 FROM " + tb + " GROUP by V_SUM_C1 " +
                    "ORDER BY V_SUM_C1 LIMIT 5", 1, 0, 2, 1);

            checkMVFix_TopAgg_ReAgg("SELECT V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 LIMIT 5", 1, 0, 2, 1);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT distinct V_SUM_C1 FROM " + tb +
                    " GROUP by V_SUM_C1 LIMIT 5", 1, 0, 2, 1);

            // Test set (2):
            checkMVFix_TopAgg_ReAgg("SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1", 1, 1, 2, 2);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1 ORDER BY V_SUM_C1", 1, 1, 2, 2);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_SUM_C1, sum(V_CNT) FROM " + tb +
                    " GROUP by V_SUM_C1 ORDER BY V_SUM_C1 limit 2", 1, 1, 2, 2);

            // Distinct: No aggregation push down.
            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_SUM_C1, sum(distinct V_CNT) " +
                    "FROM " + tb + " GROUP by V_SUM_C1 ORDER BY V_SUM_C1", 1, 1, 2, 2);

            // Test set (3)
            checkMVFix_TopAgg_ReAgg("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1", 3, 1, 2, 2);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1,V_B1, V_SUM_C1", 3, 1, 2, 2);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1,V_B1, V_SUM_C1 LIMIT 5", 3, 1, 2, 2);

            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_A1,V_B1, V_SUM_C1, sum(V_SUM_D1) FROM " + tb +
                    " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1, V_SUM_C1 LIMIT 5", 3, 1, 2, 2);

            // Distinct: No aggregation push down.
            checkMVFix_TopAgg_ReAgg_with_TopProjection("SELECT V_A1,V_B1, V_SUM_C1, sum( distinct V_SUM_D1) FROM " +
                    tb + " GROUP BY V_A1,V_B1, V_SUM_C1 ORDER BY V_A1, V_SUM_C1 LIMIT 5", 3, 1, 2, 2);
        }
    }

    private void checkMVFixWithWhere(String sql, String aggFilter, String scanFilter) {
        pns = compileToFragments(sql);
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
        checkMVFixWithWhere( aggFilter == null? null: new String[] {aggFilter},
                    scanFilter == null? null: new String[] {scanFilter});
    }

    private void checkMVFixWithWhere(String sql, Object aggFilters[]) {
        pns = compileToFragments(sql);
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
        checkMVFixWithWhere(aggFilters, null);
    }

    private void checkMVFixWithWhere(Object aggFilters, Object scanFilters) {
        AbstractPlanNode p = pns.get(0);

        List<AbstractPlanNode> nodes = p.findAllNodesOfType(PlanNodeType.RECEIVE);
        assertEquals(1, nodes.size());
        p = nodes.get(0);

        // Find re-aggregation node.
        assertTrue(p instanceof ReceivePlanNode);
        assertTrue(p.getParent(0) instanceof HashAggregatePlanNode);
        HashAggregatePlanNode reAggNode = (HashAggregatePlanNode) p.getParent(0);
        String reAggNodeStr = reAggNode.toExplainPlanString().toLowerCase();

        // Find scan node.
        p = pns.get(1);
        assert (p.getScanNodeList().size() == 1);
        p = p.getScanNodeList().get(0);
        String scanNodeStr = p.toExplainPlanString().toLowerCase();

        if (aggFilters != null) {
            String[] aggFilterStrings = null;
            if (aggFilters instanceof String) {
                aggFilterStrings = new String[] { (String) aggFilters };
            } else {
                aggFilterStrings = (String[]) aggFilters;
            }
            for (String aggFilter : aggFilterStrings) {
                System.out.println(reAggNodeStr.contains(aggFilter
                        .toLowerCase()));
                assertTrue(reAggNodeStr.contains(aggFilter.toLowerCase()));
                System.out
                        .println(scanNodeStr.contains(aggFilter.toLowerCase()));
                assertFalse(scanNodeStr.contains(aggFilter.toLowerCase()));
            }
        } else {
            assertNull(reAggNode.getPostPredicate());
        }

        if (scanFilters != null) {
            String[] scanFilterStrings = null;
            if (scanFilters instanceof String) {
                scanFilterStrings = new String[] { (String) scanFilters };
            } else {
                scanFilterStrings = (String[]) scanFilters;
            }
            for (String scanFilter : scanFilterStrings) {
                System.out.println(reAggNodeStr.contains(scanFilter
                        .toLowerCase()));
                assertFalse(reAggNodeStr.contains(scanFilter.toLowerCase()));
                System.out.println(scanNodeStr.contains(scanFilter
                        .toLowerCase()));
                assertTrue(scanNodeStr.contains(scanFilter.toLowerCase()));
            }
        }
    }

    public void testMVBasedQuery_Where() {
        //      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //      FROM P1  GROUP BY A1, B1;
        // Test
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        checkMVFixWithWhere("SELECT * FROM V_P1 where v_cnt = 1", "v_cnt = 1", null);
        checkMVFixWithWhere("SELECT * FROM V_P1 where v_a1 = 9", null, "v_a1 = 9");
        checkMVFixWithWhere("SELECT * FROM V_P1 where v_a1 = 9 AND v_cnt = 1", "v_cnt = 1", "v_a1 = 9");
        checkMVFixWithWhere("SELECT * FROM V_P1 where v_a1 = 9 OR v_cnt = 1", new String[] {"v_a1 = 9) OR ", "v_cnt = 1)"});
        checkMVFixWithWhere("SELECT * FROM V_P1 where v_a1 = v_cnt + 1", new String[] {"v_a1 = (", "v_cnt + 1)"});
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    private void checkMVFixWithJoin_reAgg(String sql, int numGroupbyOfReaggNode, int numAggsOfReaggNode,
            Object aggFilter, String scanFilter) {
        checkMVFixWithJoin(sql, -1, -1, numGroupbyOfReaggNode, numAggsOfReaggNode, aggFilter, scanFilter);
    }

    private void checkMVFixWithJoin_reAgg_noOrder(String sql, int numGroupbyOfReaggNode, int numAggsOfReaggNode,
            Object aggFilters, Object scanFilters) {
        checkMVFixWithJoin_noOrder(sql, -1, -1, numGroupbyOfReaggNode, numAggsOfReaggNode, aggFilters, scanFilters);
    }

    private void checkMVFixWithJoin(String sql, int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode, Object aggFilter, Object scanFilter) {
        checkMVFixWithJoin_noOrder(sql, numGroupbyOfTopAggNode, numAggsOfTopAggNode, numGroupbyOfReaggNode, numAggsOfReaggNode,
                aggFilter, scanFilter);
    }

    private void checkMVFixWithJoin_noOrder(String sql, int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode, Object aggFilters, Object scanFilters) {

        String[] joinType = {"inner join", "left join", "right join"};

        for (int i = 0; i < joinType.length; i++) {
            String newsql = sql.replace("@joinType", joinType[i]);
            pns = compileToFragments(newsql);
            System.out.println("Query:" + newsql);
            // No join node under receive node.
            checkMVReaggregateFeature(true, numGroupbyOfTopAggNode, numAggsOfTopAggNode,
                    numGroupbyOfReaggNode, numAggsOfReaggNode, false, false, false, false);

            checkMVFixWithWhere(aggFilters, scanFilters);
        }
    }

    /**
     * No tested for Outer join, no 'using' unclear column reference tested.
     * Non-aggregation queries.
     */
    public void testMVBasedQuery_Join_NoAgg() {
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        String sql = "";

        // Two tables joins.
        sql = "select v_a1 from v_p1 @joinType v_r1 using(v_a1)";
        checkMVFixWithJoin_reAgg(sql, 2, 0, null, null);

        sql = "select v_a1 from v_p1 @joinType v_r1 using(v_a1) " +
                "where v_a1 > 1 and v_p1.v_cnt > 2 and v_r1.v_b1 < 3 ";
        checkMVFixWithJoin_reAgg(sql, 2, 1, "v_cnt > 2", null /* "v_a1 > 1" is optional */);

        sql = "select v_cnt from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 ";
        checkMVFixWithJoin_reAgg(sql, 2, 2,
                new String[] { "v_sum_c1 < 3)", "v_cnt > 1)" }, "v_a1 > 2");

        // join on different columns.
        sql = "select v_p1.v_cnt from v_r1 @joinType v_p1 on v_r1.v_sum_c1 = v_p1.v_sum_d1 ";
        checkMVFixWithJoin_reAgg(sql, 2, 2, null, null);


        // Three tables joins.
        sql = "select v_r1.v_a1, v_r1.v_cnt from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "@joinType r1v on v_p1.v_a1 = r1v.v_a1 ";
        checkMVFixWithJoin_reAgg(sql, 2, 0, null, null);

        sql = "select v_r1.v_cnt, v_r1.v_a1 from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt ";
        checkMVFixWithJoin_reAgg(sql, 2, 1, null, null);

        // join on different columns.
        sql = "select v_p1.v_cnt from v_r1 @joinType v_p1 on v_r1.v_sum_c1 = v_p1.v_sum_d1 " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_sum_c1";
        checkMVFixWithJoin_reAgg(sql, 2, 2, null, null);

        sql = "select v_r1.v_cnt, v_r1.v_a1 from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 ";
        checkMVFixWithJoin_reAgg_noOrder(sql, 2, 2,
                new String[] {"v_cnt > 1", "v_sum_c1 < 3"}, "v_a1 > 2");

        sql = "select v_r1.v_cnt, v_r1.v_a1 from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt " +
                "@joinType r1v on v_p1.v_cnt = r1v.v_cnt where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and " +
                "v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 and r1v.v_sum_c1 > 6";
        checkMVFixWithJoin_reAgg_noOrder(sql, 2, 2,
                new String[] {"v_cnt > 1", "v_sum_c1 < 3"}, "v_a1 > 2");
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    /**
     * No tested for Outer join, no 'using' unclear column reference tested.
     * Aggregation queries.
     */
    public void testMVBasedQuery_Join_Agg() {
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
        checkMVFixWithJoin(sql, 1, 1, 2, 0, null, null);

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) " +
                "group by v_p1.v_b1, v_p1.v_cnt;";
        checkMVFixWithJoin(sql, 2, 1, 2, 1, null, null);

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_p1.v_a1) from v_p1 @joinType v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "where v_p1.v_a1 > 1 AND v_p1.v_cnt < 8 group by v_p1.v_b1, v_p1.v_cnt;";
        checkMVFixWithJoin(sql, 2, 1, 2, 1, "v_cnt < 8", "v_a1 > 1");

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_a1), max(v_p1.v_sum_c1) from v_p1 @joinType v_r1 " +
                "on v_p1.v_a1 = v_r1.v_a1 " +
                "where v_p1.v_a1 > 1 AND v_p1.v_cnt < 8 group by v_p1.v_b1, v_p1.v_cnt;";
        checkMVFixWithJoin(sql, 2, 2, 2, 2, "v_cnt < 8", "v_a1 > 1");



        sql = "select v_r1.v_b1, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) group by v_r1.v_b1;";
        checkMVFixWithJoin(sql, 1, 1, 2, 0, null, null);

        sql = "select v_r1.v_b1, v_r1.v_cnt, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) " +
                "group by v_r1.v_b1, v_r1.v_cnt;";
        checkMVFixWithJoin(sql, 2, 1, 2, 0, null, null);

        sql = "select v_r1.v_b1, v_p1.v_cnt, sum(v_a1) from v_p1 @joinType v_r1 using(v_a1) " +
                "group by v_r1.v_b1, v_p1.v_cnt;";
        checkMVFixWithJoin(sql, 2, 1, 2, 1, null, null);


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
        checkMVFixWithJoin(sql, 2, 1, 2, 2, null, null);

        sql = "select v_p1.v_cnt, v_p1.v_b1, SUM(v_p1.v_sum_d1), MAX(v_r1.v_a1)  " +
                "from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt @joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "group by v_p1.v_cnt, v_p1.v_b1";
        checkMVFixWithJoin(sql, 2, 2, 2, 2, null, null);

        sql = "select v_p1.v_cnt, v_p1.v_b1, SUM(v_p1.v_sum_d1) " +
                "from v_p1 @joinType v_r1 on v_p1.v_cnt = v_r1.v_cnt @joinType r1v on v_p1.v_cnt = r1v.v_cnt " +
                "where v_p1.v_cnt > 1 and v_p1.v_a1 > 2 and v_p1.v_sum_c1 < 3 and v_r1.v_b1 < 4 " +
                "group by v_p1.v_cnt, v_p1.v_b1 ";
        checkMVFixWithJoin(sql, 2, 1, 2, 3, new String[] { "v_sum_c1 < 3)", "v_cnt > 1)" }, "v_a1 > 2");
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    public void testENG5385() {
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        String sql = "";

        sql = "select v_a1 from v_p1 left join v_r1 on v_p1.v_a1 = v_r1.v_a1 AND v_p1.v_cnt = 2 ";
        checkMVFixWithJoin_reAgg(sql, 2, 1, "v_cnt = 2", null);

        // When ENG-5385 is fixed, use the next line to check its plan.
//        checkMVFixWithJoin_reAgg(sql, 2, 1, null, null);
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    public void testENG389_Having() {
        boolean asItWas = AbstractExpression.disableVerboseExplainForDebugging();
        //      CREATE VIEW V_P1 (V_A1, V_B1, V_CNT, V_SUM_C1, V_SUM_D1)
        //      AS SELECT A1, B1, COUNT(*), SUM(C1), COUNT(D1)
        //      FROM P1  GROUP BY A1, B1;

        String sql = "";

        failToCompile("select sum(V_A1) from v_r1 having v_cnt > 3", "invalid HAVING expression");
        failToCompile("select sum(V_A1) from v_r1 having 3 > 3", "does not support HAVING clause without aggregation");

        sql = "select V_A1, count(v_cnt) from v_r1 group by v_a1 having count(v_cnt) > 1; ";
        checkHavingClause(sql, true, ".v_cnt) having (column#1 > 1)");

        sql = "select sum(V_A1) from v_r1 having avg(v_cnt) > 3; ";
        checkHavingClause(sql, true, ".v_cnt) having (column#1 > 3)");

        sql = "select avg(v_cnt) from v_r1 having avg(v_cnt) > 3; ";
        checkHavingClause(sql, true, ".v_cnt) having (column#0 > 3)");
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    private void checkHavingClause(String sql, boolean aggInline, Object aggPostFilters) {
        pns = compileToFragments(sql);
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }

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
            } else {
                aggFilterStrings = (String[]) aggPostFilters;
            }
            for (String aggFilter : aggFilterStrings) {
                assertTrue(aggNodeStr.contains(aggFilter.toLowerCase()));
            }
        } else {
            assertNull(aggNode.getPostPredicate());
        }
    }

    private void checkMVFix_reAgg(
            String sql,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode) {

        checkMVReaggreateFeature(sql, true,
                -1, -1,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                false, true, false, false);
    }

    private void checkMVFix_TopAgg_ReAgg(
            String sql,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode) {

        checkMVReaggreateFeature(sql, true,
                numGroupbyOfTopAggNode, numAggsOfTopAggNode,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                false, false, false, false);
    }

    private void checkMVFix_TopAgg_ReAgg_with_TopProjection(
            String sql,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode) {

        checkMVReaggreateFeature(sql, true,
                numGroupbyOfTopAggNode, numAggsOfTopAggNode,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                false, true, false, false);
    }


    // topNode, reAggNode
    private void checkMVReaggreateFeature(
            String sql, boolean needFix,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode,
            boolean distinctPushdown, boolean projectionNode,
            boolean aggPushdown, boolean aggInline) {

        pns = compileToFragments(sql);
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
        checkMVReaggregateFeature(needFix, numGroupbyOfTopAggNode, numAggsOfTopAggNode,
                numGroupbyOfReaggNode, numAggsOfReaggNode,
                distinctPushdown, projectionNode, aggPushdown, aggInline);
    }

    // topNode, reAggNode
    private void checkMVReaggregateFeature(
            boolean needFix,
            int numGroupbyOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupbyOfReaggNode, int numAggsOfReaggNode,
            boolean distinctPushdown, boolean projectionNode,
            boolean aggPushdown, boolean aggInline) {

        assertTrue(pns.size() == 2);
        AbstractPlanNode p = pns.get(0);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);

        if (projectionNode) {
            assertTrue(p instanceof ProjectionPlanNode);
            p = p.getChild(0);
        }

        if (p instanceof LimitPlanNode) {
            // No limit pushed down.
            p = p.getChild(0);
        }

        if (p instanceof OrderByPlanNode) {
            p = p.getChild(0);
        }
        if (p instanceof DistinctPlanNode) {
            p = p.getChild(0);
        }

        HashAggregatePlanNode reAggNode = null;

        List<AbstractPlanNode> nodes = p.findAllNodesOfType(PlanNodeType.RECEIVE);
        assertEquals(1, nodes.size());
        AbstractPlanNode receiveNode = nodes.get(0);

        // Indicates that there is no top aggregation node.
        if (numGroupbyOfTopAggNode == -1 ) {
            if (needFix) {
                p = receiveNode.getParent(0);
                assertTrue(p instanceof HashAggregatePlanNode);
                reAggNode = (HashAggregatePlanNode) p;

                assertEquals(numGroupbyOfReaggNode, reAggNode.getGroupByExpressionsSize());
                assertEquals(numAggsOfReaggNode, reAggNode.getAggregateTypesSize());

                p = p.getChild(0);
            }
            assertTrue(p instanceof ReceivePlanNode);

            p = pns.get(1);
            assertTrue(p instanceof SendPlanNode);
            p = p.getChild(0);

            if (distinctPushdown) {
                assertTrue(p instanceof DistinctPlanNode);
                p = p.getChild(0);
            }
            assertTrue(p instanceof AbstractScanPlanNode);
        } else {
            assertTrue(p instanceof AggregatePlanNode);
            AggregatePlanNode topAggNode = (AggregatePlanNode) p;

            assertEquals(numGroupbyOfTopAggNode, topAggNode.getGroupByExpressionsSize());
            assertEquals(numAggsOfTopAggNode, topAggNode.getAggregateTypesSize());
            p = p.getChild(0);

            if (needFix) {
                p = receiveNode.getParent(0);
                assertTrue(p instanceof HashAggregatePlanNode);
                reAggNode = (HashAggregatePlanNode) p;

                assertEquals(numGroupbyOfReaggNode, reAggNode.getGroupByExpressionsSize());
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
                    assertTrue(p.getInlinePlanNode(PlanNodeType.AGGREGATE) != null ||
                            p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE) != null);
                } else {
                    assertTrue(p instanceof AggregatePlanNode);
                    p = p.getChild(0);
                }
            }

            if (needFix) {
                assertTrue(p instanceof AbstractScanPlanNode);
            } else {
                assertTrue(p instanceof AbstractScanPlanNode ||
                        p instanceof AbstractJoinPlanNode);
            }
        }

    }
}
