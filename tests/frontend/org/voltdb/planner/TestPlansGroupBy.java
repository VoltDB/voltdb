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
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractReceivePlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class TestPlansGroupBy extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansGroupBy.class.getResource("testplans-groupby-ddl.sql"),
                "testplansgroupby", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testInlineSerialAgg_noGroupBy() {
        checkSimpleTableInlineAgg("SELECT SUM(A1) from T1");
        checkSimpleTableInlineAgg("SELECT MIN(A1) from T1");
        checkSimpleTableInlineAgg("SELECT MAX(A1) from T1");
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
        List<AbstractPlanNode> pns;

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
      List<AbstractPlanNode> pns;

      pns = compileToFragments("SELECT AVG(A1) from T1");
      //* enable to debug */ printExplainPlan(pns);
      p = pns.get(0).getChild(0);
      assertTrue(p instanceof ProjectionPlanNode);
      assertTrue(p.getChild(0) instanceof AggregatePlanNode);
      assertTrue(p.getChild(0).getChild(0) instanceof ReceivePlanNode);

      p = pns.get(1).getChild(0);
      assertTrue(p instanceof SeqScanPlanNode);
      assertNotNull(p.getInlinePlanNode(PlanNodeType.PROJECTION));
      assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
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
     */
    public void testAggregateOptimizationWithIndex() {
        AbstractPlanNode p;
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A, count(B) from R2 where B > 2 group by A;");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        assertTrue(p.toExplainPlanString().contains("primary key index"));

        // matching the partial index where clause
        pns = compileToFragments("SELECT A, count(B) from R2 where B > 3 group by A;");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
        assertTrue(p.toExplainPlanString().contains("PARTIAL_IDX_R2"));

        // using the partial index with serial aggregation
        pns = compileToFragments("SELECT A, count(B) from R2 where A > 5 and B > 3 group by A;");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
        assertTrue(p.toExplainPlanString().contains("PARTIAL_IDX_R2"));

        // order by will help pick up the partial index
        pns = compileToFragments("SELECT A, count(B) from R2 where B > 3 group by A order by A;");
        assertEquals(1, pns.size());
        //* enable to debug */ printExplainPlan(pns);
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
        assertTrue(p.toExplainPlanString().contains("PARTIAL_IDX_R2"));

        // using the partial index with partial aggregation
        pns = compileToFragments("SELECT C, A, MAX(B) FROM R2 WHERE A > 0 and B > 3 GROUP BY C, A");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, p.getPlanNodeType());
        assertNotNull(p.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));
        assertTrue(p.toExplainPlanString().contains("PARTIAL_IDX_R2"));

        // Partition IndexScan with HASH aggregate is optimized to use Partial aggregate -
        // index (F_D1) covers part of the GROUP BY columns
        pns = compileToFragments("SELECT F_D1, F_VAL1, MAX(F_VAL2) FROM F WHERE F_D1 > 0 GROUP BY F_D1, F_VAL1 ORDER BY F_D1, MAX(F_VAL2)");
        assertEquals(2, pns.size());
        p = pns.get(1).getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, p.getPlanNodeType());
        assertNotNull(p.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));
        assertTrue(p.toExplainPlanString().contains("COL_F_TREE1"));

        // IndexScan with HASH aggregate is optimized to use Serial aggregate -
        // index (F_VAL1, F_VAL2) covers all of the GROUP BY columns
        pns = compileToFragments("SELECT F_VAL1, F_VAL2, MAX(F_VAL3) FROM RF WHERE F_VAL1 > 0 GROUP BY F_VAL2, F_VAL1");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, p.getPlanNodeType());
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));
        assertTrue(p.toExplainPlanString().contains("COL_RF_TREE2"));

        // IndexScan with HASH aggregate remains not optimized -
        // The first column index (F_VAL1, F_VAL2) is not part of the GROUP BY
        pns = compileToFragments("SELECT F_VAL2, MAX(F_VAL2) FROM RF WHERE F_VAL1 > 0 GROUP BY F_VAL2");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, p.getPlanNodeType());
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        assertTrue(p.toExplainPlanString().contains("COL_RF_TREE2"));

        // Partition IndexScan with HASH aggregate remains unoptimized -
        // index (F_VAL1, F_VAL2) does not cover any of the GROUP BY columns
        pns = compileToFragments("SELECT MAX(F_VAL2) FROM F WHERE F_VAL1 > 0 GROUP BY F_D1");
        assertEquals(2, pns.size());
        p = pns.get(1).getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, p.getPlanNodeType());
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        assertTrue(p.toExplainPlanString().contains("COL_F_TREE2"));

        // IndexScan with HASH aggregate remains unoptimized - the index COL_RF_HASH is not scannable
        pns = compileToFragments("SELECT F_VAL3, MAX(F_VAL2) FROM RF WHERE F_VAL3 = 0 GROUP BY F_VAL3");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, p.getPlanNodeType());
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        assertTrue(p.toExplainPlanString().contains("COL_RF_HASH"));

        // where clause not matching
        pns = compileToFragments("SELECT A, count(B) from R2 where B > 2 group by A order by A;");
        assertEquals(1, pns.size());
        p = pns.get(0).getChild(0);
        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        assertTrue(p instanceof OrderByPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof SeqScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
    }

    public void testCountStar() {
        compileToFragments("SELECT count(*) FROM T1");
    }

    public void testCountDistinct() {
        AbstractPlanNode p;
        List<AbstractPlanNode> pns;

        // push down distinct because of group by partition column
        pns = compileToFragments("SELECT A4, count(distinct B4) FROM T4 GROUP BY A4");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        // group by multiple columns
        pns = compileToFragments("SELECT C4, A4, count(distinct B4) FROM T4 GROUP BY C4, A4");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        // not push down distinct
        pns = compileToFragments("SELECT ABS(A4), count(distinct B4) FROM T4 GROUP BY ABS(A4)");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof HashAggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        // test not group by partition column with index available
        pns = compileToFragments("SELECT A.NUM, COUNT(DISTINCT A.ID ) AS Q58 FROM P2 A GROUP BY A.NUM; ");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof HashAggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertTrue(p.toExplainPlanString().contains("for deterministic order only"));
    }

    public void testDistinctA1() {
        compileToFragments("SELECT DISTINCT A1 FROM T1");
    }

    public void testDistinctA1_Subquery() {
        AbstractPlanNode p;
        List<AbstractPlanNode> pns;

        // Distinct rewrote with group by
        pns = compileToFragments("select * from (SELECT DISTINCT A1 FROM T1) temp");

        p = pns.get(0).getChild(0);
        assertTrue(p instanceof SeqScanPlanNode);
        assertTrue(p.getChild(0) instanceof HashAggregatePlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof ReceivePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
    }

    public void testGroupByA1() {
        AbstractPlanNode p;
        AggregatePlanNode aggNode;
        List<AbstractPlanNode> pns;

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

    private void checkGroupByPartitionKey(List<AbstractPlanNode> pns,
            boolean topAgg, boolean having) {
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
        List<AbstractPlanNode> pns;

        // Primary key is equal to partition key
        pns = compileToFragments("SELECT PKEY, COUNT(*) from T1 group by PKEY");
        // "its primary key index (for optimized grouping only)"
        // Not sure why not use serial aggregate instead
        checkGroupByPartitionKey(pns, false, false);

        // Test Having expression
        pns = compileToFragments("SELECT PKEY, COUNT(*) from T1 group by PKEY Having count(*) > 3");
        checkGroupByPartitionKey(pns, false, true);

        // Primary key is not equal to partition key
        pns = compileToFragments("SELECT A3, COUNT(*) from T3 group by A3");
        checkGroupByPartitionKey(pns, false, false);

        // Test Having expression
        pns = compileToFragments("SELECT A3, COUNT(*) from T3 group by A3 Having count(*) > 3");
        checkGroupByPartitionKey(pns, false, true);


        // Group by partition key and others
        pns = compileToFragments("SELECT B3, A3, COUNT(*) from T3 group by B3, A3");
        checkGroupByPartitionKey(pns, false, false);

        // Test Having expression
        pns = compileToFragments("SELECT B3, A3, COUNT(*) from T3 group by B3, A3 Having count(*) > 3");
        checkGroupByPartitionKey(pns, false, true);
    }

    public void testGroupByPartitionKey_Negative() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT ABS(PKEY), COUNT(*) from T1 group by ABS(PKEY)");
        checkGroupByPartitionKey(pns, true, false);

        pns = compileToFragments("SELECT ABS(PKEY), COUNT(*) from T1 group by ABS(PKEY) Having count(*) > 3");
        checkGroupByPartitionKey(pns, true, true);
    }

    // Group by with index
    private void checkGroupByOnlyPlan(List<AbstractPlanNode> pns,
            boolean twoFragments, PlanNodeType type, boolean isIndexScan) {
        AbstractPlanNode apn = pns.get(0).getChild(0);
        if (twoFragments) {
            assertEquals(PlanNodeType.HASHAGGREGATE, apn.getPlanNodeType());
            apn = pns.get(1).getChild(0);
        }

        // For a single table aggregate, it is inline always.
        assertEquals(
                (isIndexScan ? PlanNodeType.INDEXSCAN : PlanNodeType.SEQSCAN),
                apn.getPlanNodeType());

        if (type == PlanNodeType.HASHAGGREGATE) {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        }
        else if (type == PlanNodeType.AGGREGATE) {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.AGGREGATE));
        }
        else if (type == PlanNodeType.PARTIALAGGREGATE) {
            assertNotNull(apn.getInlinePlanNode(PlanNodeType.PARTIALAGGREGATE));
        }
    }

    PlanNodeType H_AGG = PlanNodeType.HASHAGGREGATE;
    PlanNodeType S_AGG = PlanNodeType.AGGREGATE;
    PlanNodeType P_AGG = PlanNodeType.PARTIALAGGREGATE;

    public void testGroupByOnly() {
        List<AbstractPlanNode> pns;

        System.out.println("Starting testGroupByOnly");

        /**
         * Serial Aggregate cases
         */
        // Replicated Table

        // only GROUP BY cols in SELECT clause
        pns = compileToFragments("SELECT F_D1 FROM RF GROUP BY F_D1");
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        // SELECT cols in GROUP BY and other aggregate cols
        pns = compileToFragments("SELECT F_D1, COUNT(*) FROM RF GROUP BY F_D1");
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        // aggregate cols are part of keys of used index
        pns = compileToFragments("SELECT F_VAL1, SUM(F_VAL2) FROM RF GROUP BY F_VAL1");
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        // expr index, full indexed case
        pns = compileToFragments("SELECT F_D1 + F_D2, COUNT(*) FROM RF GROUP BY F_D1 + F_D2");
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        // function index, prefix indexed case
        pns = compileToFragments("SELECT ABS(F_D1), COUNT(*) FROM RF GROUP BY ABS(F_D1)");
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        // order of GROUP BY cols is different of them in index definition
        // index on (ABS(F_D1), F_D2 - F_D3), GROUP BY on (F_D2 - F_D3, ABS(F_D1))
        pns = compileToFragments("SELECT F_D2 - F_D3, ABS(F_D1), COUNT(*) FROM RF GROUP BY F_D2 - F_D3, ABS(F_D1)");
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        pns = compileToFragments("SELECT F_VAL1, F_VAL2, COUNT(*) FROM RF GROUP BY F_VAL2, F_VAL1");
        //* enable to debug */ System.out.println(pns, "DEBUG: " + pns.get(0).toExplainPlanString());
        checkGroupByOnlyPlan(pns, false, S_AGG, true);

        // Partitioned Table
        pns = compileToFragments("SELECT F_D1 FROM F GROUP BY F_D1");
        // index scan for group by only, no need using hash aggregate
        checkGroupByOnlyPlan(pns, true, S_AGG, true);

        pns = compileToFragments("SELECT F_D1, COUNT(*) FROM F GROUP BY F_D1");
        checkGroupByOnlyPlan(pns, true, S_AGG, true);

        pns = compileToFragments("SELECT F_VAL1, SUM(F_VAL2) FROM F GROUP BY F_VAL1");
        checkGroupByOnlyPlan(pns, true, S_AGG, true);

        pns = compileToFragments("SELECT F_D1 + F_D2, COUNT(*) FROM F GROUP BY F_D1 + F_D2");
        checkGroupByOnlyPlan(pns, true, S_AGG, true);

        pns = compileToFragments("SELECT ABS(F_D1), COUNT(*) FROM F GROUP BY ABS(F_D1)");
        checkGroupByOnlyPlan(pns, true, S_AGG, true);

        pns = compileToFragments("SELECT F_D2 - F_D3, ABS(F_D1), COUNT(*) FROM F GROUP BY F_D2 - F_D3, ABS(F_D1)");
        checkGroupByOnlyPlan(pns, true, S_AGG, true);


        /**
         * Hash Aggregate cases
         */
        // unoptimized case (only use second col of the index), but will be replaced in
        // SeqScanToIndexScan optimization for deterministic reason
        // use EXPR_RF_TREE1 not EXPR_RF_TREE2
        pns = compileToFragments("SELECT F_D2 - F_D3, COUNT(*) FROM RF GROUP BY F_D2 - F_D3");
        checkGroupByOnlyPlan(pns, false, H_AGG, true);

        // unoptimized case: index is not scannable
        pns = compileToFragments("SELECT F_VAL3, COUNT(*) FROM RF GROUP BY F_VAL3");
        checkGroupByOnlyPlan(pns, false, H_AGG, true);

        // unoptimized case: F_D2 is not prefix indexable
        pns = compileToFragments("SELECT F_D2, COUNT(*) FROM RF GROUP BY F_D2");
        checkGroupByOnlyPlan(pns, false, H_AGG, true);

        // unoptimized case (only uses second col of the index), will not be replaced in
        // SeqScanToIndexScan for determinism because of non-deterministic receive.
        // Use primary key index
        pns = compileToFragments("SELECT F_D2 - F_D3, COUNT(*) FROM F GROUP BY F_D2 - F_D3");
        checkGroupByOnlyPlan(pns, true, H_AGG, true);

        // unoptimized case (only uses second col of the index), will be replaced in
        // SeqScanToIndexScan for determinism.
        // use EXPR_F_TREE1 not EXPR_F_TREE2
        pns = compileToFragments("SELECT F_D2 - F_D3, COUNT(*) FROM RF GROUP BY F_D2 - F_D3");
        //* enable to debug */ System.out.println(pns, pns.get(0).toExplainPlanString());
        checkGroupByOnlyPlan(pns, false, H_AGG, true);

        /**
         * Partial Aggregate cases
         */
        // unoptimized case: no prefix index found for (F_D1, F_D2)
        pns = compileToFragments("SELECT F_D1, F_D2, COUNT(*) FROM RF GROUP BY F_D1, F_D2");
        checkGroupByOnlyPlan(pns, false, P_AGG, true);

        pns = compileToFragments("SELECT ABS(F_D1), F_D3, COUNT(*) FROM RF GROUP BY ABS(F_D1), F_D3");
        checkGroupByOnlyPlan(pns, false, P_AGG, true);

        // partition table
        pns = compileToFragments("SELECT F_D1, F_D2, COUNT(*) FROM F GROUP BY F_D1, F_D2");
        checkGroupByOnlyPlan(pns, true, P_AGG, true);

        pns = compileToFragments("SELECT ABS(F_D1), F_D3, COUNT(*) FROM F GROUP BY ABS(F_D1), F_D3");
        checkGroupByOnlyPlan(pns, true, P_AGG, true);

        /**
         * Regression case
         */
        // ENG-9990 Repeating GROUP BY partition key in SELECT corrupts output schema.
        //* enable to debug */ boolean was = AbstractPlanNode.enableVerboseExplainForDebugging();
        pns = compileToFragments("SELECT G_PKEY, COUNT(*) C, G_PKEY FROM G GROUP BY G_PKEY");
        //* enable to debug */ System.out.println(pns.get(0).toExplainPlanString());
        //* enable to debug */ System.out.println(pns.get(1).toExplainPlanString());
        //* enable to debug */ AbstractPlanNode.restoreVerboseExplainForDebugging(was);
        AbstractPlanNode pn = pns.get(0);
        pn = pn.getChild(0);
        NodeSchema os = pn.getOutputSchema();
        // The problem was a mismatch between the output schema
        // of the coordinator's send node and its feeding receive node
        // that had incorrectly rearranged its columns.
        SchemaColumn middleCol = os.getColumn(1);
        System.out.println(middleCol.toString());
        assertTrue(middleCol.getColumnAlias().equals("C"));

    }

    private void checkPartialAggregate(List<AbstractPlanNode> pns,
            boolean twoFragments) {
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
        pns = compileToFragments(sql);
        checkPartialAggregate(pns, true);

        // With different group by key ordered
        sql = "SELECT G.G_D1, RF.F_D2, COUNT(*) " +
                "FROM G LEFT OUTER JOIN RF ON G.G_D2 = RF.F_D1 " +
                "GROUP BY RF.F_D2, G.G_D1";
        pns = compileToFragments(sql);
        checkPartialAggregate(pns, true);


        // three table joins with aggregate
        sql = "SELECT G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3, COUNT(*) " +
                "FROM G LEFT OUTER JOIN F ON G.G_PKEY = F.F_PKEY " +
                "     LEFT OUTER JOIN RF ON G.G_D1 = RF.F_D1 " +
                "GROUP BY G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3";
        pns = compileToFragments(sql);
        checkPartialAggregate(pns, true);

        // With different group by key ordered
        sql = "SELECT G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3, COUNT(*) " +
                "FROM G LEFT OUTER JOIN F ON G.G_PKEY = F.F_PKEY " +
                "     LEFT OUTER JOIN RF ON G.G_D1 = RF.F_D1 " +
                "GROUP BY G.G_PKEY, RF.F_D2, G.G_D1, F.F_D3";
        pns = compileToFragments(sql);
        checkPartialAggregate(pns, true);

        // With different group by key ordered
        sql = "SELECT G.G_D1, G.G_PKEY, RF.F_D2, F.F_D3, COUNT(*) " +
                "FROM G LEFT OUTER JOIN F ON G.G_PKEY = F.F_PKEY " +
                "     LEFT OUTER JOIN RF ON G.G_D1 = RF.F_D1 " +
                "GROUP BY RF.F_D2, G.G_PKEY, F.F_D3, G.G_D1";
        pns = compileToFragments(sql);
        checkPartialAggregate(pns, true);
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
        List<AbstractPlanNode> pns;

        // replicated table with serial aggregation and inlined limit
        pns = compileToFragments("SELECT F_PKEY FROM RF GROUP BY F_PKEY LIMIT 5");
        checkGroupByOnlyPlanWithLimit(pns, false, false, true, true);

        pns = compileToFragments("SELECT F_D1 FROM RF GROUP BY F_D1 LIMIT 5");
        checkGroupByOnlyPlanWithLimit(pns, false, false, true, true);

        // partitioned table with serial aggregation and inlined limit
        // group by columns contain the partition key is the only case allowed
        pns = compileToFragments("SELECT F_PKEY FROM F GROUP BY F_PKEY LIMIT 5");
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
        for (AbstractPlanNode apn: pns) {
            explainPlan += apn.toExplainPlanString();
        }
        assertTrue(explainPlan.contains(expectedStr));

        pns = compileToFragments("SELECT A3, COUNT(*) FROM T3 GROUP BY A3 LIMIT 5");
        checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);

        pns = compileToFragments("SELECT A3, B3, COUNT(*) FROM T3 GROUP BY A3, B3 LIMIT 5");
        checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);

        pns = compileToFragments("SELECT A3, B3, COUNT(*) FROM T3 WHERE A3 > 1 GROUP BY A3, B3 LIMIT 5");
        checkGroupByOnlyPlanWithLimit(pns, true, false, true, true);

        //
        // negative tests
        //
        pns = compileToFragments("SELECT F_VAL2 FROM RF GROUP BY F_VAL2 LIMIT 5");
        checkGroupByOnlyPlanWithLimit(pns, false, true, true, false);

        // Limit should not be pushed down for case like:
        // Group by non-partition without partition key and order by.
        // ENG-6485
    }

    public void testEdgeComplexRelatedCases() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("select PKEY+A1 from T1 Order by PKEY+A1");
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0) instanceof MergeReceivePlanNode);
        assertNotNull(p.getChild(0).getInlinePlanNode(PlanNodeType.ORDERBY));

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof OrderByPlanNode);
        assertTrue(p.getChild(0) instanceof AbstractScanPlanNode);

        // Useless order by clause.
        pns = compileToFragments("SELECT count(*)  FROM P1 order by PKEY");
        p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);
        assertTrue(p.getChild(0) instanceof ReceivePlanNode);
        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);

        pns = compileToFragments("SELECT A1, count(*) as tag FROM P1 group by A1 order by tag, A1 limit 1");
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

        pns = compileToFragments("SELECT F_D1, count(*) as tag FROM RF group by F_D1 order by tag");
        p = pns.get(0).getChild(0);
        //* enable to debug */ System.out.println("DEBUG: " + p.toExplainPlanString());
        if (p instanceof ProjectionPlanNode) {
            p = p.getChild(0);
        }
        assertTrue(p instanceof OrderByPlanNode);
        p = p.getChild(0);
        assertTrue(p instanceof IndexScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.AGGREGATE));

        pns = compileToFragments("SELECT F_D1, count(*) FROM RF group by F_D1 order by 2");
        p = pns.get(0).getChild(0);
        //* enable to debug */ System.out.println("DEBUG: " + p.toExplainPlanString());
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
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2");
        checkHasComplexAgg(pns);

        // Test limit is not pushed down
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof OrderByPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        assertTrue(p.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        // inline limit with order by
        assertTrue(p instanceof OrderByPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        p = p.getChild(0);
        // inline aggregate
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

    }

    public void testComplexAggwithDistinct() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1");
        checkHasComplexAgg(pns);

        // Test aggregation node not push down with distinct
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof OrderByPlanNode);
        assertTrue(p.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
    }

    public void testComplexAggwithLimitDistinct() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A1, sum(A1), sum(distinct A1)+11 FROM P1 GROUP BY A1 ORDER BY A1 LIMIT 2");
        checkHasComplexAgg(pns);

        // Test no limit push down
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof OrderByPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.LIMIT));
        assertTrue(p.getChild(0) instanceof ProjectionPlanNode);
        assertTrue(p.getChild(0).getChild(0) instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
    }

    public void testComplexAggCase() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY) as A2, (SUM(PKEY) / 888) as A3, (SUM(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);

        pns = compileToFragments("SELECT A1, SUM(PKEY), COUNT(PKEY), (AVG(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkHasComplexAgg(pns);
    }

    public void testComplexAggCaseProjectPushdown() {
        List<AbstractPlanNode> pns;

        // This complex aggregate case will push down ORDER BY LIMIT
        // so the projection plan node should be also pushed down
        pns = compileToFragments("SELECT PKEY, sum(A1) + 1 FROM P1 GROUP BY PKEY order by 1, 2 Limit 10");
        checkHasComplexAgg(pns, true);

        pns = compileToFragments("SELECT PKEY, AVG(A1) FROM P1 GROUP BY PKEY order by 1, 2 Limit 10");
        checkHasComplexAgg(pns, true);
    }

    public void testComplexGroupBy() {
        List<AbstractPlanNode> pns;

        pns = compileToFragments("SELECT A1, ABS(A1), ABS(A1)+1, sum(B1) FROM P1 GROUP BY A1, ABS(A1)");
        checkHasComplexAgg(pns);

        // Check it can compile
        pns = compileToFragments("SELECT ABS(A1), sum(B1) FROM P1 GROUP BY ABS(A1)");
        AbstractPlanNode p = pns.get(0).getChild(0);
        assertTrue(p instanceof AggregatePlanNode);

        p = pns.get(1).getChild(0);
        // inline aggregate
        assertTrue(p instanceof AbstractScanPlanNode);
        assertNotNull(p.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));

        pns = compileToFragments("SELECT A1+PKEY, avg(B1) as tag FROM P1 GROUP BY A1+PKEY ORDER BY ABS(tag), A1+PKEY");
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
        List<AbstractPlanNode> pns;

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
        List<AbstractPlanNode> pns;

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

    public void testGroupByColsNotInDisplayCols() {
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
            explainStr1 = explainStr1.replaceAll("\\$\\$_VOLT_TEMP_TABLE_\\$\\$\\.column#[\\d]",
                    "\\$\\$_VOLT_TEMP_TABLE_\\$\\$\\.column#[Index]");
            explainStr2 = explainStr2.replaceAll("\\$\\$_VOLT_TEMP_TABLE_\\$\\$\\.column#[\\d]",
                    "\\$\\$_VOLT_TEMP_TABLE_\\$\\$\\.column#[Index]");
            assertEquals(explainStr1, explainStr2);
        }
        assertEquals(explainStr1, explainStr2);
    }

    public void testGroupByBooleanConstants() {
        String[] conditions = {"1=1", "1=0", "TRUE", "FALSE", "1>2"};
        for (String condition : conditions) {
            failToCompile("SELECT count(P1.PKEY) FROM P1 GROUP BY " + condition,
                    "A GROUP BY clause does not allow a BOOLEAN expression.");
        }
    }

    public void testGroupByAliasENG9872() {
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

        //
        // ambiguous group by query because of A1 is a column name and a select alias
        //
        pns = compileToFragments(
                "SELECT ABS(A1) AS A1, count(*) as ct FROM P1 GROUP BY A1");
        //* enable to debug */ printExplainPlan(pns);
        AbstractPlanNode p = pns.get(1).getChild(0);
        assertTrue(p instanceof AbstractScanPlanNode);
        AggregatePlanNode agg = AggregatePlanNode.getInlineAggregationNode(p);
        assertNotNull(agg);
        // group by column, instead of the ABS(A1) expression
        assertEquals(agg.getGroupByExpressions().get(0).getExpressionType(), ExpressionType.VALUE_TUPLE);
    }

    public void testGroupByAlias() {
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
                sortDirection);
    }

    private void checkMVNoFix_NoAgg(
            String sql, int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            boolean aggPushdown, boolean aggInline) {

        checkMVReaggreateFeature(sql, false, numGroupByOfTopAggNode, numAggsOfTopAggNode,
                -1, -1, aggPushdown, aggInline);

    }

    public void testNoFix_MVBasedQuery() {
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
        checkMVNoFix_NoAgg(sql, 1, 0, true, true);

        // Distributed group by query
        sql = "SELECT V_SUM_C1 FROM V_P1_NO_FIX_NEEDED GROUP by V_SUM_C1";
        checkMVNoFix_NoAgg(sql, 1, 0, true, true);

        sql = "SELECT V_SUM_C1, sum(V_CNT) FROM V_P1_NO_FIX_NEEDED " +
                "GROUP by V_SUM_C1";
        checkMVNoFix_NoAgg(sql, 1, 1, true, true);

        // (2) Table V_P1 and V_P1_NEW:
        pns = compileToFragments("SELECT SUM(V_SUM_C1) FROM V_P1");
        checkMVReaggregateFeature(pns, false, 0, 1, -1, -1, true, true);

        pns = compileToFragments("SELECT MIN(V_MIN_C1) FROM V_P1_NEW");
        checkMVReaggregateFeature(pns, false, 0, 1, -1, -1, true, true);

        pns = compileToFragments("SELECT MAX(V_MAX_D1) FROM V_P1_NEW");
        checkMVReaggregateFeature(pns, false, 0, 1, -1, -1, true, true);

        sql = "SELECT MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1";
        checkMVNoFix_NoAgg(sql, 1, 1, true, true);

        sql = "SELECT V_A1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1";
        checkMVNoFix_NoAgg(sql, 1, 1, true, true);

        sql = "SELECT V_A1,V_B1, MAX(V_MAX_D1) FROM V_P1_NEW GROUP BY V_A1, V_B1";
        checkMVNoFix_NoAgg(sql, 2, 1, true, true);

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
        checkMVNoFix_NoAgg(sql, 2, 1, true, true);


        sql = "select sum(v_p1.v_cnt) " +
                "from v_p1 INNER JOIN v_r1 using(v_a1)";
        checkMVNoFix_NoAgg(sql, 0, 1, true, true);

        sql = "select v_p1.v_b1, sum(v_p1.v_sum_d1) " +
                "from v_p1 INNER JOIN v_r1 on v_p1.v_a1 > v_r1.v_a1 " +
                "group by v_p1.v_b1;";
        checkMVNoFix_NoAgg(sql, 1, 1, true, true);

        sql = "select MAX(v_r1.v_a1) " +
                "from v_p1 INNER JOIN v_r1 on v_p1.v_a1 = v_r1.v_a1 " +
                "INNER JOIN r1v on v_p1.v_a1 = r1v.v_a1 ";
        checkMVNoFix_NoAgg(sql, 0, 1, true, true);
    }

    public void testMVBasedQuery_EdgeCases() {
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
        checkMVFix_TopAgg_ReAgg(sql, 1, 0, 1, 0);

        sql = "SELECT v_a1 FROM V_P1 group by v_a1";
        checkMVFix_TopAgg_ReAgg(sql, 1, 0, 1, 0);

        sql = "SELECT distinct v_cnt FROM V_P1";
        checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);

        sql = "SELECT v_cnt FROM V_P1 group by v_cnt";
        checkMVFix_TopAgg_ReAgg(sql, 1, 0, 2, 1);
    }

    public void testMVBasedQuery_NoAggQuery() {
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

        sql = "select v_p1.v_b1, v_p1.v_cnt, sum(v_p1.v_a1), max(v_p1.v_sum_c1) from v_p1 @joinType v_r1 " +
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

        sql = "select v_p1.v_a1 from v_p1 left join v_r1 on v_p1.v_a1 = v_r1.v_a1 AND v_p1.v_cnt = 2 ";
        checkMVFixWithJoin_ReAgg(sql, 2, 1, "v_cnt = 2", null);

        // When ENG-5385 is fixed, use the next line to check its plan.
//        checkMVFixWithJoin_reAgg(sql, 2, 1, null, null);
        AbstractExpression.restoreVerboseExplainForDebugging(asItWas);
    }

    public void testENG6962DistinctCases() {
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

    private void checkHavingClause(
            String sql, boolean aggInline, Object aggPostFilters) {
        List<AbstractPlanNode> pns;

        pns = compileToFragments(sql);

        AbstractPlanNode p = pns.get(0);
        AggregatePlanNode aggNode;

        List<AbstractPlanNode> nodesList = p.findAllNodesOfType(PlanNodeType.AGGREGATE);
        assertEquals(1, nodesList.size());
        p = nodesList.get(0);

        boolean isInline = p.isInline();
        assertEquals(aggInline, isInline);

        assertTrue(p instanceof AggregatePlanNode);
        aggNode = (AggregatePlanNode) p;

        String aggNodeStr = aggNode.toExplainPlanString().toLowerCase();

        if (aggPostFilters != null) {
            final String[] aggFilterStrings;
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

    private void checkMVFix_reAgg_MergeReceive(
            String sql, int numGroupByOfReaggNode, int numAggsOfReaggNode, SortDirectionType sortDirection) {
        checkMVReaggregateFeatureMergeReceive(compileToFragments(sql), true,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                sortDirection);
}

    private void checkMVFix_reAgg(
            String sql, int numGroupByOfReaggNode, int numAggsOfReaggNode) {
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
        checkMVReaggregateFeature(compileToFragments(sql), needFix,
                numGroupByOfTopAggNode, numAggsOfTopAggNode,
                numGroupByOfReaggNode, numAggsOfReaggNode,
                aggPushdown, aggInline);
    }

    // topNode, reAggNode
    private void checkMVReaggregateFeatureMergeReceive(List<AbstractPlanNode> pns,
            boolean needFix,
            int numGroupByOfReaggNode,
            int numAggsOfReaggNode,
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
        } else {
            assertNull(reAggNode);
        }

        p = pns.get(1);
        assertTrue(p instanceof SendPlanNode);
        p = p.getChild(0);

        assertTrue(p instanceof IndexScanPlanNode);
        assertEquals(sortDirection, ((IndexScanPlanNode)p).getSortDirection());
    }

    // topNode, reAggNode
    private void checkMVReaggregateFeature(
            List<AbstractPlanNode> pns, boolean needFix,
            int numGroupByOfTopAggNode, int numAggsOfTopAggNode,
            int numGroupByOfReaggNode, int numAggsOfReaggNode,
            boolean aggPushdown, boolean aggInline) {

        assertEquals(2, pns.size());
        AbstractPlanNode p = pns.get(0);
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
        HashAggregatePlanNode reAggNode;

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
        AggregatePlanNode topAggNode;
        if (p instanceof AbstractJoinPlanNode) {
            // Inline aggregation with join
            topAggNode = AggregatePlanNode.getInlineAggregationNode(p);
        } else {
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
            } else {
                assertTrue(p instanceof AggregatePlanNode);
                p = p.getChild(0);
            }
        }

        if (needFix) {
            assertTrue(p instanceof AbstractScanPlanNode);
        } else {
            assertTrue(p instanceof AbstractScanPlanNode || p instanceof AbstractJoinPlanNode);
        }

    }
}
