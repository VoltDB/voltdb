/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.planner;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.WindowFunctionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class TestWindowedFunctions extends PlannerTestCase {
    public void testOrderByAndPartitionByExpressions() throws Exception {
        try {
            compile("SELECT RANK() OVER (PARTITION BY A*A ORDER BY B) * 2 FROM AAA;");
        } catch (Exception ex) {
            fail("PartitionBy expressions in windowed expressions don't compile");
        }
        try {
            compile("SELECT RANK() OVER (PARTITION BY A ORDER BY B*B) FROM AAA order by B*B;");
        } catch (Exception ex) {
            fail("OrderBy expressions in windowed expressions don't compile");
        }
    }

    public void testRank() {
        String windowedQuery;
        windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_RANK);

        // Altering the position of the rank column does not radically
        // change the plan structure.
        windowedQuery = "SELECT RANK() OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_RANK);
        // Try some strange edge case that trivially order by a partition
        // by column, so they should trivially result in a rank of 1 for
        // each partition.

        windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 1, 2, ExpressionType.AGGREGATE_WINDOWED_RANK);

        // The order in which the PARTITION BY keys are listed should not
        // radically change the plan structure.
        windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 0, 2, ExpressionType.AGGREGATE_WINDOWED_RANK);

        // Test that we can read from a subquery.  If the sort desc is 1000, we
        // will always expect an ascending sort.
        windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY BBB.A ORDER BY ALPHA.A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateWindowedFunctionPlan(windowedQuery, 2, 100, 1, ExpressionType.AGGREGATE_WINDOWED_RANK);
    }

    public void testMin() {
        String windowedQuery;

        windowedQuery = "SELECT A+B, MOD(A, B), B, MIN(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_MIN);

        // Altering the position of the rank column does not radically
        // change the plan structure.
        windowedQuery = "SELECT MIN(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_MIN);

        // Try some strange edge case that trivially order by a partition
        // by column, so they should trivially result in a rank of 1 for
        // each partition.
        windowedQuery = "SELECT A+B, MOD(A, B), B, MIN(A+B) OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 1, 2, ExpressionType.AGGREGATE_WINDOWED_MIN);

        // The order in which the PARTITION BY keys are listed should not
        // radically change the plan structure.
        windowedQuery = "SELECT A+B, MOD(A, B), B, MIN(A+B) OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 0, 2, ExpressionType.AGGREGATE_WINDOWED_MIN);

        // Test that we can read from a subquery.  If the sort desc is 1000, we
        // will always expect an ascending sort.
        windowedQuery = "SELECT BBB.B, MIN(BBB.A+BBB.B) OVER (PARTITION BY BBB.A ORDER BY ALPHA.A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateWindowedFunctionPlan(windowedQuery, 2, 100, 1, ExpressionType.AGGREGATE_WINDOWED_MIN);
    }

    public void testMax() {
        String windowedQuery;

        windowedQuery = "SELECT A+B, MOD(A, B), B, MAX(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_MAX);

        // Altering the position of the rank column does not radically
        // change the plan structure.
        windowedQuery = "SELECT MAX(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_MAX);

        // Try some strange edge case that trivially order by a partition
        // by column, so they should trivially result in a rank of 1 for
        // each partition.
        windowedQuery = "SELECT A+B, MOD(A, B), B, MAX(A+B) OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 1, 2, ExpressionType.AGGREGATE_WINDOWED_MAX);

        // The order in which the PARTITION BY keys are listed should not
        // radically change the plan structure.
        windowedQuery = "SELECT A+B, MOD(A, B), B, MAX(A+B) OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 0, 2, ExpressionType.AGGREGATE_WINDOWED_MAX);

        // Test that we can read from a subquery.  If the sort desc is 1000, we
        // will always expect an ascending sort.
        windowedQuery = "SELECT BBB.B, MAX(BBB.A+BBB.B) OVER (PARTITION BY BBB.A ORDER BY ALPHA.A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateWindowedFunctionPlan(windowedQuery, 2, 100, 1, ExpressionType.AGGREGATE_WINDOWED_MAX);
    }

    public void testSum() {
        String windowedQuery;

        windowedQuery = "SELECT A+B, MOD(A, B), B, SUM(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_SUM);

        // Altering the position of the rank column does not radically
        // change the plan structure.
        windowedQuery = "SELECT SUM(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_SUM);

        // Try some strange edge case that trivially order by a partition
        // by column, so they should trivially result in a rank of 1 for
        // each partition.
        windowedQuery = "SELECT A+B, MOD(A, B), B, SUM(A+B) OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 1, 2, ExpressionType.AGGREGATE_WINDOWED_SUM);

        // The order in which the PARTITION BY keys are listed should not
        // radically change the plan structure.
        windowedQuery = "SELECT A+B, MOD(A, B), B, SUM(A+B) OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 0, 2, ExpressionType.AGGREGATE_WINDOWED_SUM);

        // Test that we can read from a subquery.  If the sort desc is 1000, we
        // will always expect an ascending sort.
        windowedQuery = "SELECT BBB.B, SUM(BBB.A+BBB.B) OVER (PARTITION BY BBB.A ORDER BY ALPHA.A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateWindowedFunctionPlan(windowedQuery, 2, 100, 1, ExpressionType.AGGREGATE_WINDOWED_SUM);
    }

    public void testCount() {
        String windowedQuery;

        windowedQuery = "SELECT A+B, MOD(A, B), B, COUNT(*) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_COUNT);

        windowedQuery = "SELECT A+B, MOD(A, B), B, COUNT(A+B) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_COUNT);

        // Altering the position of the rank column does not radically
        // change the plan structure.
        windowedQuery = "SELECT COUNT(*) OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2, ExpressionType.AGGREGATE_WINDOWED_COUNT);
        // Try some strange edge case that trivially order by a partition
        // by column, so they should trivially result in a rank of 1 for
        // each partition.

        windowedQuery = "SELECT A+B, MOD(A, B), B, COUNT(*) OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 1, 2, ExpressionType.AGGREGATE_WINDOWED_COUNT);

        // The order in which the PARTITION BY keys are listed should not
        // radically change the plan structure.
        windowedQuery = "SELECT A+B, MOD(A, B), B, COUNT(*) OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 0, 2, ExpressionType.AGGREGATE_WINDOWED_COUNT);

        // Test that we can read from a subquery.  If the sort desc is 1000, we
        // will always expect an ascending sort.
        windowedQuery = "SELECT BBB.B, COUNT(*) OVER (PARTITION BY BBB.A ORDER BY ALPHA.A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateWindowedFunctionPlan(windowedQuery, 2, 100, 1, ExpressionType.AGGREGATE_WINDOWED_COUNT);
    }

    /**
     * Validate that each similar windowed query in testRank produces a similar
     * plan, with the expected minor variation to its ORDER BY node.
     * @param windowedQuery a variant of a test query of a known basic format
     * @param nSorts the expected number of sort criteria that should have been
     *        extracted from the variant query's PARTITION BY and ORDER BY.
     * @param descSortIndex the position among the sort criteria of the original
     *        ORDER BY column, always distinguishable by its "DESC" direction.
     **/
    private void validateWindowedFunctionPlan(String windowedQuery, int nSorts, int descSortIndex, int numPartitionExprs, ExpressionType winOpType) {
        // Sometimes we get multi-fragment nodes when we
        // expect single fragment nodes.  Keeping all the fragments
        // helps to diagnose the problem.
        List<AbstractPlanNode> nodes = compileToFragments(windowedQuery);
        assertEquals(1, nodes.size());

        AbstractPlanNode node = nodes.get(0);
        // The plan should look like:
        // SendNode -> ProjectionPlanNode -> PartitionByPlanNode -> OrderByPlanNode -> SeqScanNode
        // We also do some sanity checking on the PartitionPlan node.
        // First dissect the plan.
        assertTrue(node instanceof SendPlanNode);

        AbstractPlanNode projPlanNode = node.getChild(0);
        assertTrue(projPlanNode instanceof ProjectionPlanNode);

        AbstractPlanNode windowFuncPlanNode = projPlanNode.getChild(0);
        assertTrue(windowFuncPlanNode instanceof WindowFunctionPlanNode);

        AbstractPlanNode abstractOrderByNode = windowFuncPlanNode.getChild(0);
        assertTrue(abstractOrderByNode instanceof OrderByPlanNode);
        OrderByPlanNode orderByNode = (OrderByPlanNode)abstractOrderByNode;
        NodeSchema input_schema = orderByNode.getOutputSchema();
        assertNotNull(input_schema);

        AbstractPlanNode seqScanNode = orderByNode.getChild(0);
        assertTrue(seqScanNode instanceof SeqScanPlanNode || seqScanNode instanceof NestLoopPlanNode);

        WindowFunctionPlanNode wfPlanNode = (WindowFunctionPlanNode)windowFuncPlanNode;
        NodeSchema  schema = wfPlanNode.getOutputSchema();

        //
        // Check that the window function plan node's output schema is correct.
        // Look at the first expression, to verify that it's the windowed expression.
        // Then check that the TVEs all make sense.
        //
        SchemaColumn column = schema.getColumn(0);
        assertEquals("ARANK", column.getColumnAlias());
        assertEquals(numPartitionExprs, wfPlanNode.getPartitionByExpressions().size());
        validateTVEs(input_schema, wfPlanNode, false);
        //
        // Check that the operation is what we expect.
        //
        assertTrue(wfPlanNode.getAggregateTypes().size() > 0);
        assertEquals(winOpType, wfPlanNode.getAggregateTypes().get(0));
        //
        // Check that all the arguments of all the aggregates in the
        // window function plan node have types.  Some have no exprs,
        // So the list of aggregates may be null.  That's ok.
        //
        for (List<AbstractExpression> exprs : wfPlanNode.getAggregateExpressions()) {
            if (exprs != null) {
                for (AbstractExpression expr : exprs) {
                    assertNotNull(expr.getValueType());
                }
            }
        }
        //
        // Check that the order by node has the right number of expressions.
        // and that they have the correct order.
        //
        assertEquals(nSorts, orderByNode.getSortExpressions().size());
        int sortIndex = 0;
        for (SortDirectionType direction : orderByNode.getSortDirections()) {
            SortDirectionType expected = (sortIndex == descSortIndex) ?
                    SortDirectionType.DESC : SortDirectionType.ASC;
            assertEquals(expected, direction);
            ++sortIndex;
        }
    }

    public void validateTVEs(
            NodeSchema input_schema,
            WindowFunctionPlanNode pbPlanNode,
            boolean waiveAliasMatch) {
        List<AbstractExpression> tves = new ArrayList<>();
        for (AbstractExpression ae : pbPlanNode.getPartitionByExpressions()) {
            tves.addAll(ae.findAllTupleValueSubexpressions());
        }
        for (AbstractExpression ae : tves) {
            TupleValueExpression tve = (TupleValueExpression)ae;
            assertTrue(0 <= tve.getColumnIndex() && tve.getColumnIndex() < input_schema.size());
            SchemaColumn col = input_schema.getColumn(tve.getColumnIndex());
            String msg = String.format("TVE %d, COL %s: ",
                                       tve.getColumnIndex(),
                                       col.getColumnName() + ":" + col.getColumnAlias());
            assertEquals(msg, col.getTableName(), tve.getTableName());
            assertEquals(msg, col.getTableAlias(), tve.getTableAlias());
            assertEquals(msg, col.getColumnName(), tve.getColumnName());
            if ( ! waiveAliasMatch) {
                assertEquals(msg, col.getColumnAlias(), tve.getColumnAlias());
            }
        }
    }

    public String nodeSchemaString(String label, NodeSchema schema) {
        StringBuffer sb = new StringBuffer();
        sb.append(label).append(": \n");
        for (SchemaColumn col : schema) {
            sb.append("  ")
              .append(col.getTableName()).append(": ")
              .append(col.getTableAlias()).append(", ")
              .append(col.getColumnName()).append(": ")
              .append(col.getColumnAlias()).append(";");
            sb.append("\n");
        }
        return sb.toString();
    }

    public void testRankWithSubqueries() {
        String windowedQuery;
        // The following variants exercise resolving columns to subquery result columns.
        // At one point in development, this would only work by disabling ALPHA.A as a possible resolution.
        // It got a mysterious "Mismatched columns A in subquery" error.
        windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY A ORDER BY BBB.B ) AS ARANK FROM (select A AS NOT_A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateQueryWithSubquery(windowedQuery, false);
        windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY RENAMED_A ORDER BY BBB.B ) AS ARANK FROM (select A AS RENAMED_A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateQueryWithSubquery(windowedQuery, true);
        windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY BBB.A ORDER BY BBB.B ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateQueryWithSubquery(windowedQuery, false);
        windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY ALPHA.A ORDER BY BBB.B ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateQueryWithSubquery(windowedQuery, false);

        // Test with windowed aggregates in the subquery itself.

        // First, use a windowed PARTITION BY which is a table partition column. The PARTITION BY node can then be
        // distributed.  So, we expect 0 coordinator partition by plan nodes and 1 distributed partition by plan nodes.
        windowedQuery = "SELECT * FROM ( SELECT A, B, C, RANK() OVER (PARTITION BY A ORDER BY B) FROM AAA_PA) ARANK;";
        validateQueryWithSubqueryWithWindowedAggregate(windowedQuery, 0, 1);

        // Now, use a windowed PARTITION BY which is not in a table partition column.  The partition by
        // node can no longer be distributed.  So we expect it to show up in the coordinator fragment.
        windowedQuery = "SELECT * FROM ( SELECT A, B, C, RANK() OVER (PARTITION BY B ORDER BY A) FROM AAA_PA ) ARANK;";
        validateQueryWithSubqueryWithWindowedAggregate(windowedQuery, 1, 0);

        // Test that putting a windowed aggregate in the outer selection list gets about the
        // same answers.  The outer windowed aggregate adds 1 to all the PB counts on the
        // coordinator fragment.
        windowedQuery = "SELECT *, RANK() OVER (PARTITION BY A ORDER BY B) FROM ( SELECT A, B, C, RANK() OVER (PARTITION BY A ORDER BY B) FROM AAA_PA) ARANK;";
        validateQueryWithSubqueryWithWindowedAggregate(windowedQuery, 1, 1);

        // Now, use a window partition by which is not in the table partition column.  The partition by
        // node can no longer be distributed.  So we expect it to show up in the coordinator fragment.
        windowedQuery = "SELECT *, RANK() OVER (PARTITION BY B ORDER BY A) FROM ( SELECT A, B, C, RANK() OVER (PARTITION BY B ORDER BY A) FROM AAA_PA ) ARANK;";
        validateQueryWithSubqueryWithWindowedAggregate(windowedQuery, 2, 0);
    }

    private void validateQueryWithSubqueryWithWindowedAggregate(String windowedQuery, int numCoordinatorPartitionBys, int numDistributedPartitionBys) {
        List<AbstractPlanNode> nodes = compileToFragments(windowedQuery);

        assertEquals(2, nodes.size());
        assertTrue(nodes.get(0) instanceof SendPlanNode);
        int numCoordPBNodes = countPBNodes(nodes.get(0));
        int numDistPBNodes  = countPBNodes(nodes.get(1));

        assertEquals(numCoordinatorPartitionBys, numCoordPBNodes);
        assertEquals(numDistributedPartitionBys, numDistPBNodes);
    }

    private int countPBNodes(AbstractPlanNode node) {
        int answer = 0;
        while (node.getChildCount() > 0) {
            if (node instanceof WindowFunctionPlanNode) {
                answer += 1;
            }
            node = node.getChild(0);
        }
        return answer;
    }

    /**
     * Validate that each similar windowed query in testRankWithSubqueries
     * produces a similar plan
     * @param windowedQuery a variant of a test query of a known basic format
     **/
    private void validateQueryWithSubquery(String windowedQuery,
            boolean waiveAliasMatch) {
        AbstractPlanNode node = compile(windowedQuery);
        // Dissect the plan.
        assertTrue(node instanceof SendPlanNode);
        AbstractPlanNode projectionPlanNode = node.getChild(0);
        assertTrue(projectionPlanNode instanceof ProjectionPlanNode);

        AbstractPlanNode partitionByPlanNode = projectionPlanNode.getChild(0);
        assertTrue(partitionByPlanNode instanceof WindowFunctionPlanNode);

        AbstractPlanNode orderByPlanNode = partitionByPlanNode.getChild(0);
        assertTrue(orderByPlanNode instanceof OrderByPlanNode);
        NodeSchema input_schema = orderByPlanNode.getOutputSchema();

        AbstractPlanNode scanNode = orderByPlanNode.getChild(0);
        assertTrue(scanNode instanceof NestLoopPlanNode);

        NodeSchema  schema = partitionByPlanNode.getOutputSchema();
        SchemaColumn column = schema.getColumn(0);
        assertEquals("ARANK", column.getColumnAlias());

        validateTVEs(input_schema, (WindowFunctionPlanNode)partitionByPlanNode,
                waiveAliasMatch);
    }

    public void testRankWithPartitions() {
        String windowedQuery;
        // Validate a plan with a rank expression with one partitioned column in the partition by list.
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY A ORDER BY B) R FROM AAA_PA;";
        validatePartitionedQuery(windowedQuery, false);
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY A ORDER BY B) R FROM AAA_PA ORDER BY A, B, C, R;";
        validatePartitionedQuery(windowedQuery, true);
        // Validate a plan with a rank expression with one partitioned column in the order by list.
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY B ORDER BY A) R FROM AAA_PA;";
        validatePartitionedQuery(windowedQuery, false);
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY B ORDER BY A) R FROM AAA_PA ORDER BY A, B, C, R;";
        validatePartitionedQuery(windowedQuery, true);
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY A, C ORDER BY B) R FROM AAA_PA";
        validatePartitionedQuery(windowedQuery, false);

        // Validate plan with a rank expression with one partitioned column and one non-partitioned
        // column in the partition by list.
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY A, C ORDER BY B) R FROM AAA_PA;";
        validatePartitionedQuery(windowedQuery, false);
        // The same as the previous two tests, but swap the partition by columns.
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY C, A ORDER BY B) R FROM AAA_PA;";
        validatePartitionedQuery(windowedQuery, false);
        windowedQuery = "SELECT A, B, C, RANK() OVER (PARTITION BY C, A ORDER BY B) R FROM AAA_PA ORDER BY A, B, C, R;";
        validatePartitionedQuery(windowedQuery, true);
        // Test that we can read from a partitioned table, but the windowed
        // partition by is not a partition column.
        windowedQuery = "Select A, B, C, Rank() Over (Partition By C Order By B) ARANK From AAA_STRING_PA;";
        validatePartitionedQuery(windowedQuery, false);
    }

    private void validatePartitionedQuery(String query, boolean hasStatementOrderBy) {
        List<AbstractPlanNode> nodes = compileToFragments(query);
        assertEquals(2, nodes.size());
        AbstractPlanNode child = nodes.get(0);

        // Validate the coordinator fragment.
        assertTrue(child instanceof SendPlanNode);
        child = child.getChild(0);
        assertTrue(child instanceof ProjectionPlanNode);
        if (hasStatementOrderBy) {
            child = child.getChild(0);
            assertTrue(child instanceof OrderByPlanNode);
        }
        child = child.getChild(0);
        assertTrue(child instanceof WindowFunctionPlanNode);
        child = child.getChild(0);
        assertTrue(child instanceof OrderByPlanNode);
        child = child.getChild(0);
        assertTrue(child instanceof ReceivePlanNode);
        assertEquals(0, child.getChildCount());

        // Get the distributed fragment.
        child = nodes.get(1);
        assertTrue(child instanceof SendPlanNode);
        child = child.getChild(0);
        assertTrue(child instanceof SeqScanPlanNode);
        assertEquals(0, child.getChildCount());
    }

    public void testWindowFailures() {
        failToCompile("SELECT AVG(A+B) OVER (PARTITION BY A ORDER BY B ) FROM AAA GROUP BY A;",
                      "Unsupported window function AVG");
    }

    public void testRankFailures() {
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) FROM AAA GROUP BY A;",
                      "Use of both a windowed function call and GROUP BY in a single query is not supported.");
        failToCompile("SELECT RANK() OVER (ORDER BY B), COUNT(*) FROM AAA;",
                      "Use of window functions (in an OVER clause) isn't supported with other aggregate functions on the SELECT list.");
        failToCompile("SELECT RANK() OVER (ORDER BY B), COUNT(B) FROM AAA;",
                      "Use of window functions (in an OVER clause) isn't supported with other aggregate functions on the SELECT list.");
        failToCompile("SELECT RANK() OVER (ORDER BY B), MAX(B) FROM AAA;",
                      "Use of window functions (in an OVER clause) isn't supported with other aggregate functions on the SELECT list.");
        failToCompile("SELECT RANK() OVER (ORDER BY B), AVG(B) FROM AAA;",
                      "Use of window functions (in an OVER clause) isn't supported with other aggregate functions on the SELECT list.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) AS R1, " +
                      "       RANK() OVER (PARTITION BY B ORDER BY A ) AS R2  " +
                      "FROM AAA;",
                      "Only one windowed function call may appear in a selection list.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY CAST(A AS FLOAT)) FROM AAA;",
                      "Windowed function call expressions can have only integer or TIMESTAMP value types in the ORDER BY expression of their window.");
        // Windowed expressions can only appear in the selection list.
        failToCompile("SELECT A, B, C FROM AAA WHERE RANK() OVER (PARTITION BY A ORDER BY B) < 3;",
                      "Windowed function call expressions can only appear in the selection list of a query or subquery.");
        failToCompile("SELECT COUNT((SELECT DISTINCT A FROM AAA)) OVER (PARTITION BY A) FROM AAA;",
                      "Window function calls with subquery expression arguments are not allowed.");

        // Detect that PARTITION BY A is ambiguous when A names multiple columns.
        // Queries like this passed at one point in development, ignoring the subquery
        // result column as a possible binding for A.
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY A, B) AS ARANK " +
                      "FROM (select A, B, C from AAA where A < B) ALPHA, BBB " +
                      "WHERE ALPHA.C <> BBB.C;",
                      "Column \"A\" is ambiguous.  It\'s in tables: ALPHA, BBB");
        failToCompile("SELECT RANK() OVER () AS ARANK " +
                      "FROM AAA;",
                      "Windowed RANK function call expressions require an ORDER BY specification.");
        failToCompile("SELECT DENSE_RANK() OVER () AS ARANK " +
                      "FROM AAA;",
                      "Windowed DENSE_RANK function call expressions require an ORDER BY specification.");
        failToCompile("SELECT RANK(DISTINCT) over (PARTITION BY A ORDER BY B) AS ARANK FROM AAA",
                      "Expected a right parenthesis (')') here.");
        failToCompile("SELECT DENSE_RANK(ALL) over (PARTITION BY A ORDER BY B) AS ARANK FROM AAA",
                      "Expected a right parenthesis (')') here.");
    }

    /*
     * Many of the failures in rank are generic, so we don't test them here.
     */
    public void testMinMaxSumCountFailures() {
        failToCompile("SELECT COUNT(DISTINCT *) OVER (PARTITION BY A ORDER BY B) AS ARANK FROM AAA",
                      "DISTINCT is not allowed in window functions.");
        failToCompile("SELECT COUNT(DISTINCT A+B) OVER (PARTITION BY A ORDER BY B) AS ARANK FROM AAA",
                      "DISTINCT is not allowed in window functions.");
        failToCompile("SELECT SUM(A) OVER (PARTITION BY A ORDER BY B) AS ARANK FROM AAA_TIMESTAMP",
                      "Windowed SUM must have exactly one numeric argument");
        failToCompile("SELECT COUNT(DISTINCT *) OVER (PARTITION BY A ORDER BY B) AS ARANK FROM AAA",
                      "DISTINCT is not allowed in window functions.");
        failToCompile("SELECT COUNT(DISTINCT A+B) OVER (PARTITION BY A ORDER BY B) AS ARANK FROM AAA",
                      "DISTINCT is not allowed in window functions.");
        failToCompile("SELECT SUM(A) OVER (ORDER BY (SELECT A FROM AAA)) AS ARANK FROM AAA_TIMESTAMP",
                      "SQL window functions cannot be ordered by subquery expression arguments.");
        failToCompile("SELECT SUM(A) OVER (PARTITION BY (SELECT A FROM AAA)) AS ARANK FROM AAA_TIMESTAMP",
                      "SQL window functions cannot be partitioned by subquery expression arguments.");
    }
    public void testExplainPlanText() {
        String windowedQuery = "SELECT RANK() OVER (PARTITION BY A ORDER BY B DESC) FROM AAA;";
        AbstractPlanNode plan = compile(windowedQuery);
        String explainPlanText = plan.toExplainPlanString();
        String expected = "WINDOW FUNCTION AGGREGATION: ops: AGGREGATE_WINDOWED_RANK()";
        assertTrue("Expected to find \"" + expected + "\" in explain plan text, but did not:\n"
                + explainPlanText, explainPlanText.contains(expected));
    }

    // This can be used to disable particular tests.
    // Change IS_ENABLED to false, and then change all
    // the IS_ENABLED occurrences to something else, like
    // IS_DEBUGGING, which you will have to define here.
    private static boolean IS_ENABLED = true;

    /**
     * There is some theory here.  There are four ranges of variation
     * in these tests.  They are:
     * <ol>
     *   <li>Does the statement have a Statement Level Order By, or SLOB?</li>
     *   <li>Does the statement have a window function, or WF?</li>
     *   <li>Is the statement MP or SP?</li>
     *   <li>Can the statement use an index?  This can have some variation
     *       itself:
     *       <ol>
     *         <li>Can the statement use an index at all?</li>
     *         <li>Can the statement use an index for a WF but not an SLOB,
     *             or for an SLOB but not a WF or for both?
     *         <li>Is there an index which can be used, say for a filter, but
     *             but not for an SLOB or a WF?
     *       </ol>
     *   </li>
     * </ol>
     */
    public void testWindowFunctionsWithIndexes() {
        //   1: No SLOB, No WF, SP Query, noindex
        //      Expect SeqScan
        //   query: select * from vanilla;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.SEQSCAN)
                         );
        }
        //   2: No SLOB, No WF, MP Query, noindex
        //      Expect RECV -> SEND -> SeqScan
        //   select * from vanilla_pa;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pa",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.SEQSCAN));
        }

        //   3: No SLOB, No WF, SP Query, index(NONEIndex)
        //      Expect IndxScan
        //   select * from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx where a = 1",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx where a < 1",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }

        // -- Force us to use the index on column vanilla_pb_idx.a
        // -- which in this case is not the partition column.
        //   4: No SLOB, No WF, MP Query, index(NONEIndex)
        //      Expect RECV -> SEND -> IndxScan
        //   select * from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx where a = 1",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx where a < 1",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //   5: No SLOB, One WF, SP Query, noindex
        //      Expect WinFun -> OrderBy -> SeqScan
        //   select a, b, max(b) over ( partition by a ) from vanilla;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.SEQSCAN));
        }
        //   6: No SLOB, One WF, MP Query, noindex
        //      Expect WinFun -> OrderBy -> RECV -> SEND -> SeqScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_pa;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pa;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.SEQSCAN));
        }
        //   7: No SLOB, one WF, SP Query, index (Can order the WF)
        //      Expect WinFun -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx where a < 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        //   7a: No SLOB, one WF, SP Query, index (Only to order the WF)
        //      Expect WinFun -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_idx;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        //   8: No SLOB, one WF, MP Query, index (Can order the WF)
        //      Expect WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // (WF)),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a < 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // (WF)),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //   8a: No SLOB, one WF, MP Query, index (Only to order the WF)
        //      Expect WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_pb_idx;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pb_idx;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // WF
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //   9: No SLOB, one WF, SP Query, index (not for the WF)
        //      Expect WinFun -> OrderBy -> IndxScan
        //   select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_idx where a < 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.INDEXSCAN));
        }
        //  10: No SLOB, one WF, MP Query, index (not for the WF)
        //      Expect WinFun -> OrderBy -> RECV -> SEND -> IndxScan
        //   select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a < 1;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  11: SLOB, No WF, SP Query, noindex
        //      Expect OrderBy(SLOB) -> SeqScan
        //   select * from vanilla order by a;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY, // (SLOB)
                                  PlanNodeType.SEQSCAN));
        }
        //  12: SLOB, No WF, MP Query, noindex
        //      Expect MergeReceive with OrderBy(SLOB) -> RECV -> SEND -> ORDER BY -> SeqScan
        //   select * from vanilla_pa order by b;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pa order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.MERGERECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.SEQSCAN));
        }
        //  13: SLOB, No WF, SP Query, index (Can order the SLOB)
        //      Expect PlanNodeType.INDEXSCAN
        //   explain select * from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx where a = 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx where a < 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  13a: SLOB, No WF, SP Query, index (only to order the SLOB)
        //      Expect PlanNodeType.INDEXSCAN
        //   explain select * from vanilla_idx order by a;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  14: SLOB, No WF, MP Query, index (Can order the SLOB)
        //      Expect MrgRecv(SLOB) -> SEND -> IndxScan
        //   select * from vanilla_pb_idx order by a;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.MERGERECEIVE), // SLOB
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  14a: SLOB, No WF, MP Query, index (Only to order the SLOB)
        //      Expect MrgRecv(SLOB) -> SEND -> IndxScan
        //   select * from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx where a = 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.MERGERECEIVE), // SLOB
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx where a < 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.MERGERECEIVE), // SLOB
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  15: SLOB, No WF, SP Query, index (Cannot order the SLOB)
        //      Expect OrderBy(SLOB) -> IndxScan
        //   select * from vanilla_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx where a = 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_idx where a < 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.INDEXSCAN));
        }
        //  16: SLOB, No WF, MP Query, index (Cannot order the SLOB)
        //      Expect MERGERECEIVE with (OrderBy(SLOB)) -> RECV -> SEND -> ORDER BY IndxScan
        //   select * from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx where a = 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.MERGERECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select * from vanilla_pb_idx where a < 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.MERGERECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.INDEXSCAN));
        }
        //  17: SLOB, One WF, SP Query, index (Cannot order SLOB or WF)
        //      Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> IndxScan
        //   select a, b, max(b) over (partition by b) from vanilla_idx where a = 1 order by c;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over (partition by b) from vanilla_idx where a = 1 order by c;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over (partition by b) from vanilla_idx where a < 1 order by c;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.INDEXSCAN));
        }
        //  18: SLOB, One WF, MP Query, index (Cannot order SLOB or WF)
        //      Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> IndxScan
        //   select a, b, max(b) over ( partition by c ) from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by c ) from vanilla_pb_idx where a = 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by c ) from vanilla_pb_idx where a < 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  19: SLOB, one WF, SP Query, index (Can order the WF, Cannot order the SLOB)
        //      Expect OrderBy(SLOB) -> WinFun -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx where a < 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        //  19a: SLOB, one WF, SP Query, index (Only to order the WF, not SLOB)
        //      Expect OrderBy(SLOB) -> WinFun -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_idx order by b;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        //  20: SLOB, one WF, MP Query, index (Can order the WF, not SLOB)
        //      Expect OrderBy(SLOB) -> WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // WF
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a < 1 order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // WF
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  20a: SLOB, one WF, MP Query, index (Can order the WF, not SLOB)
        //      Expect OrderBy(SLOB) -> WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //   select a, b, max(b) over ( partition by a ) from vanilla_pb_idx order by b;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_pb_idx order by b;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // WF
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  21: SLOB, one WF, SP Query, index (Can order the SLOB, not WF)
        //      The index is not usable for the SLOB, since the WF invalidates the order.
        //       Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> IndxScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB, can't use index),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_idx where a < 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB, can't use index),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.INDEXSCAN));
        }
        //  21a: SLOB, one WF, SP Query, index (Can order the SLOB, not WF)
        //      The index is unusable for the SLOB, since the WF invalidates the order.
        //       Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> SeqScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_idx order by a;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB, can't use index),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.SEQSCAN));
        }
        //  22: SLOB, one WF, MP Query, index (Can order the SLOB, not WF)
        //      The index is unusable by the SLOB since the WF invalidates it.
        //       Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a < 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        //  22a: SLOB, one WF, MP Query, index (Can order the SLOB, not WF)
        //      The index is unusable by the SLOB since the WF invalidates it.
        //      Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> SeqScan
        //  select a, b, max(b) over ( partition by b ) from vanilla_pb_idx order by a;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by b ) from vanilla_pb_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY, // (SLOB),
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY, // (WF),
                                  PlanNodeType.RECEIVE),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.SEQSCAN));
        }
        //  23: SLOB, one WF, SP Query, index (Can order the WF and SLOB both)
        //      Expect WinFun -> IndxScan
        // select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select a, b, max(b) over ( partition by a ) from vanilla_idx where a < 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        // 23a: SLOB, one WF, SP Query, index (Can order the WF and SLOB both)
        //      Expect WinFun -> IndxScan
        // select a, b, max(b) over ( partition by a ) from vanilla_idx order by a;
        if (IS_ENABLED) {
            validatePlan("select max(b) over ( partition by a ) from vanilla_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("SELECT * FROM O3 WHERE PK1 = 0 ORDER BY PK2 DESC;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.INDEXSCAN));
        }
        // 24: SLOB, one WF, MP Query, index (For the WF and SLOB both)
        //      Expect WinFun -> MrgRecv(SLOB or WF) -> SEND -> IndxScan
        // select max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validatePlan("select max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // (SLOB or WF)
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("select max(b) over ( partition by a ) from vanilla_pb_idx where a < 1 order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.MERGERECEIVE), // (SLOB or WF)
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }

        // This is one of the queries from the regression test.
        // It is here because it tests that the window function
        // and order by function have the same expressions but
        // different sort directions.
        if (IS_ENABLED) {
            validatePlan("select a, rank() over (order by a desc) from vanilla_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
            validatePlan("select a, rank() over (order by a) from vanilla_idx order by a desc;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));

            // These are like the last one, but the window function
            // and order by have the same orders.
            validatePlan("select a, rank() over (order by a) from vanilla_idx order by a;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
            validatePlan("select a, rank() over (order by a desc) from vanilla_idx order by a desc;",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            // Check to see that order information is
            // propagated through outer branches of
            // joins.  The order the tables are given in
            // the command should not matter, so
            // test with both orders.  Both should
            // produce the same plan, though they need
            // to order by the one with the index.
            validatePlan("select * from vanilla_idx as oo, vanilla as ii order by oo.a, oo.b",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.NESTLOOP,
                                  PlanNodeType.INDEXSCAN));
            validatePlan("select * from vanilla as oo, vanilla_idx as ii order by ii.a, ii.b",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.NESTLOOP,
                                  PlanNodeType.INDEXSCAN));
            validatePlan("select * from vanilla_idx as oo join vanilla_idx as ii on oo.a = ii.a and oo.b = ii.b order by ii.a, ii.b",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.NESTLOOPINDEX,
                                  PlanNodeType.INDEXSCAN));
        }
        if (IS_ENABLED) {
            // Test that similar indexes don't cause
            // problems.
            //
            // We have an index on CTR + 100.
            validatePlan("select * from O4 where CTR + 100 < 1000 order by id",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.INDEXSCAN));
            // We don't have an index on CTR + 200.
            // But this is planned as CTR + P where P is a
            // parameter which knows it has been created from
            // a value of 100.  So, when we add 200 we should
            // not match, and we should not get an INDEXSCAN.
            validatePlan("select * from O4 where CTR + 200 < 100 order by id",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.SEQSCAN));
        }
        if (IS_ENABLED) {
            validatePlan("SELECT *, RANK() OVER ( ORDER BY ID ) FUNC FROM (SELECT *, RANK() OVER ( ORDER BY ID DESC ) SUBFUNC FROM P_DECIMAL W12) SUB",
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  PlanNodeType.ORDERBY,
                                  PlanNodeType.SEQSCAN,
                                  PlanNodeType.PROJECTION,
                                  PlanNodeType.WINDOWFUNCTION,
                                  planWithInlineNodes(PlanNodeType.MERGERECEIVE, PlanNodeType.ORDERBY)),
                         fragSpec(PlanNodeType.SEND,
                                  PlanNodeType.INDEXSCAN));
        }
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestWindowedFunctions.class.getResource("testplans-windowingfunctions-ddl.sql"), "testwindowfunctions");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
