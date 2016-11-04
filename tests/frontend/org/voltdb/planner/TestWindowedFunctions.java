/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2);

        // Altering the position of the rank column does not radically
        // change the plan structure.
        windowedQuery = "SELECT RANK() OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 3, 2, 2);
        // Try some strange edge case that trivially order by a partition
        // by column, so they should trivially result in a rank of 1 for
        // each partition.

        windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 1, 2);

        // The order in which the PARTITION BY keys are listed should not
        // radically change the plan structure.
        windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
        validateWindowedFunctionPlan(windowedQuery, 2, 0, 2);

        // Test that we can read from a subquery.  If the sort desc is 1000, we
        // will always expect an ascending sort.
        windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY BBB.A ORDER BY ALPHA.A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
        validateWindowedFunctionPlan(windowedQuery, 2, 100, 1);
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
    private void validateWindowedFunctionPlan(String windowedQuery, int nSorts, int descSortIndex, int numPartitionExprs) {
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

        AbstractPlanNode partitionByPlanNode = projPlanNode.getChild(0);
        assertTrue(partitionByPlanNode instanceof WindowFunctionPlanNode);

        AbstractPlanNode abstractOrderByNode = partitionByPlanNode.getChild(0);
        assertTrue(abstractOrderByNode instanceof OrderByPlanNode);
        OrderByPlanNode orderByNode = (OrderByPlanNode)abstractOrderByNode;
        NodeSchema input_schema = orderByNode.getOutputSchema();
        assertNotNull(input_schema);

        AbstractPlanNode seqScanNode = orderByNode.getChild(0);
        assertTrue(seqScanNode instanceof SeqScanPlanNode || seqScanNode instanceof NestLoopPlanNode);

        WindowFunctionPlanNode pbPlanNode = (WindowFunctionPlanNode)partitionByPlanNode;
        NodeSchema  schema = pbPlanNode.getOutputSchema();

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

        //
        // Check that the partition by plan node's output schema is correct.
        // Look at the first expression, to verify that it's the windowed expression.
        // Then check that the TVEs all make sense.
        //
        SchemaColumn column = schema.getColumns().get(0);
        assertEquals("ARANK", column.getColumnAlias());
        assertEquals(numPartitionExprs, pbPlanNode.getPartitionByExpressions().size());
        validateTVEs(input_schema, pbPlanNode, false);
    }

    public void validateTVEs(
            NodeSchema input_schema,
            WindowFunctionPlanNode pbPlanNode,
            boolean waiveAliasMatch) {
        List<AbstractExpression> tves = new ArrayList<>();
        for (AbstractExpression ae : pbPlanNode.getPartitionByExpressions()) {
            tves.addAll(ae.findAllTupleValueSubexpressions());
        }
        List<SchemaColumn> columns = input_schema.getColumns();
        for (AbstractExpression ae : tves) {
            TupleValueExpression tve = (TupleValueExpression)ae;
            assertTrue(0 <= tve.getColumnIndex() && tve.getColumnIndex() < columns.size());
            SchemaColumn col = columns.get(tve.getColumnIndex());
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
        List<SchemaColumn> columns = schema.getColumns();
        StringBuffer sb = new StringBuffer();
        sb.append(label).append(": \n");
        for (SchemaColumn col : columns) {
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
        SchemaColumn column = schema.getColumns().get(0);
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

    public void testRankFailures() {
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) FROM AAA GROUP BY A;",
                      "Use of both a windowed function call and GROUP BY in a single query is not supported.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) AS R1, " +
                      "       RANK() OVER (PARTITION BY B ORDER BY A ) AS R2  " +
                      "FROM AAA;",
                      "Only one windowed function call may appear in a selection list.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY A, B) FROM AAA;",
                      "Windowed function call expressions can have only one ORDER BY expression in their window.");

        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY CAST(A AS FLOAT)) FROM AAA;",
                      "Windowed function call expressions can have only integer or TIMESTAMP value types in the ORDER BY expression of their window.");
        // Windowed expressions can only appear in the selection list.
        failToCompile("SELECT A, B, C FROM AAA WHERE RANK() OVER (PARTITION BY A ORDER BY B) < 3;",
                      "Windowed function call expressions can only appear in the selection list of a query or subquery.");

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

    public void testExplainPlanText() {
        String windowedQuery = "SELECT RANK() OVER (PARTITION BY A ORDER BY B DESC) FROM AAA;";
        AbstractPlanNode plan = compile(windowedQuery);
        String explainPlanText = plan.toExplainPlanString();
        String expected = "WindowFunctionPlanNode: ops: AGGREGATE_WINDOWED_RANK()";
        assertTrue("Expected to find \"" + expected + "\" in explain plan text, but did not:\n"
                + explainPlanText, explainPlanText.contains(expected));
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
