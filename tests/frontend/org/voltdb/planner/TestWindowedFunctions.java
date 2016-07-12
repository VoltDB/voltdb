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

import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.PartitionByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.SortDirectionType;

public class TestWindowedFunctions extends PlannerTestCase {
    public void testRank() {
        // Save the guard and restore it after.
        boolean savedGuard = PlanAssembler.HANDLE_WINDOWED_OPERATORS;
        PlanAssembler.HANDLE_WINDOWED_OPERATORS = true;
        try {
            String windowedQuery;
            windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK FROM AAA;";
            validateWindowedFunctionPlan(windowedQuery, 3, 2);

            // Altering the position of the rank column does not radically
            // change the plan structure.
            windowedQuery = "SELECT RANK() OVER (PARTITION BY A, C ORDER BY B DESC) AS ARANK, A+B, MOD(A, B), B FROM AAA;";
            validateWindowedFunctionPlan(windowedQuery, 3, 2);

            // Try some strange edge case that trivially order by a partition
            // by column, so they should trivially result in a rank of 1 for
            // each partition.

            windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY A, B ORDER BY B DESC) AS ARANK FROM AAA;";
            validateWindowedFunctionPlan(windowedQuery, 2, 1);

            // The order in which the PARTITION BY keys are listed should not
            // radically change the plan structure.
            windowedQuery = "SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY B, A ORDER BY B DESC ) AS ARANK FROM AAA;";
            validateWindowedFunctionPlan(windowedQuery, 2, 0);
        } finally {
            PlanAssembler.HANDLE_WINDOWED_OPERATORS = savedGuard;
        }
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
    private void validateWindowedFunctionPlan(String windowedQuery, int nSorts, int descSortIndex) {
        AbstractPlanNode node = compile(windowedQuery);
        // The plan should look like:
        // SendNode -> PartitionByPlanNode -> OrderByPlanNode -> SeqScanNode
        // We also do some sanity checking on the PartitionPlan node.
        // First dissect the plan.
        assertTrue(node instanceof SendPlanNode);

        AbstractPlanNode projPlanNode = node.getChild(0);
        assertTrue(projPlanNode instanceof ProjectionPlanNode);

        AbstractPlanNode partitionByPlanNode = projPlanNode.getChild(0);
        assertTrue(partitionByPlanNode instanceof PartitionByPlanNode);

        AbstractPlanNode abstractOrderByNode = partitionByPlanNode.getChild(0);
        assertTrue(abstractOrderByNode instanceof OrderByPlanNode);
        OrderByPlanNode orderByNode = (OrderByPlanNode)abstractOrderByNode;
        NodeSchema input_schema = orderByNode.getOutputSchema();
        assertNotNull(input_schema);

        AbstractPlanNode seqScanNode = orderByNode.getChild(0);
        assertTrue(seqScanNode instanceof SeqScanPlanNode);

        PartitionByPlanNode pbPlanNode = (PartitionByPlanNode)partitionByPlanNode;
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
        assertTrue(column.getExpression() instanceof WindowedExpression);
        assertEquals("ARANK", column.getColumnAlias());
        assertEquals(2, pbPlanNode.getNumberOfPartitionByExpressions());
        validateTVEs(input_schema, pbPlanNode);
    }

    public void validateTVEs(NodeSchema input_schema, PartitionByPlanNode pbPlanNode) {
        List<AbstractExpression> tves = pbPlanNode.getAllTVEs();
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
            assertEquals(msg, col.getColumnAlias(), tve.getColumnAlias());
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
        // Save the guard and restore it after.
        boolean savedGuard = PlanAssembler.HANDLE_WINDOWED_OPERATORS;
        PlanAssembler.HANDLE_WINDOWED_OPERATORS = true;
        try {
            String windowedQuery;
            // The following variants exercise resolving columns to subquery result columns.
            // At one point in development, this would only work by disabling ALPHA.A as a possible resolution.
            // It got a mysterious "Mismatched columns A in subquery" error.

            windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY A ORDER BY BBB.B ) AS ARANK FROM (select A AS NOT_A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
            validateQueryWithSubquery(windowedQuery);

            windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY RENAMED_A ORDER BY BBB.B ) AS ARANK FROM (select A AS RENAMED_A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
            validateQueryWithSubquery(windowedQuery);

            windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY BBB.A ORDER BY BBB.B ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
            validateQueryWithSubquery(windowedQuery);

            windowedQuery = "SELECT BBB.B, RANK() OVER (PARTITION BY ALPHA.A ORDER BY BBB.B ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;";
            validateQueryWithSubquery(windowedQuery);
        } finally {
            PlanAssembler.HANDLE_WINDOWED_OPERATORS = savedGuard;
        }
    }

    /**
     * Validate that each similar windowed query in testRankWithSubqueries
     * produces a similar plan
     * @param windowedQuery a variant of a test query of a known basic format
     **/
    private void validateQueryWithSubquery(String windowedQuery) {
        AbstractPlanNode node = compile(windowedQuery);
        // Dissect the plan.
        assertTrue(node instanceof SendPlanNode);
        AbstractPlanNode projectionPlanNode = node.getChild(0);
        assertTrue(projectionPlanNode instanceof ProjectionPlanNode);

        AbstractPlanNode partitionByPlanNode = projectionPlanNode.getChild(0);
        assertTrue(partitionByPlanNode instanceof PartitionByPlanNode);

        AbstractPlanNode orderByPlanNode = partitionByPlanNode.getChild(0);
        assertTrue(orderByPlanNode instanceof OrderByPlanNode);
        NodeSchema input_schema = orderByPlanNode.getOutputSchema();

        AbstractPlanNode scanNode = orderByPlanNode.getChild(0);
        assertTrue(scanNode instanceof NestLoopPlanNode);

        NodeSchema  schema = partitionByPlanNode.getOutputSchema();
        SchemaColumn column = schema.getColumns().get(0);
        assertTrue(column.getExpression() instanceof WindowedExpression);
        assertEquals("ARANK", column.getColumnAlias());

        validateTVEs(input_schema, (PartitionByPlanNode)partitionByPlanNode);
    }

    public void testRankFailures() {
        boolean savedGuard = PlanAssembler.HANDLE_WINDOWED_OPERATORS;
        PlanAssembler.HANDLE_WINDOWED_OPERATORS = true;
        try {
            failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) FROM AAA GROUP BY A;",
                          "Use of both windowed operations and GROUP BY is not supported.");
            failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) AS R1, " +
                          "       RANK() OVER (PARTITION BY B ORDER BY A ) AS R2  " +
                          "FROM AAA;",
                          "At most one windowed display column is supported.");
            // Detect that PARTITION BY A is ambiguous when A names multiple columns.
            // Queries like this passed at one point in development, ignoring the subquery
            // result column as a possible binding for A.
            failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY A, B) AS ARANK " +
                          "FROM (select A, B, C from AAA where A < B) ALPHA, BBB " +
                          "WHERE ALPHA.C <> BBB.C;",
                          "Column \"A\" is ambiguous.  It\'s in tables: ALPHA, BBB");
        } finally {
            PlanAssembler.HANDLE_WINDOWED_OPERATORS = savedGuard;
        }

    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestWindowedFunctions.class.getResource("testwindowingfunctions-ddl.sql"), "testwindowfunctions");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
