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

public class TestWindowedAggregatePlans extends PlannerTestCase {
    public void testRank() {
        AbstractPlanNode node = compile("SELECT A+B, MOD(A, B), B, RANK() OVER (PARTITION BY A,B ORDER BY B DESC ) AS ARANK FROM AAA;");
        // The plan should look like:
        // SendNode -> PartitionByPlanNode -> OrderByPlanNode -> SeqScanNode
        // We also do some santity checking on the PartitionPlan node.
        // First dissect the plan.
        assertTrue(node instanceof SendPlanNode);
        AbstractPlanNode sendNode = node;

        AbstractPlanNode projPlanNode = node.getChild(0);
        assertTrue(projPlanNode instanceof ProjectionPlanNode);

        AbstractPlanNode partitionByPlanNode = projPlanNode.getChild(0);
        assertTrue(partitionByPlanNode instanceof PartitionByPlanNode);

        AbstractPlanNode abstractOrderByNode = partitionByPlanNode.getChild(0);
        assertTrue(abstractOrderByNode instanceof OrderByPlanNode);
        OrderByPlanNode orderByNode = (OrderByPlanNode)abstractOrderByNode;
        // The PartitionByPlanNode's input schema will be the
        // same as the OrderByPlanNode's output schema.
        NodeSchema pb_input_schema = orderByNode.getOutputSchema();
        assertNotNull(pb_input_schema);
        // The order by plan node should just have "A" and "B".
        // The column "B" should be ordered descending.
        verifyOrderByPlanNode(orderByNode,
                              "A", SortDirectionType.ASC,
                              "B", SortDirectionType.DESC);

        AbstractPlanNode seqScanNode = orderByNode.getChild(0);
        assertTrue(seqScanNode instanceof SeqScanPlanNode);

        PartitionByPlanNode pbPlanNode = (PartitionByPlanNode)partitionByPlanNode;
        NodeSchema  pb_outputschema = pbPlanNode.getOutputSchema();
        assertNotNull(pb_outputschema);
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
        AbstractPlanNode node = compile("SELECT BBB.B, RANK() OVER (PARTITION BY A ORDER BY A ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;");
        // Dissect the plan.
        assertTrue(node instanceof SendPlanNode);
        AbstractPlanNode projectionPlanNode = node.getChild(0);
        assertTrue(projectionPlanNode instanceof ProjectionPlanNode);

        AbstractPlanNode partitionByPlanNode = projectionPlanNode.getChild(0);
        assertTrue(partitionByPlanNode instanceof PartitionByPlanNode);

        AbstractPlanNode orderByPlanNodeBase = partitionByPlanNode.getChild(0);
        assertTrue(orderByPlanNodeBase instanceof OrderByPlanNode);
        NodeSchema input_schema = orderByPlanNodeBase.getOutputSchema();
        assertNotNull(input_schema);
        OrderByPlanNode orderByPlanNode = (OrderByPlanNode)orderByPlanNodeBase;
        verifyOrderByPlanNode(orderByPlanNode,
                              "A", SortDirectionType.ASC);

        AbstractPlanNode scanNode = orderByPlanNodeBase.getChild(0);
        assertTrue(scanNode instanceof NestLoopPlanNode);

        NodeSchema  schema = partitionByPlanNode.getOutputSchema();
        SchemaColumn column = schema.getColumns().get(0);
        assertEquals("ARANK", column.getColumnAlias());
    }

    public void testRankFailures() {
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) FROM AAA GROUP BY A;",
                      "Use of both windowed operations and GROUP BY is not supported.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) AS R1, " +
                      "       RANK() OVER (PARTITION BY B ORDER BY A ) AS R2  " +
                      "FROM AAA;",
                      "At most one windowed display column is supported.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY A, B) FROM AAA;",
                      "Aggregate windowed expressions with range " +
                      "window frame units can have only one order by expression.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY CAST(A AS FLOAT)) FROM AAA;",
                      "Aggregate windowed expressions with RANGE " +
                      "window frame units can have only integer or TIMESTAMP value types.");

    }

    public void notestOrderByAndPartitionByExpressions() throws Exception {
        AbstractPlanNode node;
        try {
            node = compile("SELECT RANK() OVER (PARTITION BY A*A ORDER BY B) FROM AAA;");
        } catch (Exception ex) {
            assertFalse("PartitionBy expressions in windowed expressions don't compile", true);
        }
        try {
            node = compile("SELECT RANK() OVER (PARTITION BY A ORDER BY B*B) FROM AAA;");
        } catch (Exception ex) {
            assertFalse("OrderBy expressions in windowed expressions don't compile", true);
        }
    }

    // This is not actually a test.  This is here just to generate a
    // catalog and a plan for the PartitionByExecutor test.  It doesn't really
    // test anything at all.  So really, just don't enable it.
    public void testRankTestGen() throws Exception {
        AbstractPlanNode node;
        try {
            node = compile("select A, B, RANK() OVER ( PARTITION BY A ORDER BY B ) from AAA;");

            assertTrue(true);
        } catch (Exception ex) {
            assertTrue("Unexpected exception in compilation", false);
        }
    }
    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestWindowedAggregatePlans.class.getResource("testwindowingfunctions-ddl.sql"), "testwindowfunctions");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
