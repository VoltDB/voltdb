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
 */package org.voltdb.planner;

import org.voltdb.expressions.WindowedExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.PartitionByPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;

public class TestWindowingFunctions extends PlannerTestCase {
    public void testRank() {
        AbstractPlanNode node = compile("SELECT B, RANK() OVER (PARTITION BY A,B ORDER BY B ) AS ARANK FROM AAA;");
        // The plan should look like:
        // SendNode -> PartitionByPlanNode -> OrderByPlanNode -> SeqScanNode
        // We also do some santity checking on the PartitionPlan node.
        assertTrue(node instanceof SendPlanNode);
        AbstractPlanNode abstractPBPlanNode = node.getChild(0);
        assertTrue(abstractPBPlanNode instanceof PartitionByPlanNode);
        PartitionByPlanNode pbPlanNode = (PartitionByPlanNode)abstractPBPlanNode;
        NodeSchema  schema = pbPlanNode.getOutputSchema();
        assertEquals(2, schema.getColumns().size());
        SchemaColumn column = schema.getColumns().get(1);
        assertTrue(column.getExpression() instanceof WindowedExpression);
        assertEquals("ARANK", column.getColumnAlias());
        AbstractPlanNode OBNode = abstractPBPlanNode.getChild(0);
        assertTrue(OBNode instanceof OrderByPlanNode);
        AbstractPlanNode SScanNode = OBNode.getChild(0);
        assertTrue(SScanNode instanceof SeqScanPlanNode);
    }

    public void testRankWithSubqueries() {
        AbstractPlanNode node = compile("SELECT BBB.B, RANK() OVER (PARTITION BY A ORDER BY B ) AS ARANK FROM (select A, B, C from AAA where A < B) ALPHA, BBB WHERE ALPHA.C <> BBB.C;");
        assertTrue(node instanceof SendPlanNode);
        AbstractPlanNode abstractPBPlanNode = node.getChild(0);
        assertTrue(abstractPBPlanNode instanceof PartitionByPlanNode);
        PartitionByPlanNode pbPlanNode = (PartitionByPlanNode)abstractPBPlanNode;
        NodeSchema  schema = pbPlanNode.getOutputSchema();
        assertEquals(2, schema.getColumns().size());
        SchemaColumn column = schema.getColumns().get(1);
        assertTrue(column.getExpression() instanceof WindowedExpression);
        assertEquals("ARANK", column.getColumnAlias());
        AbstractPlanNode OBNode = abstractPBPlanNode.getChild(0);
        assertTrue(OBNode instanceof OrderByPlanNode);
        AbstractPlanNode abstractSScanNode = OBNode.getChild(0);
        assertTrue(abstractSScanNode instanceof NestLoopPlanNode);
    }

    public void testRankFailures() {
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) FROM AAA GROUP BY A;",
                      "Use of both windowed operations and GROUP BY is not supported.");
        failToCompile("SELECT RANK() OVER (PARTITION BY A ORDER BY B ) AS R1, " +
                      "       RANK() OVER (PARTITION BY B ORDER BY A ) AS R2  " +
                      "FROM AAA;",
                      "At most one windowed display column is supported.");

    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestWindowingFunctions.class.getResource("testwindowingfunctions-ddl.sql"), "testwindowfunctions");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
