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

import org.voltdb.types.PlanNodeType;

public class TestPlansInsertIntoSelect extends PlannerTestCase {
    public void testInlineInsertReplicatedToReplicated() {
        validatePlan("INSERT INTO T1 SELECT * from T2;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              planWithInlineNodes(PlanNodeType.SEQSCAN,
                                                  PlanNodeType.PROJECTION,
                                                  PlanNodeType.INSERT)));
        validatePlan("INSERT INTO T2 SELECT * from T1;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              planWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                  PlanNodeType.PROJECTION,
                                                  PlanNodeType.INSERT)));
    }

    // Note that P1 and P2 have the same partition columns
    // here, so they can be copied within a single site.  If
    // they had different partition columns we couldn't plan the
    // query.
    public void testInlineInsertPartitionedToPartitioned() {
        validatePlan("INSERT INTO P1 SELECT * from P2;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              planWithInlineNodes
                              (PlanNodeType.SEQSCAN,
                               PlanNodeType.INSERT,
                               PlanNodeType.PROJECTION)));
        validatePlan("INSERT INTO P2 SELECT * from P1;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              planWithInlineNodes
                              (PlanNodeType.INDEXSCAN,
                               PlanNodeType.INSERT,
                               PlanNodeType.PROJECTION)));
    }

    public void testInlineInsertReplicatedToPartitioned() {
        validatePlan("INSERT INTO P1 SELECT * from T1;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              planWithInlineNodes
                              (PlanNodeType.INDEXSCAN,
                               PlanNodeType.PROJECTION,
                               PlanNodeType.INSERT)));
        validatePlan("INSERT INTO P1 SELECT * from T2;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              planWithInlineNodes
                              (PlanNodeType.SEQSCAN,
                               PlanNodeType.PROJECTION,
                               PlanNodeType.INSERT)));
    }

    public void testNoInlineInsert() {
        // No inline insert for UPSERT.
        validatePlan("UPSERT INTO T1 SELECT ID, AAA, BBB FROM T1 ORDER BY ID;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.INDEXSCAN));
        validatePlan("UPSERT INTO T1 SELECT * FROM T2 ORDER BY ID, AAA, BBB;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.ORDERBY,
                              PlanNodeType.SEQSCAN));
        validatePlan("INSERT INTO T1 SELECT L.ID, L.AAA, R.BBB from T1 L JOIN T1 R ON L.ID = R.ID;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.PROJECTION,
                              planWithInlineNodes(PlanNodeType.NESTLOOPINDEX, PlanNodeType.INDEXSCAN),
                              planWithInlineNodes(PlanNodeType.INDEXSCAN, PlanNodeType.PROJECTION)));
        validatePlan("INSERT INTO T1 SELECT ID, AAA, AAA+ID from T1 group by ID, AAA;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.PROJECTION,
                              planWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                  PlanNodeType.PARTIALAGGREGATE,
                                                  PlanNodeType.PROJECTION)));

        // Recursive insert, sequential scan, cannot inline (ENG-13036).
        validatePlan("INSERT INTO T2 SELECT * from T2;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.SEQSCAN));
        validatePlan("INSERT INTO P2 SELECT * from P2;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.SEQSCAN));

        // Recursive insert, index scan, cannot inline (ENG-13036).
        validatePlan("INSERT INTO T1 SELECT * from T1;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.LIMIT,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.INDEXSCAN));
        validatePlan("INSERT INTO P1 SELECT * from P1;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.INSERT,
                              PlanNodeType.INDEXSCAN));
    }

    //
    // This is not allowed, no surprise: INSERT INTO T1 SELECT * from P2;
    //
    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestPlansInsertIntoSelect.class.getResource("testplans-insertintoselect-ddl.sql"), "testinsertintoselect");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
