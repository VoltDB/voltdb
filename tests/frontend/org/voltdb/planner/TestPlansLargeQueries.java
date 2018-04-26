/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltdb.types.PlanNodeType;

public class TestPlansLargeQueries extends PlannerTestCase {
    public void testPlansSerialAggregates() {
        planForLargeQueries(true);
        // Ranges of variation.
        // -- Replicated or Partitioned Tables.
        //     NRMP = N replicated, M partitions
        //            N, M \in {0, 1}
        // -- Single or Joined Tables.
        //     Implicit in NRMP.
        //     Joined iff N+M > 1.
        // -- Indexable expressions.
        //     IN -- No indexable expressions.
        //     IO -- Indexable Order By, not Group By,
        //     IG -- Indexable Group By, not Order By,
        //     IOG -- Indexable Group By and Order By.
        // -- Number of aggregates
        //     NA - N aggregate functions, N \in {0, 1}
        // -- Number of group bys: 0, 1, + = many.
        //     NXG - N Group By Expressions: 0, 1, + = many.
        //           X \in {P, R} for Partitioned or Replicated.
        // -- Orderby Expressions
        //     NO - N Order By Expressions: 0, 1, + = many.
        // -- Select Distinct.
        //     D for select distinct,
        //     ND for select [not distinct].
        // Total range of variation:
        //   4   // Num replicated, num partitioned
        //   * 4 // Indexable
        //   * 2 // Number Agg Functions.
        //   * 3 // Number Group Bys.
        //   * 3 // Number Order Bys.
        //   * 2 // Distinct or not.
        //   == 576 cases.
        //
        //  select sum(id) from R1;
        //  1R0P-IN-1A-0RG-0O-0W-ND
        //       1 Replicated table (unjoined), 0 Partitioned tables,
        //       Not Indexable GB expressions.
        //       Not Indexable OB expressions.
        //       0 partitioned group by
        //       0 partitioned order by
        //       Not Distinct
        //  select distinct sum(R1.id), sum(P1.id) from R1, P1 group by P1.id order by P1.id
        //  1R1P-NIOB-NIGB-1PGB-1P0B-D
        //       1 Replicated Table, 1 Partitioned Table, Joined.
        //       1 Partitioned group by,
        //       1 Partitioned order by,
        //       1 Window Function,
        //       Select distinct.
        //
        // One table, no indexes, no group bys, no order bys.
        validatePlan("select max(aa) from r1;",
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));
        // One table, no indexes, one group by expression, no order by.
        validatePlan("select max(aa) from r1 group by aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));

        // One table, no indexes, one group by expression, one order by expression, GB and OB are compatible.
        validatePlan("select max(aa) from r1 group by aa, id order by aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
        // One Table, no index, two group by expressions, order bys which equal the group by.
        validatePlan("select count(*) from r1_idpk group by aa, id order by id, aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));

        // One table, primary key index, no group by or order by.
        // We don't scan the primary key index because
        // we are going to aggregate anyway, so it doesn't
        // help in any way.
        validatePlan("select max(aa) from r1_idpk;",
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));
        // One table, primary key index, one non-index group by expression, no order by.
        // We scan the PK index for determinism, so the groups are in a
        // predictable order.
        validatePlan("select max(aa) from r1_idpk group by aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));
        // One Table, primary key index, one index group by expression, no order by.
        // The group by expression can optimize the group scan, so
        // we will do serial aggregation.  This is the same as with
        // small temp tables.
        validatePlan("select max(id) from r1_idpk group by id",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION,
                                                      PlanNodeType.AGGREGATE)));
        // One Table, Primary Key index, two group by expressions, one
        // indexed and one not, one non-indexed order by.
        validatePlan("select count(*) from r1_idpk group by aa, id order by aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));

        // One Table, Primary Key index, two group by expressions, one
        // indexed and one not, order bys which equal the group by.
        validatePlan("select count(*) from r1_idpk group by aa, id order by id, aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));

        // One Table, Primary Key index, two group by expressions, one
        // indexed and one not, order bys which equal the group by.
        validatePlan("select count(*) from r1_allidx group by aa, id order by id, aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));

    }


    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansLargeQueries.class.getResource("testplans-adhoc-large.sql"),
                    "testadhoclarge",
                    true /* Plan for single partitioning. */);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

}
