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
    /////////////////////////////////////////////////////////////////
    //
    // Ranges of variation.
    //   Partitioned Tables
    //   Group By Expressions.
    //     None (N)
    //     Partitioned (P) (Partition Column in GroupBy)
    //     NotPartioned (NP) (No Partition Column In Group By
    //                        or all tables are replicated)
    //   Compatible Order By Expressions.
    //   Group By Index.
    //   Joined Tables
    //     One table,
    //     Outer Join
    //     Outer Joins
    //   Distinct aggregate.
    //   Distinct select.
    //
    //   2^6 * 3 == 192 cases.
    //
    /////////////////////////////////////////////////////////////////

    public void testPlansReplicated() {
        planForLargeQueries(true);
        /////////////////////////////////////////////////////////////////
        //
        // Replicated tables and indexes.
        //
        /////////////////////////////////////////////////////////////////
        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  N,    N,   1    N    N
        // Nothing to help us here.  We have no indexes and no
        // order bys.
        validatePlan("select max(aa) from r1 group by aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,  // group by
                              PlanNodeType.ORDERBY,
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  N,    N,   1    N    N
        // We have an order by, but it doesn't help us, since
        validatePlan("select max(aa) from r1 group by id, aa order by aa * aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION, // project onto the display list.
                              PlanNodeType.ORDERBY,    // order by aa * aa
                              PlanNodeType.AGGREGATE,  // group by
                              PlanNodeType.ORDERBY,    // Order by for serial aggregation.
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  Y,    N,   1    N    N
        // This is like the previous, but with a compatible order by.
        // The order by node is for the order by expressions, but
        // the group by will use it.
        validatePlan("select max(aa) from r1 group by id, aa order by aa, id",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION, // project onto the display list.
                              PlanNodeType.AGGREGATE,  // group by.
                              PlanNodeType.ORDERBY,    // order by for group and order.
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  Y,    Y,   1    N    N
        validatePlan("select max(id) from r1_idpk group by id",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.AGGREGATE,
                                                      PlanNodeType.PROJECTION)));
        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  N,    N,   1    N    Y
        validatePlan("select distinct aa * aa from r1;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE, // distinct
                              PlanNodeType.ORDERBY,   // distinct
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  N,    N,   1    N    Y
        // Here the display columns contain all the group by
        // columns.  So the distinct is not necessary, and we
        // should get the same plan as above.  But this
        // time the orderby/aggregate pair is for the
        // group by.
        validatePlan("select distinct id * id, aa * aa from r1 group by aa * aa, id * id;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE, // group by grouping
                              PlanNodeType.ORDERBY,   // group by serial aggregation
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));

        // PT, GBE  COBE  GBI  JT   DA   DS
        // N,  NP,  N,    N,   1    N    Y
        // This has the same profile as above.  But there are
        // order by expressions which can help with the group
        // by.
        validatePlan("select distinct id * id, aa * aa from r1 "
                     + "group by aa * aa, id * id "
                     + "order by aa * aa, id * id;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.ORDERBY,   // order by
                              PlanNodeType.AGGREGATE, // group by grouping
                              PlanNodeType.ORDERBY,   // group by serial aggregation
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
        // GBE  COBE  GBI  PT   JT   DA   DS
        // R,   N,    N,   N,    1   N    Y
        validatePlan("select distinct aa * aa from r1 group by aa;",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.AGGREGATE, // distinct
                              PlanNodeType.ORDERBY,   // distinct ordering
                              PlanNodeType.PROJECTION,// Project onto the display columns.
                              PlanNodeType.AGGREGATE, // group by grouping
                              PlanNodeType.ORDERBY,   // group by order
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN, PlanNodeType.PROJECTION)));
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
