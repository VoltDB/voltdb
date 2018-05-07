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
        /////////////////////////////////////////////////////////////////
        //
        // Replicated tables and indexes.
        //
        /////////////////////////////////////////////////////////////////
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
        ////////////////////////////////////////////////////////////////////////////////
        //
        // Partitioned tables.
        //
        ////////////////////////////////////////////////////////////////////////////////
        //
        // Plain, simple partitioned query.
        //
        validatePlan("select sum(aa) from p1 group by aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));

        // Plain, simple partitioned query with order by.  We don't need an
        // order by plan node because the index scan does the work for us.
        validatePlan("select sum(id) as ss from p1 group by id order by id",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.MERGERECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));
        // group by and order by on a partitioned table.  There
        // is no index to scan, and we need two order by nodes,
        // one for serial aggregation and one for ordering by the
        // sums.
        validatePlan("select sum(aa) as ss from p1 group by aa order by ss",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.ORDERBY,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                      PlanNodeType.PROJECTION)));
        // Group by and order by on a partitioned table.
        // The group by and order by keys are the same, but
        // are in different order.  The order by node for
        // serial aggregation gives the order for the order
        // by list.
        validatePlan("select sum(aa) as ss from p1 group by aa, id order by id, aa",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.ORDERBY,
                              PlanNodeType.RECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));
        // Select distinct with a group by but no order by.  We expect
        // the index scan to give an order for the serial aggregation,
        // and the mergereceive node to preserve this order.
        validatePlan("select distinct sum(id) as ss from p1 group by id",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.MERGERECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
                                                      PlanNodeType.PROJECTION)));
        // Select distinct with a group by and order by the group key.  We expect
        // the index scan to give an order for the serial aggregation,
        // the mergereceive node to preserve this order and the order by
        // to use the same order.  Note that some of the aggregate
        // computation is pushed down to the distributed fragment, but
        // there is a vestigial combiner in the coordinator fragment.
        validatePlan("select distinct sum(id) as ss from p1 group by id",
                     fragSpec(PlanNodeType.SEND,
                              PlanNodeType.PROJECTION,
                              PlanNodeType.AGGREGATE,
                              PlanNodeType.MERGERECEIVE),
                     fragSpec(PlanNodeType.SEND,
                              new PlanWithInlineNodes(PlanNodeType.INDEXSCAN,
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
