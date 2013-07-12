/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltdb.plannodes.AbstractPlanNode;

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

/*
    public void testGroupByA1() {
        AbstractPlanNode pn = compile("SELECT A1 from T1 group by A1");
        System.out.println(pn.toJSONString());
    }

    public void testCountA1() {
        AbstractPlanNode pn = compile("SELECT count(A1) from T1");
        System.out.println(pn.toJSONString());
    }

    public void testCountStar()
    {
        AbstractPlanNode pn = compile("SELECT count(*) from T1");
        System.out.println(pn.toJSONString());
    }

   public void testCountDistinctA1() {
       AbstractPlanNode pn = compile("SELECT count(distinct A1) from T1");
       System.out.println(pn.toJSONString());
   }

    public void testDistinctA1() {
        AbstractPlanNode pn = compile("SELECT DISTINCT A1 FROM T1");
        System.out.println(pn.toJSONString());
    }

*/

//    public void testReplicatedTableComplexAggregate1() {
//        AbstractPlanNode pn = compile("SELECT A1, SUM(PKEY) FROM R1 GROUP BY A1");
//        System.out.println(pn.toJSONString());
//    }
//
//    public void testPartitionedTableComplexAggregate1() {
//        AbstractPlanNode pn = compile("SELECT A1, SUM(PKEY) FROM T1 GROUP BY A1");
//        System.out.println(pn.toJSONString());
//    }

    public void testReplicatedTableComplexAggregate() {
        AbstractPlanNode pn = compile("SELECT A1, SUM(PKEY) as A2, (SUM(PKEY) / 888) as A3, (SUM(PKEY) + 1) as A4 FROM R1 GROUP BY A1");
        System.out.println(pn.toExplainPlanString());
    }
//
//    public void testPartitionedTableComplexAggregate() {
//        AbstractPlanNode pn = compile("SELECT A1, SUM(PKEY) / 3 FROM T1 GROUP BY A1");
//        System.out.println(pn.toJSONString());
//    }

}
