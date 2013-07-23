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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;

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

    List<AbstractPlanNode> pns = new ArrayList<AbstractPlanNode>();

    public void testGroupByA1() {
        pns = compileToFragments("SELECT A1 from T1 group by A1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountA1() {
        pns = compileToFragments("SELECT count(A1) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountStar()
    {
        pns = compileToFragments("SELECT count(*) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testCountDistinctA1() {
        pns = compileToFragments("SELECT count(distinct A1) from T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    public void testDistinctA1() {
        pns = compileToFragments("SELECT DISTINCT A1 FROM T1");
        for (AbstractPlanNode apn: pns) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    // Make it to false when we fix ENG-4397
    // ENG-4937 - As a developer, I want to ignore the "order by" clause on non-grouped aggregate queries.
    public void testEdgeComplexCase() {
        pns = compileToFragments("SELECT count(*)  FROM P1 order by PKEY");
        checkComplexAgg(pns, true);
    }

    public void testSimpleComplexCase() {
        pns = compileToFragments("SELECT A1, sum(A1), sum(A1)+11 FROM P1 GROUP BY A1");
        checkComplexAgg(pns, true);

        pns = compileToFragments("SELECT A1, SUM(PKEY) as A2, (SUM(PKEY) / 888) as A3, (SUM(PKEY) + 1) as A4 FROM P1 GROUP BY A1");
        checkComplexAgg(pns, true);

        pns = compileToFragments("SELECT A1, count(*) as tag FROM P1 group by A1 order by tag, A1 limit 1");
        checkComplexAgg(pns, false);

    }

    public void testReplicatedTableComplexAggregate() {
          pns = compileToFragments("select PKEY+A1 from T1 Order by PKEY+A1");
          checkComplexAgg(pns, false);
    }

    private void checkComplexAgg(List<AbstractPlanNode> pns, boolean hasProjectionNode) {
        assertTrue(pns.size() > 0);
        boolean isDistributed = pns.size() > 1 ? true: false;

        for ( AbstractPlanNode nd : pns) {
            System.out.println("PlanNode Explan string:\n" + nd.toExplainPlanString());
        }
        AbstractPlanNode p = pns.get(0).getChild(0);
        if (hasProjectionNode)
            assertTrue(p instanceof ProjectionPlanNode);

        if (isDistributed) {
            p = pns.get(1).getChild(0);
            assertFalse(p instanceof ProjectionPlanNode);
        }
    }
}
