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

import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;

public class TestPlansMatView extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception
    {
        setupSchema(TestPlansMatView.class.getResource("testplans-matview-ddl.sql"),
                    "testplansmatview", false);
    }

    public void testPartitionedMatView()
    {
        List<AbstractPlanNode> pns = compileToFragments("SELECT V_D1 FROM VP WHERE V_PARTKEY = 1;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(1, pns.size());
    }

    public void testFixedCasePartitionedMatView()
    {
        List<AbstractPlanNode> pns = compileToFragments("SELECT V_D1 FROM VPF WHERE V_PARTKEY = 1;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(1, pns.size());
    }

    public void testFixedJoinCasePartitionedMatView()
    {
        List<AbstractPlanNode> pns =
            compileToFragments("SELECT CNT, VAL1 FROM VPF, P " +
                               "WHERE PARTKEY = ? AND V_PARTKEY = PARTKEY;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(1, pns.size());
    }

    public void testFixedJoinCaseMatView()
    {
        List<AbstractPlanNode> pns =
            compileToFragments("SELECT CNT, VAL1 FROM VPF, P WHERE V_PARTKEY = PARTKEY;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(2, pns.size());
    }

    public void testMultipartitionedQueryOnMatView()
    {
        List<AbstractPlanNode> pns = compileToFragments("SELECT V_D1 FROM VP WHERE V_D2 = 1;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(2, pns.size());
        System.out.println(pns.get(1).toExplainPlanString());
    }

    public void testMultipartitionedMatView()
    {
        List<AbstractPlanNode> pns = compileToFragments("SELECT V_D1 FROM VNP WHERE V_D2 = 1;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(2, pns.size());
        System.out.println(pns.get(1).toExplainPlanString());
    }

    public void testReplicatedMatView()
    {
        List<AbstractPlanNode> pns = compileToFragments("SELECT V_D1 FROM VR WHERE V_D2 = 1;");
        System.out.println(pns.get(0).toExplainPlanString());
        assertEquals(1, pns.size());
    }

}
