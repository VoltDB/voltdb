/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;

public class testLoadPlanNodeFromJSON extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestIndexSelection.class.getResource("testplans-indexselection-ddl.sql"), "testindexselectionplans",
                                                         true);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testLoadQueryPlans() throws JSONException {
        testLoadQueryPlanTree("select count(*) from l,t where lname=? and l.a=t.a order by l.b limit ?;");
        testLoadQueryPlanTree("select * from l,t where lname=? and l.a=t.a order by l.b limit ?;");
        testLoadQueryPlanTree("select l.id, count(*) as tag from l group by l.id order by tag, l.id limit ?;");
        testLoadQueryPlanTree("select count(*) from l where lname=? and id < ?;");
    }

    public void testLoadQueryPlanTree(String sql) throws JSONException {
        AbstractPlanNode pn = compile(sql);
        PlanNodeTree pnt = new PlanNodeTree(pn);
        String str = pnt.toJSONString();
        System.out.println(str);
        JSONArray jarray = new JSONObject(str)
                .getJSONArray(PlanNodeTree.Members.PLAN_NODES.name());
        PlanNodeTree pnt1 = new PlanNodeTree();
        pnt1.loadFromJSONArray(jarray, getDatabase());
        String str1 = pnt1.toJSONString();
        assertTrue(str.equals(str1));
    }
}
