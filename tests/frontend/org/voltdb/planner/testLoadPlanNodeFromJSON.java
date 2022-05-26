/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
        testLoadQueryPlanTree("select l.id from l where l.id = ? and exists (select a from t where l.a = t.a and l.a =t.b)");
        testLoadQueryPlanTree("select l.id from l where l.id = ? and exists (select a from t where exists(select 1 from t where t.a = l.id))");
        testLoadQueryPlanTree("select l.id from l where l.id = ? or exists (select a from t where l.a = t.a)");
        testLoadQueryPlanTree("select l.id from l join t on l.id=t.a and exists (select a from t where l.a = t.a)");
        testLoadQueryPlanTree("select 1 from l, t where exists (select 1 from t t1 where l.b = t1.a and t1.b = t.b)");
        testLoadQueryPlanTree("select 1 from l where exists (select count(*) from t offset 1)");
        testLoadQueryPlanTree("select a, sum(id) as sc1 from l where (a, id) in ( SELECT a, count(id) as sc2 from  l  GROUP BY a ORDER BY a DESC) GROUP BY a");
        testLoadQueryPlanTree("select a from l group by a having max(id) in (select b from t )");
        testLoadQueryPlanTree("select a from l group by a having max(id) in (select b from t )");
        testLoadQueryPlanTree("select a, (select b from t limit 1) b from l ");
        testLoadQueryPlanTree("select a FROM t where a = (SELECT a FROM l where a = ?)");
        testLoadQueryPlanTree("select a FROM t where (b,b) in (SELECT a, a FROM l where l.a = t.a)");

        // UNION
        testLoadQueryPlanTree("select l.id from l union all select a from t;");

    }

    public void testLoadQueryPlanTree(String sql) throws JSONException {
        AbstractPlanNode pn = compile(sql);
        PlanNodeTree pnt = new PlanNodeTree(pn);
        String str = pnt.toJSONString();
        JSONObject jsonPlan = new JSONObject(str);
        PlanNodeTree pnt1 = new PlanNodeTree();
        pnt1.loadFromJSONPlan(jsonPlan, getDatabase());
        String str1 = pnt1.toJSONString();
        assertTrue(str.equals(str1));
    }
}
