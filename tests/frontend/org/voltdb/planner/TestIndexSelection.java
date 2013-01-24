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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.plannodes.AbstractPlanNode;

public class TestIndexSelection extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        //TODO: consider switching to true for single-partition planning or replication to simplify resulting plans
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestIndexSelection.class.getResource("testplans-indexselection-ddl.sql"), "testindexselectionplans",
                    planForSinglePartitionFalse);
        forceHackPartitioning();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*public void testEng931Plan()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ? and c = ? and d = ? and e >= ? and e <= ?;");

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1\""));

        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
    }*/

    public void testEng2541Plan() throws JSONException
    {
        AbstractPlanNode pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;");
        pn = pn.getChild(0);
        //TODO: This test wants its teeth back. As is, it doesn't really prove much.
        //assertTrue(pn instanceof IndexScanPlanNode);
        //assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1\""));

        if (pn != null) {
            JSONObject j = new JSONObject(pn.toJSONString());
            System.out.println(j.toString(2));
            System.out.println();
            System.out.println(pn.toExplainPlanString());
        }
    }
}

