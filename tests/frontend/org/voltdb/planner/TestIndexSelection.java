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
import org.voltdb.plannodes.IndexScanPlanNode;

public class TestIndexSelection extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestIndexSelection.class.getResource("testplans-indexselection-ddl.sql"),
                    "testindexselectionplans",
                    planForSinglePartitionFalse);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    // This tests recognition of covering parameters to prefer a hash index that would use a
    // greater number of key components than a competing tree index.
    // Not sure how this relates to ENG-931?
    public void testEng931Plan()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ? and c = ? and d = ? " +
                                      "and e >= ? and e <= ?;");

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1_HASH\""));

        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
    }

    // This tests recognition of prefix parameters and constants to prefer an index that
    // would use a greater number of key components AND would give the desired ordering.
    public void testEng2541Plan() throws JSONException
    {
        AbstractPlanNode pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_B\""));

        if (pn != null) {
            JSONObject j = new JSONObject(pn.toJSONString());
            System.out.println(j.toString(2));
            System.out.println();
            System.out.println(pn.toExplainPlanString());
        }
    }

    // This tests recognition of a prefix parameter and upper bound to prefer an index that would
    // use a greater number of key components even though another index would give the desired ordering.
    public void testEng4792PlanWithCompoundEQLTEOrderedByPK() throws JSONException
    {
        AbstractPlanNode pn = compile("select id from a where deleted=? and updated_date <= ? order by id limit ?;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"DELETED_SINCE_IDX\""));

        if (pn != null) {
            JSONObject j = new JSONObject(pn.toJSONString());
            System.out.println(j.toString(2));
            System.out.println();
            System.out.println(pn.toExplainPlanString());
        }
    }
}
