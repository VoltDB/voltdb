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

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

public class TestIndexSelection extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestIndexSelection.class.getResource("testplans-indexselection-ddl.sql"),
                    "testindexselectionplans",
                    planForSinglePartitionFalse);
    }

    // This tests recognition of a complex expression value
    // -- an addition -- used as an indexable join key's search key value.
    // Some time ago, this would throw a casting error in the planner.
    public void testEng3850ComplexIndexablePlan()
    {
        AbstractPlanNode pn = compile("select id from a, t where a.id < (t.a + ?);");
        pn = pn.getChild(0);
        pn = pn.getChild(0);
//        System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.LT, indexScan.getLookupType());
        assertTrue(indexScan.toJSONString().contains("\"TARGET_INDEX_NAME\":\"" +
                HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "ID"));
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
//        System.out.println("DEBUG: " + pn.toJSONString());
        assertTrue(pn.toJSONString().contains("\"TARGET_TABLE_NAME\":\"T\""));
    }

    // This tests recognition of covering parameters to prefer a hash index that would use a
    // greater number of key components than a competing tree index.
    // Not sure how this relates to ENG-931?
    public void testEng931Plan()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ? and c = ? and d = ? " +
                                      "and e >= ? and e <= ?;");
        pn = pn.getChild(0);
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1_HASH\""));
    }

    // This tests recognition of prefix parameters and constants to prefer an index that
    // would use a greater number of key components AND would give the desired ordering.
    public void testEng2541Plan() throws JSONException
    {
        AbstractPlanNode pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;");
        pn = pn.getChild(0);
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_B\""));
    }

    // This tests recognition of a prefix parameter and upper bound to prefer an index that would
    // use a greater number of key components even though another index would give the desired ordering.
    public void testEng4792PlanWithCompoundEQLTEOrderedByPK() throws JSONException
    {
        AbstractPlanNode pn = compile("select id from a where deleted=? and updated_date <= ? order by id limit ?;");
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        pn = pn.getChild(0);
        // ENG-5066: now Limit is pushed under Projection
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"DELETED_SINCE_IDX\""));
    }

    public void testFixedPlanWithExpressionIndexAndAlias()
    {
        AbstractPlanNode pn;
        IndexScanPlanNode ispn;
        String json;
        pn = compile(
                "select * from l aliased where  b = ? and DECODE(a, null, 0, a) = 0 and id = ?;");
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l aliased, l where l.b = ? and DECODE(l.a, null, 0, l.a) = 0 and l.id = ? and l.lname = aliased.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0).getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0).getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"" +
                HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "PK_LOG"));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(l.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname;");
        System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and l.id = ? and l.lname = x.lname;");
        System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname;");
        System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"" +
                HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "PK_LOG"));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());
    }

    public void testCaseWhenIndex()
    {
        AbstractPlanNode pn = compile("select * from l where CASE WHEN a > b THEN a ELSE b END > 8;");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("CASEWHEN_IDX1"));


        pn = compile("select * from l WHERE CASE WHEN a < 10 THEN a*5 ELSE a + 5 END > 2");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("CASEWHEN_IDX2"));

        // Negative case
        pn = compile("select * from l WHERE CASE WHEN a < 10 THEN a*2 ELSE a + 5 END > 2");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("using its primary key index (for deterministic order only)"));
    }
}
