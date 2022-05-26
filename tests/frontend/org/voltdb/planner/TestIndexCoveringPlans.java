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

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.IndexLookupType;

public class TestIndexCoveringPlans extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartition = true;
        setupSchema(TestIndexCoveringPlans.class.getResource("testplans-indexvshash-ddl.sql"),
                    "testindexvshashplans", planForSinglePartition);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    // The planner should choose cover2_tree, which is over columns a and b.
    // It should use this index as a covering index, so it should be a
    // greater-than lookup type
    public void testEng1023()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b < ?;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER2_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LT, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
    }

    public void testCover2ColumnsWithEquality()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ?;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER2_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.EQ, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
    }

    public void testCover3ColumnsInOrderWithLessThan()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and c = ? and b < ?;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LT, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
    }

    public void testCover3ColumnsWithLessThanAndOrderBy()
    {
        AbstractPlanNode pn = compile("select a, b from t where a = ? and c = ? and b < ? order by b;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
            System.out.println(pn.toExplainPlanString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.GTE, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
    }

    public void testCover3ColumnsInOrderWithLessThanAndOrderBy()
    {
        AbstractPlanNode pn = compile("select a, b from t where a = ? and c = ? and b < ? order by b;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.GTE, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
    }

    public void testCover3ColumnsOutOfOrderWithLessThan()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ? and c < ?;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("IDX_1_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LT, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
    }

    public void testSingleColumnLessThan()
    {
        AbstractPlanNode pn = compile("select a from t where a < ?;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
    }

}
