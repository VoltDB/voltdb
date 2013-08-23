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

import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.SortDirectionType;

public class TestReverseIndexScan extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartition = true;
        setupSchema(TestReverseIndexScan.class.getResource("testplans-indexvshash-ddl.sql"),
                    "testindexvshashplans", planForSinglePartition);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void test001()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b < ?");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER2_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LT, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
        assertEquals(1, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        // SortDirection is still INVALID because in EE, we use LookupType to determine
        // index scan direction
        assertEquals(SortDirectionType.INVALID, ispn.getSortDirection());
    }

    public void test002()
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
        assertEquals(2, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        // SortDirection is still INVALID because in EE, we use LookupType to determine
        // index scan direction
        assertEquals(SortDirectionType.INVALID, ispn.getSortDirection());
    }

    // ORDER BY: do not do reverse scan optimization
    public void test0031()
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
        assertEquals(3, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(SortDirectionType.ASC, ispn.getSortDirection());
    }

    // no ORDER BY node because order-by is handled by index scan inherent ordering
    public void test0032()
    {
        AbstractPlanNode pn = compile("select a, b from t where a = ? and c = ? and b < ? order by b desc;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LT, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(SortDirectionType.DESC, ispn.getSortDirection());
    }

    public void test004()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b <= ?");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER2_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LTE, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
        assertEquals(1, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getInitialExpression()).size());
        // SortDirection is still INVALID because in EE, we use LookupType to determine
        // index scan direction
        assertEquals(SortDirectionType.INVALID, ispn.getSortDirection());
    }

    public void test005()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and c = ? and b <= ?;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LTE, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(3, ExpressionUtil.uncombine(ispn.getInitialExpression()).size());
        // SortDirection is still INVALID because in EE, we use LookupType to determine
        // index scan direction
        assertEquals(SortDirectionType.INVALID, ispn.getSortDirection());
    }

    // ORDER BY: do not do reverse scan optimization
    public void test0061()
    {
        AbstractPlanNode pn = compile("select a, b from t where a = ? and c = ? and b <= ? order by b;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.GTE, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
        assertEquals(3, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(SortDirectionType.ASC, ispn.getSortDirection());
    }

    // no ORDER BY node because order-by is handled by index scan inherent ordering
    public void test0062()
    {
        AbstractPlanNode pn = compile("select a, b from t where a = ? and c = ? and b <= ? order by b desc;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LTE, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(3, ExpressionUtil.uncombine(ispn.getInitialExpression()).size());
        assertEquals(SortDirectionType.DESC, ispn.getSortDirection());
    }


}
