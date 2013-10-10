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

    List <AbstractPlanNode> pns = new ArrayList<AbstractPlanNode>();

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
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
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
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
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
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(SortDirectionType.DESC, ispn.getSortDirection());
    }

    public void test0033()
    {
        AbstractPlanNode pn = compile("select a, b from t where a = ? and b = ? and c < ? order by c desc;");
        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("IDX_1_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.LT, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
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
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
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
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
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
        assertEquals(1, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(3, ExpressionUtil.uncombine(ispn.getInitialExpression()).size());
        assertEquals(SortDirectionType.DESC, ispn.getSortDirection());
    }

    public void testENG5297()
    {
        String sql = "SELECT * FROM data_reports " +
                "WHERE appID = 1486287933647372287 AND reportID = 1526804868369481731 " +
                "AND metricID = 1486287935375409155 AND field = 'accountID' " +
                "AND time >= '2013-09-29 00:00:00' AND time <= '2013-10-07 00:00:00' " +
                "ORDER BY time DESC LIMIT 150";

        AbstractPlanNode pn = compile(sql);
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());

        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertTrue(ispn.getTargetIndexName().contains("SYS_IDX_IDX_REPORTDATA_PK"));
        assertEquals(IndexLookupType.LTE, ispn.getLookupType());
        assertEquals(3, ispn.getSearchKeyExpressions().size());
        assertEquals(3, ExpressionUtil.uncombine(ispn.getEndExpression()).size());
        assertEquals(2, ExpressionUtil.uncombine(ispn.getPredicate()).size());
        assertEquals(3, ExpressionUtil.uncombine(ispn.getInitialExpression()).size());
        assertEquals(SortDirectionType.DESC, ispn.getSortDirection());
    }

}
