/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.IndexLookupType;

public class TestCoveringIndexPlans extends TestCase {

    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount,
                                     boolean singlePartition)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn =  aide.compile(sql, paramCount, singlePartition);
        }
        catch (NullPointerException ex) {
            // aide may throw NPE if no plangraph was created
            ex.printStackTrace();
            fail();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestCoveringIndexPlans.class.getResource("testplans-indexvshash-ddl.sql"),
                                         "testindexvshashplans");

        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
        System.out.println(aide.getCatalog().serialize());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }

    // The planner should choose cover2_tree, which is over columns a and b.
    // It should use this index as a covering index, so it should be a
    // greater-than lookup type
    public void testEng1023()
    {
        AbstractPlanNode pn = null;
        pn =
            compile("select a from t where a = ? and b < ?;", 2, true);
        assertTrue(pn != null);

        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER2_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.GT, ispn.getLookupType());
        assertEquals(1, ispn.getSearchKeyExpressions().size());
    }

    public void testCover2ColumnsWithEquality()
    {
        AbstractPlanNode pn = null;
        pn =
            compile("select a from t where a = ? and b = ?;", 2, true);
        assertTrue(pn != null);

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
        AbstractPlanNode pn = null;
        pn =
            compile("select a from t where a = ? and c = ? and b < ?;", 3, true);
        assertTrue(pn != null);

        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.GT, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
    }

    public void testCover3ColumnsInOrderWithLessThanAndOrderBy()
    {
        AbstractPlanNode pn = null;
        pn =
            compile("select a, b from t where a = ? and c = ? and b < ? order by b;", 3, true);
        assertTrue(pn != null);

        pn = pn.getChild(0).getChild(0).getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode)pn;
        assertEquals("COVER3_TREE", ispn.getTargetIndexName());
        assertEquals(IndexLookupType.GT, ispn.getLookupType());
        assertEquals(2, ispn.getSearchKeyExpressions().size());
    }

    public void testCover3ColumnsOutOfOrderWithLessThan()
    {
        AbstractPlanNode pn = null;
        pn =
            compile("select a from t where a = ? and b = ? and c < ?;", 3, true);
        assertTrue(pn != null);

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

    public void testSingleColumnLessThan()
    {
        AbstractPlanNode pn = null;
        pn =
            compile("select a from t where a < ?;", 1, true);
        assertTrue(pn != null);

        pn = pn.getChild(0);
        if (pn != null) {
            System.out.println(pn.toJSONString());
        }
        assertTrue(pn instanceof SeqScanPlanNode);
    }
}
