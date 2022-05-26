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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class TestReplaceWithIndexLimit extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-indexlimit-ddl.sql"),
                    "testindexlimit", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void checkIndexLimit(List<AbstractPlanNode> pn, boolean replaced, String[] indexNames) {
        assertTrue (pn.size() > 0);

        for ( AbstractPlanNode nd : pn) {
            System.out.println("PlanNode Explain string:\n" + nd.toExplainPlanString());
        }

        // Navigate to the leaf node of the last plan fragment.
        AbstractPlanNode p = pn.get(pn.size() - 1).getChild(0);
        while (p.getChildCount() > 0) {
            p = p.getChild(0);
        }

        if (replaced) {
            Set<String> indexSet = new HashSet<String>();
            for (String index : indexNames){
                indexSet.add(index);
            }
            assertTrue (p instanceof IndexScanPlanNode);
            assertTrue (p.getInlinePlanNode(PlanNodeType.LIMIT) != null);
            assertTrue (indexSet.contains(((IndexScanPlanNode)p).getCatalogIndex().getTypeName()));
        }
        else {
            boolean flag = false;
            if ((p instanceof IndexScanPlanNode) == false)
                flag = true;
            else if (p.getInlinePlanNode(PlanNodeType.LIMIT) == null)
                flag = true;
            assertTrue (flag);
        }
    }

    /**
     * Test on replicated with pure column indexes
     */

    // ========================================================================

    // simple min() on indexed col
    public void testMin0001() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R");
        checkIndexLimit(pn, true, new String[]{"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    // simple min() on unindexed col
    public void testMin0002() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C4) FROM R");
        checkIndexLimit(pn, false, null);
    }

    // simple max() on indexed col
    public void testMax0001() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C1) FROM R");
        checkIndexLimit(pn, true, new String[]{"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    // simple max() on unindexed col
    public void testMax0002() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C4) FROM R");
        checkIndexLimit(pn, false, null);
    }

    // max() on indexed col with WHERE clause: should not replace
    public void testMax0003() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C2) FROM R WHERE C1 = ?");
        checkIndexLimit(pn, false, null);
    }

    public void testMax0004() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C2) FROM R WHERE C1=1 and C2 < ?");
        checkIndexLimit(pn, true, new String[]{"R_IDX2_TREE"});
    }

    public void testMax0005() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C2) FROM R WHERE C2 <= ? and C1 = 1");
        checkIndexLimit(pn, true, new String[]{"R_IDX2_TREE"});
    }

    public void testMax0006() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C2) FROM R WHERE C2 = ? and C1 = 1");
        checkIndexLimit(pn, true, new String[]{"R_IDX2_TREE"});
    }

    // combination of [min(), max(), sum()] tests: should not replace
    public void testCom0001() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1), MAX(C1) FROM R");
        checkIndexLimit(pn, false, null);
    }

    public void testCom0002() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1), SUM(C1) FROM R");
        checkIndexLimit(pn, false, null);
    }

    public void testCom0003() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT SUM(C1), MAX(C1) FROM R");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // min() on indexed col with where (on indexed col): case 1
    public void testMin0021() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"R_IDX2_TREE", "R_IDX4_TREE"});
    }

    // min() on indexed col with where (on indexed col): case 2
    public void testMin0022() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C2 = ?");
        checkIndexLimit(pn, true, new String[]{"R_IDX3_TREE"});
    }

    // min() on indexed col which is also in where
    public void testMin0023() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    // ========================================================================

    // min() on indexed col with more complicated where
    public void testMin0031() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM R WHERE C1 = ? AND C2 = ?");
        checkIndexLimit(pn, true, new String[]{"R_IDX4_TREE"});
    }

    // min() on indexed col with more complicated where: should not replace
    public void testMin0032() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM R WHERE C1 = ? OR C2 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with more complicated where: should not replace
    public void testMin0033() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM R WHERE C1 > ? OR C2 > ?");
        checkIndexLimit(pn, false, null);
    }

    public void testMin0034() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 > ?");
        checkIndexLimit(pn, false, null);
    }

    public void testMin0035() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM R WHERE C1 = ?");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // min() on indexed col with where (on indexed col), but should not replace: case 3
    public void testMin0041() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with where (partially on indexed col)
    public void testMin0042() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 = ? AND C4 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with where (none on indexed col)
    public void testMin0043() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C4 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with group by
    public void testMin005() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================


    /**
     * Test on replicated with expression indexes
     */

    // ========================================================================

    // no where clause, min() is expression matching index
    public void testMin10011() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1 + C2) FROM ER");
        checkIndexLimit(pn, true, new String[]{"ER_IDX1_TREE", "ER_IDX5_TREE"});
    }

    // no where clause, min() if function matching index
    public void testMin10012() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(ABS(C3 - 100)) FROM ER");
        checkIndexLimit(pn, true, new String[]{"ER_IDX2_TREE"});
    }

    // not replacable
    public void testMin10013() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM ER");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // where clause is expression, min() is column
    public void testMin10021() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM ER WHERE C1 + C2 = ?");
        checkIndexLimit(pn, true, new String[]{"ER_IDX5_TREE"});
    }

    // where clause is column-based, min() is expression
    public void testMin10022() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2 * C3) FROM ER WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"ER_IDX4_TREE"});
    }

    // both where and min() are expressions
    public void testMin10023() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2 - C3) FROM ER WHERE C1 - C2 = ?");
        checkIndexLimit(pn, true, new String[]{"ER_IDX3_TREE"});
    }

    // ========================================================================

    /**
     * Test on partitioned table, partitioned column is indexable
     */

    // ========================================================================

    // simple min() on indexed col
    public void testMin200() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P1");
        checkIndexLimit(pn, true, new String[]{"P1_IDX1_TREE", "P1_IDX2_TREE", "P1_IDX4_TREE"});
    }

    // simple min() on unindexed col
    public void testMin201() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C4) FROM P1");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // min() on indexed col with where (on indexed col): case 1
    public void testMin2021() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P1 WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"P1_IDX2_TREE", "P1_IDX4_TREE"});
    }

    // min() on indexed col with where (on indexed col): case 2
    public void testMin2022() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P1 WHERE C2 = ?");
        checkIndexLimit(pn, true, new String[]{"P1_IDX3_TREE"});
    }

    // min() on indexed col which is also in where
    public void testMin2023() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P1 WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"P1_IDX1_TREE", "P1_IDX2_TREE", "P1_IDX4_TREE"});
    }

    // ========================================================================

    // min() on indexed col with more complicated where
    public void testMin2031() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM P1 WHERE C1 = ? AND C2 = ?");
        checkIndexLimit(pn, true, new String[]{"P1_IDX4_TREE"});
    }

    // min() on indexed col with more complicated where: should not replace
    public void testMin2032() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM P1 WHERE C1 = ? OR C2 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with more complicated where: should not replace
    public void testMin2033() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM P1 WHERE C1 > ? OR C2 > ?");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // min() on indexed col with where (on indexed col), but should not replace: case 3
    public void testMin2041() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P1 WHERE C1 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with where (partially on indexed col)
    public void testMin2042() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P1 WHERE C1 = ? AND C4 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with where (none on indexed col)
    public void testMin2043() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P1 WHERE C4 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with group by
    public void testMin205() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P1 WHERE C1 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    /**
     * Test on partitioned table, partitioned column is not indexable
     */

    // ========================================================================

    // simple min() on indexed col
    public void testMin300() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P2");
        checkIndexLimit(pn, true, new String[]{"P2_IDX1_TREE", "P2_IDX2_TREE", "P2_IDX4_TREE"});
    }

    // simple min() on unindexed col
    public void testMin301() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C4) FROM P2");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // min() on indexed col with where (on indexed col): case 1
    public void testMin3021() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P2 WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"P2_IDX2_TREE", "P2_IDX4_TREE"});
    }

    // min() on indexed col with where (on indexed col): case 2
    public void testMin3022() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P2 WHERE C2 = ?");
        checkIndexLimit(pn, true, new String[]{"P2_IDX3_TREE"});
    }

    // min() on indexed col which is also in where
    public void testMin3023() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P2 WHERE C1 = ?");
        checkIndexLimit(pn, true, new String[]{"P2_IDX1_TREE", "P2_IDX2_TREE", "P2_IDX4_TREE"});
    }

    // ========================================================================

    // min() on indexed col with more complicated where
    public void testMin3031() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM P2 WHERE C1 = ? AND C2 = ?");
        checkIndexLimit(pn, true, new String[]{"P2_IDX4_TREE"});
    }

    // min() on indexed col with more complicated where: should not replace
    public void testMin3032() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM P2 WHERE C1 = ? OR C2 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with more complicated where: should not replace
    public void testMin3033() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM P2 WHERE C1 > ? OR C2 > ?");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    // min() on indexed col with where (on indexed col), but should not replace: case 3
    public void testMin3041() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P2 WHERE C1 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with where (partially on indexed col)
    public void testMin3042() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P2 WHERE C1 = ? AND C4 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with where (none on indexed col)
    public void testMin3043() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM P2 WHERE C4 = ?");
        checkIndexLimit(pn, false, null);
    }

    // min() on indexed col with group by
    public void testMin305() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM P2 WHERE C1 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    // ========================================================================

    /**
     * Test edge cases.
     */

    // ========================================================================

    public void testMin4011() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C1 > ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4012() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C1 >= ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4013() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C1 < ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4014() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C1 <= ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4015() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 = ? AND C2 > ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX2_TREE"});
    }

    public void testMin4016() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C2) FROM R WHERE C1 = ? AND C2 < ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX2_TREE"});
    }

    public void testMin4017() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C3) FROM R WHERE C1 = ? AND C3 < ?");
        checkIndexLimit(pn, false, null);
    }

    public void testMin4018() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MIN(C1) FROM R WHERE C1 > ? AND C2 = ? AND C3 = ?");
        checkIndexLimit(pn, false, null);
    }

    public void testMin4021() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C1) FROM R WHERE C1 > ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4022() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C1) FROM R WHERE C1 >= ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4023() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C1) FROM R WHERE C1 < ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4024() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C1) FROM R WHERE C1 <= ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX1_TREE", "R_IDX2_TREE", "R_IDX4_TREE"});
    }

    public void testMin4025() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C2) FROM R WHERE C1 = ? AND C2 > ?");
        checkIndexLimit(pn, false, null);
    }

    public void testMin4026() {
        List<AbstractPlanNode> pn = compileToFragments("SELECT MAX(C2) FROM R WHERE C2 = ?");
        checkIndexLimit(pn, true, new String[] {"R_IDX3_TREE"});
    }

}
