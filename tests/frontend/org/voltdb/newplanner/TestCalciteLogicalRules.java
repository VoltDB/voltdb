/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.newplanner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;
import org.voltdb.newplanner.rules.PlannerPhase;
import org.voltdb.types.CalcitePlannerType;

public class TestCalciteLogicalRules extends VoltConverterTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestVoltSqlValidator.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init(CatalogAdapter.schemaPlusFromDatabase(getDatabase()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertPlanMatch(String sql, String expectedPlan) {
        RelRoot root = parseValidateAndConvert(sql);
        RelNode node = CalcitePlanner.transform(CalcitePlannerType.HEP, PlannerPhase.CALCITE_LOGICAL,
                root.rel);
        String actualPlan = RelOptUtil.toString(node);
        assertEquals(expectedPlan, actualPlan);
    }

    public void testSimple() {
        assertPlanMatch("select si from Ri1",
                "LogicalCalc(expr#0..3=[{inputs}], SI=[$t1])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RI1]])\n");
    }

    public void testSeqScan() {
        assertPlanMatch("select * from R1",
                "LogicalCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithProjection() {
        assertPlanMatch("select i, si from R1",
                "LogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }
}
