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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;
import org.voltdb.newplanner.rules.PlannerPhase;
import org.voltdb.newplanner.util.VoltDBRelUtil;
import org.voltdb.types.CalcitePlannerType;

public class TestPhysicalConversionRules extends PlanRulesTestCase {
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

    @Override
    void assertPlanMatch(String sql, String expectedPlan) {
        RelRoot root = parseValidateAndConvert(sql);

        RelTraitSet logicalTraits = root.rel.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL);
        RelNode nodeAfterLogicalRules = CalcitePlanner.transform(CalcitePlannerType.VOLCANO, PlannerPhase.LOGICAL,
                root.rel, logicalTraits);

        Preconditions.checkArgument(nodeAfterLogicalRules.getTraitSet().contains(VoltDBLRel.VOLTDB_LOGICAL));

        nodeAfterLogicalRules.getCluster().getPlanner().addRelTraitDef(RelDistributions.SINGLETON.getTraitDef());

        nodeAfterLogicalRules = VoltDBRelUtil.addTraitRecurcively(nodeAfterLogicalRules, RelDistributions.ANY);

        RelTraitSet physicalTraits = nodeAfterLogicalRules.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL)
                .replace(RelDistributions.ANY);

        RelNode nodeAfterPhysicalRules = CalcitePlanner.transform(CalcitePlannerType.VOLCANO,
                PlannerPhase.PHYSICAL_CONVERSION, nodeAfterLogicalRules, physicalTraits);

        String actualPlan = RelOptUtil.toString(nodeAfterPhysicalRules);
        assertEquals(expectedPlan, actualPlan);
    }

    public void testSimpleSeqScan() {
        assertPlanMatch("select si from Ri1",
                "VoltDBPCalc(expr#0..3=[{inputs}], SI=[$t1], split=[1])\n" +
                        "  VoltDBPSingletonExchange(distribution=[single])\n" +
                        "    VoltDBPTableSeqScan(table=[[catalog, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n");
    }
}
