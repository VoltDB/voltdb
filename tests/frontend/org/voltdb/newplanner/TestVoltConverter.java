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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;

public class TestVoltConverter extends VoltConverterTestCase {
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


    public void testSimple() {
        RelRoot root = parseValidateAndConvert("select i from R2");
        assertEquals("Root {kind: SELECT, rel: LogicalProject#1, rowType: RecordType(INTEGER I), fields: [<0, I>], collation: []}",
                root.toString());

        final HepProgram program = HepProgram.builder()
                .addRuleInstance(CalcMergeRule.INSTANCE)
                .addRuleInstance(FilterCalcMergeRule.INSTANCE)
                .addRuleInstance(FilterToCalcRule.INSTANCE)
                .addRuleInstance(ProjectCalcMergeRule.INSTANCE)
                .addRuleInstance(ProjectToCalcRule.INSTANCE)
                .addRuleInstance(ProjectMergeRule.INSTANCE)
                .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
                .build();

        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(root.rel);
        root = root.withRel(planner.findBestExp());
        System.out.println(RelOptUtil.toString(root.rel));
//        RelNode tNode = CalcitePlanner.transform(CalcitePlannerType.HEP, PlannerPhase.CALCITE_LOGICAL,
//                root.rel);
//        System.out.println(RelOptUtil.toString(tNode));
    }

}
