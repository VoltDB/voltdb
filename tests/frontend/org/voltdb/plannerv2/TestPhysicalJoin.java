/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2;

import org.voltdb.plannerv2.rules.PlannerRules;

public class TestPhysicalJoin extends Plannerv2TestCase {

    private PhysicalConversionRulesTester m_tester = new PhysicalConversionRulesTester();

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(PlannerRules.Phase.PHYSICAL_CONVERSION);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testNLIJ1() {
        m_tester.sql("SELECT RI1.i FROM RI1 INNER JOIN R2 ON RI1.i = R2.i")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t1], split=[1])\n" +
                            "  VoltPhysicalNestLoopIndexJoin(condition=[=($1, $0)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                            "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], split=[1])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], split=[1])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testNLIJ2() {
        m_tester.sql("SELECT RI1.i FROM R2 INNER JOIN RI1 ON RI1.i = R2.i")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t1], split=[1])\n" +
                            "  VoltPhysicalNestLoopIndexJoin(condition=[=($1, $0)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                            "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], split=[1])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], split=[1])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }


}
