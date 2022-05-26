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

package org.voltdb.plannerv2;

import org.voltdb.plannerv2.rules.PlannerRules.Phase;

public class TestOuterJoinSimplificationRules extends Plannerv2TestCase {

    private OuterJoinRulesTester m_tester = new OuterJoinRulesTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(Phase.LOGICAL_JOIN);
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRightToLeftJoin() {
        m_tester.sql("select R1.SI from R1 right join R2 on R1.I = R2.I")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], SI=[$t2])\n" +
                   "  VoltLogicalJoin(condition=[=($1, $0)], joinType=[left])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n")
        .pass();
    }

    public void testLeftJoin() {
        m_tester.sql("select R1.SI from R1 left join R2 on R1.I = R2.I")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], SI=[$t1])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[left])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullJoin() {
        m_tester.sql("select R1.SI from R1 full join R2 on R1.I = R2.I")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], SI=[$t1])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[full])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testInnerJoin() {
        m_tester.sql("select R1.SI from R1 inner join R2 on R1.I = R2.I")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], SI=[$t1])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

}
