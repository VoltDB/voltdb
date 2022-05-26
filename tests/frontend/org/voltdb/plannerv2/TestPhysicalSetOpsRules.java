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

import org.junit.Test;
import org.voltdb.plannerv2.rules.PlannerRules.Phase;

public class TestPhysicalSetOpsRules extends Plannerv2TestCase {

    private PhysicalConversionRulesTester m_tester = new PhysicalConversionRulesTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(Phase.PHYSICAL_CONVERSION);
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testUnion() {
        m_tester.sql("select si from R1 union ALL select si from R2 union select ii from R3")
                .transform("VoltPhysicalUnion(all=[false])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n")
                .pass();
    }

    @Test
    public void testSetOpsLimit() {
        m_tester.sql("select si from R1 union ALL select si from R2 limit ? offset ?")
                .transform("VoltPhysicalLimit(limit=[?0], offset=[?1], pusheddown=[false])\n" +
                           "  VoltPhysicalUnion(all=[true])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();

        m_tester.sql("select si from R1 union ALL select si from R2 order by 1 limit 5 offset 4")
                .transform("VoltPhysicalLimit(limit=[5], offset=[4], pusheddown=[false])\n" +
                           "  VoltPhysicalSort(sort0=[$0], dir0=[ASC], pusheddown=[false])\n" +
                           "    VoltPhysicalUnion(all=[true])\n" +
                           "      VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "        VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "      VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "        VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
               .pass();
    }

    @Test
    public void testSetOpsOrderBy() {
        m_tester.sql("select si*2 as si2, si as si1 from R1 union ALL select i, bi from R2 order by si2 ASC, si1 DESC")
                .transform("VoltPhysicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], pusheddown=[false])\n" +
                           "  VoltPhysicalUnion(all=[true])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[2], expr#7=[*($t1, $t6)], SI2=[$t7], SI1=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    @Test
    public void testIntersect() {
        m_tester.sql("select si from R1 intersect select si from R2 intersect ALL select ii from R3")
                .transform("VoltPhysicalIntersect(all=[true])\n" +
                           "  VoltPhysicalIntersect(all=[false])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n")
                .pass();
    }

    @Test
    public void testSetOpsWithExpressionSubqueiries() {
        // @TODO Need to validate the results
//        m_tester.sql("select * from R1 where EXISTS (select si from R1 intersect select si from R2)")
//                       .transform("foo")
//                       .test();
//
//        m_tester.sql("select si from R2 union select i from R1 where i in (select i from R2 where R1.I > i)")
//                       .transform("foo")
//                       .test();

    }

    @Test
    public void testMultipleSetOps() {
        m_tester.sql("select si from R1 intersect select si from R2 union select ii from R3")
                .transform("VoltPhysicalUnion(all=[false])\n" +
                           "  VoltPhysicalIntersect(all=[false])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n")
                .pass();

        m_tester.sql("select si from R1 intersect (select si from R2 union select ii from R3)")
                .transform("VoltPhysicalIntersect(all=[false])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalUnion(all=[false])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "    VoltPhysicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n")
                .pass();
    }

    @Test
    public void testExcept() {
        m_tester.sql("select si from R1 except ALL select si from R2 except ALL select ii from R3")
                .transform("VoltPhysicalMinus(all=[true])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n")
                .pass();

        m_tester.sql("select si from R1 except (select si from R2 except select ii from R3)")
                .transform("VoltPhysicalMinus(all=[false])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalMinus(all=[false])\n" +
                           "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "    VoltPhysicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n")
                .pass();
    }

    @Test
    public void testSetOpsFilter() {
        m_tester.sql("select si from (select si from R1 union ALL select si from R2) u where si > 0")
                .transform("VoltPhysicalUnion(all=[true])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t1, $t6)], SI=[$t1], $condition=[$t7])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t1, $t6)], SI=[$t1], $condition=[$t7])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }
}
