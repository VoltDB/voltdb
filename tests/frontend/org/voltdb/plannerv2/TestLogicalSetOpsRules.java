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

public class TestLogicalSetOpsRules extends Plannerv2TestCase {

    private LogicalRulesTester m_logicalTester = new LogicalRulesTester();
    private ValidationTester m_validationTester = new ValidationTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_logicalTester.phase(Phase.LOGICAL);
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testUnion() {
        m_logicalTester.sql("select si from R1 union ALL select si from R2")
                .transform("VoltLogicalUnion(all=[true])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 union select si from R2 union select ii from R3")
                .transform("VoltLogicalUnion(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 union select si from R2 union ALL select ii from R3")
                .transform("VoltLogicalUnion(all=[true])\n" +
                           "  VoltLogicalUnion(all=[false])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 union ALL select si from R2 union select ii from R3")
                .transform("VoltLogicalUnion(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();
    }

    public void testSetOpsLimit() {
        m_logicalTester.sql("select si from R1 union ALL select si from R2 limit 5 offset 4")
                .transform("VoltLogicalLimit(limit=[5], offset=[4])\n" +
                           "  VoltLogicalUnion(all=[true])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 union ALL select si from R2 limit ? offset ?")
                .transform("VoltLogicalLimit(limit=[?0], offset=[?1])\n" +
                           "  VoltLogicalUnion(all=[true])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 union ALL select si from R2 order by 1 limit 5 offset 4")
               .transform("VoltLogicalLimit(limit=[5], offset=[4])\n" +
                          "  VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                          "    VoltLogicalUnion(all=[true])\n" +
                          "      VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                          "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                          "      VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                          "        VoltLogicalTableScan(table=[[public, R2]])\n")
               .pass();
    }

    public void testSetOpsOrderBy() {
        m_logicalTester.sql("select si*2 as msi from R1 union ALL select si from R2 order by msi")
                .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                           "  VoltLogicalUnion(all=[true])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[2], expr#7=[*($t1, $t6)], MSI=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si*2 as si2, si as si1 from R1 union ALL select i, bi from R2 order by si2 ASC, si1 DESC")
                .transform("VoltLogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                           "  VoltLogicalUnion(all=[true])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[2], expr#7=[*($t1, $t6)], SI2=[$t7], SI1=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 union ALL select si from R2 order by si+1")
                .transform("VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                           "  VoltLogicalUnion(all=[true])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[+($t1, $t6)], SI=[$t1], EXPR$1=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[+($t1, $t6)], SI=[$t1], EXPR$1=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }

    public void testUnsupportedSetOps() {
        m_validationTester.sql("select ^si^ from R1 union ALL select v from R2")
                .exception("Type mismatch in column 1 of UNION ALL")
                .pass();

        m_validationTester.sql("select si from R1 union ALL select max(si) as msi from R2 order by ^msi^")
                .exception("Column 'MSI' not found in any table")
                .pass();

        m_validationTester.sql("select si + 1 from R1 union ALL select si from R2 order by ^si^+1")
                .exception("Column 'SI' not found in any table")
                .pass();

    }

    public void testIntersect() {
        m_logicalTester.sql("select si from R1 intersect ALL select si from R2 where si > 3")
                .transform("VoltLogicalIntersect(all=[true])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], SI=[$t1], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 intersect select si from R2 intersect select ii from R3")
                .transform("VoltLogicalIntersect(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 intersect ALL select si from R2 intersect select ii from R3")
                .transform("VoltLogicalIntersect(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                        "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 intersect select si from R2 intersect ALL select ii from R3")
                .transform("VoltLogicalIntersect(all=[true])\n" +
                           "  VoltLogicalIntersect(all=[false])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();
    }

    public void testSetOpsWithExpressionSubqueiries() {
        // @TODO Need to validate the results
        m_logicalTester.sql("select * from R1 where EXISTS (select si from R1 intersect select si from R2)")
//                       .transform("VoltLogicalCalc(expr#0..6=[{inputs}], proj#0..5=[{exprs}])\n" +
//                                  "  VoltLogicalJoin(condition=[true], joinType=[inner])\n" +
//                                  "    VoltLogicalTableScan(table=[[public, R1]])\n" +
//                                  "    VoltLogicalCalc(expr#0=[{inputs}], expr#1=[IS NOT NULL($t0)], $f0=[$t0], $condition=[$t1])\n" +
//                                  "      VoltLogicalAggregate(group=[{}], agg#0=[MIN($0)])\n" +
//                                  "        VoltLogicalCalc(expr#0=[{inputs}], expr#1=[true], $f0=[$t1])\n" +
//                                  "          VoltLogicalIntersect(all=[false])\n" +
//                                  "            VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
//                                  "              VoltLogicalTableScan(table=[[public, R1]])\n" +
//                                  "            VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
//                                  "              VoltLogicalTableScan(table=[[public, R2]])\n")
                       .pass();

        m_logicalTester.sql("select si from R2 union select i from R1 where i in (select i from R2 where R1.I > i)")
//                       .transform("VoltLogicalUnion(all=[false])\n" +
//                                  "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
//                                  "    VoltLogicalTableScan(table=[[public, R2]])\n" +
//                                  "  VoltLogicalCalc(expr#0..7=[{inputs}], I=[$t0])\n" +
//                                  "    VoltLogicalJoin(condition=[AND(=($0, $7), =($0, $6))], joinType=[inner])\n" +
//                                  "      VoltLogicalTableScan(table=[[public, R1]])\n" +
//                                  "      VoltLogicalAggregate(group=[{0, 1}])\n" +
//                                  "        VoltLogicalCalc(expr#0..6=[{inputs}], I=[$t0], I0=[$t6])\n" +
//                                  "          VoltLogicalJoin(condition=[>($6, $0)], joinType=[inner])\n" +
//                                  "            VoltLogicalTableScan(table=[[public, R2]])\n" +
//                                  "            VoltLogicalAggregate(group=[{0}])\n" +
//                                  "              VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
//                                  "                VoltLogicalTableScan(table=[[public, R1]])\n")
                       .pass();

    }

    public void testMultipleSetOps() {
        m_logicalTester.sql("select si from R1 intersect select si from R2 union select ii from R3")
                .transform("VoltLogicalUnion(all=[false])\n" +
                           "  VoltLogicalIntersect(all=[false])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 intersect (select si from R2 union select ii from R3)")
                .transform("VoltLogicalIntersect(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalUnion(all=[false])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "      VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();
    }

    public void testExcept() {
        m_logicalTester.sql("select si from R1 except ALL select si from R2")
                .transform("VoltLogicalMinus(all=[true])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                        "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 except select si from R2 except select ii from R3")
                .transform("VoltLogicalMinus(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 except ALL select si from R2 except ALL select ii from R3")
                .transform("VoltLogicalMinus(all=[true])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();

        m_logicalTester.sql("select si from R1 except (select si from R2 except select ii from R3)")
                .transform("VoltLogicalMinus(all=[false])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalMinus(all=[false])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                           "      VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();
    }

    public void testSetOpsFilter() {
        m_logicalTester.sql("select si from (select si from R1 union ALL select si from R2) u where si > 0")
                .transform("VoltLogicalUnion(all=[true])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t1, $t6)], SI=[$t1], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t1, $t6)], SI=[$t1], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }
}
