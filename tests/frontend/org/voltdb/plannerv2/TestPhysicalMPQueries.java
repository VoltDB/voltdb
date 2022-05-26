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
import org.voltdb.plannerv2.rules.PlannerRules;

public class TestPhysicalMPQueries extends Plannerv2TestCase {

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
    public void testPartitioned1() {
        m_tester.sql("select I from P1")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                            "  VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitioned2() {
        m_tester.sql("select I from P1 where I = 10")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[10], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                            "  VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedLimit1() {
        m_tester.sql("select i from P1 limit 10")
                .transform("VoltPhysicalLimit(limit=[10], pusheddown=[true])\n" +
                            "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                            "    VoltPhysicalLimit(limit=[10], pusheddown=[false])\n" +
                            "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                            "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedLimit2() {
        m_tester.sql("select i from P1 limit 10 offset 3")
                .transform("VoltPhysicalLimit(limit=[10], offset=[3], pusheddown=[true])\n" +
                            "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                            "    VoltPhysicalLimit(limit=[13], pusheddown=[false])\n" +
                            "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                            "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedLimit3() {
        m_tester.sql("select i from P1 offset 3")
                .transform("VoltPhysicalLimit(offset=[3], pusheddown=[false])\n" +
                            "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                            "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedLimit4() {
        m_tester.sql("select i from P1 where i = 8 limit 3")
                .transform("VoltPhysicalLimit(limit=[3], pusheddown=[false])\n" +
                            "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[8], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                            "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedLimit5() {
        m_tester.sql("select i from P1 where si = 8 limit 3")
                .transform("VoltPhysicalLimit(limit=[3], pusheddown=[true])\n" +
                            "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                            "    VoltPhysicalLimit(limit=[3], pusheddown=[false])\n" +
                            "      VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[8], expr#8=[=($t6, $t7)], I=[$t0], $condition=[$t8])\n" +
                            "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedSort1() {
        m_tester.sql("select i from PI1 where I = 6 order by ii")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[6], expr#7=[=($t0, $t6)], I=[$t0], II=[$t2], $condition=[$t7])\n" +
                            "  VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}], index=[PI1_IND1_ASCEQ0_0])\n")
                .pass();
    }

    public void testPartitionedSort2() {
        // Here the final plan does not have SORT pushed down because it's cost is the same
        // as Sort / Merge Exchange / Sort / Scan plan
        m_tester.sql("select i from P1 order by SI")
                .transform("VoltPhysicalSort(sort0=[$1], dir0=[ASC], pusheddown=[false])\n" +
                            "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                            "    VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testPartitionedLimitSort1() {
        m_tester.sql("select i from PI1 order by ii limit 10 offset 4")
                .transform("VoltPhysicalLimit(limit=[10], offset=[4], pusheddown=[true])\n" +
                            "  VoltPhysicalSort(sort0=[$1], dir0=[ASC], pusheddown=[true])\n" +
                            "    VoltPhysicalMergeExchange(distribution=[hash[0]], collation=[[1]])\n" +
                            "      VoltPhysicalLimit(limit=[14], pusheddown=[false])\n" +
                            "        VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], II=[$t2])\n" +
                            "          VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}], index=[PI1_IND1_ASCEQ0_0])\n")
                .pass();
    }

    public void testPartitionedLimitSort2() {
        m_tester.sql("select i from PI1 order by ii offset 10")
                .transform("VoltPhysicalLimit(offset=[10], pusheddown=[false])\n" +
                            "  VoltPhysicalSort(sort0=[$1], dir0=[ASC], pusheddown=[true])\n" +
                            "    VoltPhysicalMergeExchange(distribution=[hash[0]], collation=[[1]])\n" +
                            "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], II=[$t2])\n" +
                            "        VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}], index=[PI1_IND1_ASCEQ0_0])\n")
                .pass();
    }

//    public void testPartitionedWithAggregate() {
//        m_tester.sql("select max(R1.I) from R1")
//        .transform("\n")
//        .pass();
//    }
//
//    public void testPartitionedWithAggregate4() {
//        m_tester.sql("select count(*) from P1")
//        .transform("\n")
//        .pass();
//    }
//
//    public void testPartitionedWithAggregate5() {
//        m_tester.sql("select count(P1.I) from P1")
//        .transform("\n")
//        .pass();
//    }
//
//    public void testPartitionedWithAggregate6() {
//        m_tester.sql("select avg(P1.I) from P1")
//        .transform("\n")
//        .pass();
//    }
//
//    public void testPartitionedWithAggregate7() {
//        m_tester.sql("select distinct(P1.I) from P1") // no coord aggr because P1.I is part column
//        .transform("\n")
//        .pass();
//    }
//
//    public void testPartitionedWithAggregate8() {
//        m_tester.sql("select distinct(P1.SI) from P1") // coord aggr because P1.SI is not a part column
//        .transform("\n")
//        .pass();
//    }

    public void testPartitionedWithAggregate9() {
        m_tester.sql("select max(P1.I) from P1 where I = 8")
        .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], coordinator=[false], type=[serial])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[8], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testPartitionedSetOp1() {
        m_tester.sql("select I from p1 union select I from r1")
        .transform("VoltPhysicalUnion(all=[false])\n" +
                    "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testPartitionedSetOp2() {
        m_tester.sql("select I from p1 where i = 9 union select I from r1")
        .transform("VoltPhysicalUnion(all=[false])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testPartitionedSetOp3() {
        m_tester.sql("select I from p1 where i = 9 union select I from p2 where i = 9")
        .transform("VoltPhysicalUnion(all=[false])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testPartitionedSetOp4() {
        m_tester.sql("select I from r1 union (select I from p2 except select I from r2)")
        .transform("VoltPhysicalUnion(all=[false])\n" +
                    "  VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "  VoltPhysicalMinus(all=[false])\n" +
                    "    VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltPhysicalTableSequentialScan(table=[[public, P2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testPartitionedJoinWithSort() {
        m_tester.sql("select r.i from p1 r full join p1 l on r.i = l.i order by 1")
        .transform("VoltPhysicalSort(sort0=[$0], dir0=[ASC], pusheddown=[false])\n" +
                    "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "    VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "      VoltPhysicalNestLoopJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "        VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "        VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testPartitionedJoinWithNotDistinct() {
        m_tester.sql("SELECT P1.I FROM P1 INNER JOIN P2 ON P1.I IS NOT DISTINCT FROM P2.I")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "  VoltPhysicalNestLoopJoin(condition=[CASE(IS NULL($0), IS NULL($1), IS NULL($1), IS NULL($0), =($0, $1))], joinType=[inner])\n" +
                    "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                    "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltPhysicalTableSequentialScan(table=[[public, P2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

}
