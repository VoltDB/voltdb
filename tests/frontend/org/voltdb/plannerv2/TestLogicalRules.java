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

public class TestLogicalRules extends Plannerv2TestCase {

    private LogicalRulesTester m_tester = new LogicalRulesTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(Phase.LOGICAL);
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSimpleSeqScan() {
        m_tester.sql("select si from Ri1")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], SI=[$t1])\n" +
                        "  VoltLogicalTableScan(table=[[public, RI1]])\n")
                .pass();
    }

    public void testSeqScan() {
        m_tester.sql("select * from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithProjection() {
        m_tester.sql("select i, si from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithProjectionExpr() {
        m_tester.sql("select i * 5 from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[*($t0, $t6)], EXPR$0=[$t7])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithFilter() {
        m_tester.sql("select i from R1 where i = 5")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[=($t0, $t6)], I=[$t0], " +
                        "$condition=[$t7])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithFilterParam() {
        m_tester.sql("select i from R1 where i = ? and v = ?")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[?0], expr#7=[=($t0, $t6)], expr#8=[?1], " +
                        "expr#9=[=($t5, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], $condition=[$t10])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithStringFilter() {
        m_tester.sql("select i from R1 where v = 'FOO1'")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['FOO1'], expr#7=[=($t5, $t6)], I=[$t0], " +
                        "$condition=[$t7])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithFilterWithTypeConversion() {
        m_tester.sql("select i from R1 where si = 5")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[5], " +
                        "expr#8=[=($t6, $t7)], I=[$t0], $condition=[$t8])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    // Partition info are introduced in the physical plan phase. In logical plan phase there is no difference.
    public void testSeqScanPartitioned() {
        m_tester.sql("select * from P1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "  VoltLogicalTableScan(table=[[public, P1]])\n")
                .pass();

        m_tester.sql("select i from P1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "  VoltLogicalTableScan(table=[[public, P1]])\n")
                .pass();
    }

    // Index info are introduced in the physical plan phase. In logical plan phase there is no difference.
    public void testIndexScan() {
        m_tester.sql("select bi from RI1 where i > 45 and ti > 3")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], expr#4=[45], expr#5=[>($t0, $t4)], expr#6=[3], " +
                        "expr#7=[>($t3, $t6)], expr#8=[AND($t5, $t7)], BI=[$t2], $condition=[$t8])\n" +
                        "  VoltLogicalTableScan(table=[[public, RI1]])\n")
                .pass();
    }

    public void testSeqScanWithLimit() {
        m_tester.sql("select i from R1 limit 5")
                .transform("VoltLogicalLimit(limit=[5])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithLimitParam() {
        m_tester.sql("select i from R1 limit ?")
                .transform("VoltLogicalLimit(limit=[?0])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithFilterAndLimit() {
        m_tester.sql("select i from R1 where si > 3 limit 5")
                .transform("VoltLogicalLimit(limit=[5])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], I=[$t0], " +
                        "$condition=[$t7])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithOrderByAndLimitAndFilter() {
        m_tester.sql("select si, i from RI1 where I > 3 order by si limit 3")
                .transform("VoltLogicalLimit(limit=[3])\n" +
                        "  VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                        "    VoltLogicalCalc(expr#0..3=[{inputs}], expr#4=[3], expr#5=[>($t0, $t4)], SI=[$t1], " +
                        "I=[$t0], $condition=[$t5])\n" +
                        "      VoltLogicalTableScan(table=[[public, RI1]])\n")
                .pass();
    }

    public void testSeqScanWithOffset() {
        m_tester.sql("select i from R1 offset 1")
                .transform("VoltLogicalLimit(offset=[1])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithLimitOffset() {
        m_tester.sql("select i from R1 limit 5 offset 1")
                .transform("VoltLogicalLimit(limit=[5], offset=[1])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select i from R1 offset 1 limit 5")
                .transform("VoltLogicalLimit(limit=[5], offset=[1])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithLimitOffsetSort() {
        m_tester.sql("select i from R1 order by bi limit 5 offset 1")
                .transform("VoltLogicalLimit(limit=[5], offset=[1])\n" +
                        "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select i from R1 order by bi offset 1 limit 5")
                .transform("VoltLogicalLimit(limit=[5], offset=[1])\n" +
                        "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithOrderByAndFilter() {
        m_tester.sql("select * from R1 where si > 3 order by i")
                .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], " +
                        "proj#0..5=[{exprs}], $condition=[$t7])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select i, bi, si from R1 where si > 3 order by i")
                .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], " +
                        "I=[$t0], BI=[$t3], SI=[$t1], $condition=[$t7])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithOrderBy() {
        m_tester.sql("select si from R1 order by i, si desc")
                .transform("VoltLogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select bi, i, si from R1 order by i, si desc")
                .transform("VoltLogicalSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[DESC])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], BI=[$t3], I=[$t0], SI=[$t1])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithOrderByExpr() {
        m_tester.sql("select bi, i, si from R1 order by i, si + 1 desc")
                .transform("VoltLogicalSort(sort0=[$1], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[+($t1, $t6)], BI=[$t3], " +
                        "I=[$t0], SI=[$t1], EXPR$3=[$t7])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testSeqScanWithOrderByAndLimit() {
        m_tester.sql("select bi, i, si from R1 order by i limit 5")
                .transform("VoltLogicalLimit(limit=[5])\n" +
                        "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], BI=[$t3], I=[$t0], SI=[$t1])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testIndexScanWithLimitOffset() {
        m_tester.sql("select si, i from RI1 where I > 3 limit 3 offset 4")
                .transform("VoltLogicalLimit(limit=[3], offset=[4])\n" +
                        "  VoltLogicalCalc(expr#0..3=[{inputs}], expr#4=[3], expr#5=[>($t0, $t4)], SI=[$t1], " +
                        "I=[$t0], $condition=[$t5])\n" +
                        "    VoltLogicalTableScan(table=[[public, RI1]])\n")
                .pass();
    }

    public void testConstIntExpr() {
        m_tester.sql("select 5 from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], EXPR$0=[$t6])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testBinaryIntExpr() {
        m_tester.sql("select 5 + i from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[+($t6, $t0)], EXPR$0=[$t7])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testConstBinaryIntExpr() {
        m_tester.sql("select 5 + 5 from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[+($t6, $t6)], EXPR$0=[$t7])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testConcatStringExpr() {
        m_tester.sql("select '55' || '22' from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['55'], expr#7=['22'], expr#8=[||($t6, $t7)], " +
                        "EXPR$0=[$t8])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select v || '22' from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['22'], expr#7=[||($t5, $t6)], " +
                        "EXPR$0=[$t7])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testDatetimeConstExpr() {
        m_tester.sql("select TIMESTAMP '1969-07-20 20:17:40' from RTYPES")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1969-07-20 20:17:40], EXPR$0=[$t9])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();
    }

    public void testBinaryDatetimeExpr() {
        m_tester.sql("select ts - INTERVAL '1' DAY from RTYPES")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[86400000], expr#10=[-($t6, $t9)], " +
                        "EXPR$0=[$t10])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();
    }

    public void testCompareInExpr() {
        // Calcite transforms the IN expression into ORs
        m_tester.sql("select 1 from RTYPES where i IN (1, 2)")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[=($t3, $t9)], expr#11=[2], " +
                        "expr#12=[=($t3, $t11)], expr#13=[OR($t10, $t12)], EXPR$0=[$t9], $condition=[$t13])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();

        m_tester.sql("select 1 from RTYPES where i IN (1, ?, 3)")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[=($t3, $t9)], expr#11=[?0], " +
                        "expr#12=[=($t3, $t11)], expr#13=[3], expr#14=[=($t3, $t13)], expr#15=[OR($t10, $t12, $t14)], " +
                        "EXPR$0=[$t9], $condition=[$t15])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();
    }

    public void testCompareLikeExpr() {
        m_tester.sql("select 1 from RTYPES where vc LIKE 'ab%c'")
                //.transform("")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=['ab%c'], " +
                        "expr#11=[LIKE($t8, $t10)], EXPR$0=[$t9], $condition=[$t11])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();

        m_tester.sql("select 1 from RTYPES where vc LIKE ?")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[?0], expr#11=[LIKE($t8, $t10)], " +
                        "EXPR$0=[$t9], $condition=[$t11])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();
    }

    public void testAbsExpr() {
        m_tester.sql("select abs(i) from RTYPES")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[ABS($t3)], EXPR$0=[$t9])\n" +
                        "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .pass();
    }

    public void testAggr() {
        m_tester.sql("select avg(ti) from R1")
                .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[AVG($0)])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], TI=[$t2])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select avg(ti) from R1 group by i")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltLogicalAggregate(group=[{0}], EXPR$0=[AVG($1)])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select count(i) from R1 where ti > 3")
                .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t2, $t6)], I=[$t0], " +
                        "$condition=[$t7])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select count(*) from R1")
                .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], $f0=[$t6])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select max(TI) from R1 group by SI having SI > 0")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)], EXPR$0=[$t1], " +
                        "$condition=[$t3])\n" +
                        "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], TI=[$t2])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select max(TI), SI, min(TI), I from R1 group by SI, I having avg(BI) > max(BI)")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[>($t4, $t5)], EXPR$0=[$t2], SI=[$t0], " +
                        "EXPR$2=[$t3], I=[$t1], $condition=[$t6])\n" +
                        "  VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)], EXPR$2=[MIN($2)], agg#2=[AVG($3)], agg#3=[MAX($3)])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0], TI=[$t2], BI=[$t3])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select max(TI), SI, I, min(TI) from R1 group by I, SI having avg(BI) > 0 and si > 0")
                .transform("VoltLogicalCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[>($t4, $t5)], expr#7=[>($t1, $t5)], " +
                        "expr#8=[AND($t6, $t7)], EXPR$0=[$t2], SI=[$t1], I=[$t0], EXPR$3=[$t3], $condition=[$t8])\n" +
                        "  VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)], EXPR$3=[MIN($2)], agg#2=[AVG($3)])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select max(TI), SI from R1 where I > 0 group by SI, I limit 3")
                .transform("VoltLogicalLimit(limit=[3])\n" +
                        "  VoltLogicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2], SI=[$t0])\n" +
                        "    VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)])\n" +
                        "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t0, $t6)], SI=[$t1], " +
                        "I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                        "        VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select max(TI), SI from R1 where I > 0 group by SI, I order by SI limit 3")
                .transform("VoltLogicalLimit(limit=[3])\n" +
                        "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltLogicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2], SI=[$t0])\n" +
                        "      VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)])\n" +
                        "        VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t0, $t6)], " +
                        "SI=[$t1], I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                        "          VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testDistinct() {
        m_tester.sql("select distinct TI, I from R1")
                .transform("VoltLogicalAggregate(group=[{0, 1}])\n" +
                        "  VoltLogicalCalc(expr#0..5=[{inputs}], TI=[$t2], I=[$t0])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select distinct max(TI) from R1 group by I")
                .transform("VoltLogicalAggregate(group=[{0}])\n" +
                        "  VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "    VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                        "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "        VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();

        m_tester.sql("select max (distinct (TI)) from R1 group by I")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX(DISTINCT $1)])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .pass();
    }

    public void testJoin() {
        // TODO: Calcite casting to INTEGER instead of BigInt.
        // We may fail some JUnit tests when we execute such Calcite plan.
        m_tester.sql("select R1.i, R2.v from R1, R2 where R2.si = R1.i and R2.v = 'foo'")
                .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0], V=[$t2])\n" +
                           "  VoltLogicalJoin(condition=[=(CAST($1):INTEGER, $0)], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], SI=[$t1], V=[$t5], $condition=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_tester.sql("select R1.i, R2.v from R1 inner join R2 on R2.si = R1.i where R2.v = 'foo'")
                .transform("VoltLogicalCalc(expr#0..2=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "  VoltLogicalJoin(condition=[=($2, $0)], joinType=[inner])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=['foo'], " +
                        "expr#8=[=($t5, $t7)], V=[$t5], SI0=[$t6], $condition=[$t8])\n" +
                        "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_tester.sql("select R2.si, R1.i from R1 inner join " +
                "R2 on R2.i = R1.si where R2.v = 'foo' and R1.si > 4 and R1.ti > R2.i")
                .transform("VoltLogicalCalc(expr#0..6=[{inputs}], SI=[$t5], I=[$t0])\n" +
                           "  VoltLogicalJoin(condition=[AND(=($4, $3), >($2, $4))], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[4], expr#8=[>($t1, $t7)], proj#0..2=[{exprs}], SI0=[$t6], $condition=[$t8])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], proj#0..1=[{exprs}], V=[$t5], $condition=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si where R1.I + R2.ti = 5")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                           "  VoltLogicalJoin(condition=[AND(=($1, $2), =(+($0, $3), 5))], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], TI=[$t2])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
}

    public void testThreeWayJoin() {
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                           "  VoltLogicalJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..4=[{inputs}], expr#5=[CAST($t4):VARCHAR(256) CHARACTER SET \"UTF-8\" COLLATE \"UTF-8$en_US$primary\"], proj#0..1=[{exprs}], V00=[$t5])\n" +
                           "      VoltLogicalJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[4], expr#8=[>($t1, $t7)], proj#0..1=[{exprs}], SI0=[$t6], $condition=[$t8])\n" +
                           "          VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "        VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], V=[$t5])\n" +
                           "          VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "    VoltLogicalCalc(expr#0..2=[{inputs}], expr#3=['foo'], expr#4=[<>($t1, $t3)], VC=[$t1], $condition=[$t4])\n" +
                           "      VoltLogicalTableScan(table=[[public, R3]])\n")
                .pass();
    }

    public void testThreeWayFullJoinWithAmbiguousColumnFail() {
        m_tester.sql("select PK from R3 full join R4  using(PK) full join R5 using(PK) ")
                .transform("");
    }

    public void testSubqueriesJoin() {
        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from R2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], V=[$t1], V0=[$t3])\n" +
                           "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], expr#8=[3], expr#9=[=($t0, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], V=[$t5], $condition=[$t10])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[30.3], expr#7=[=($t4, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }

    public void testIntersect() {
        m_tester.sql("select * from R1 where EXISTS (select si from R1 intersect select si from R2)")
                .transform("VoltLogicalCalc(expr#0..6=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "  VoltLogicalJoin(condition=[true], joinType=[inner])\n" +
                        "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                        "    VoltLogicalCalc(expr#0=[{inputs}], expr#1=[IS NOT NULL($t0)], $f0=[$t0], $condition=[$t1])\n" +
                        "      VoltLogicalAggregate(group=[{}], agg#0=[MIN($0)])\n" +
                        "        VoltLogicalCalc(expr#0=[{inputs}], expr#1=[true], $f0=[$t1])\n" +
                        "          VoltLogicalIntersect(all=[false])\n" +
                        "            VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                        "              VoltLogicalTableScan(table=[[public, R1]])\n" +
                        "            VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                        "              VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }

    public void testInExpr() {
        m_tester.sql("select * from r1 where i in(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t0, $t6)], expr#8=[1], " +
                        "expr#9=[=($t0, $t8)], expr#10=[2], expr#11=[=($t0, $t10)], expr#12=[3], expr#13=[=($t0, $t12)], " +
                        "expr#14=[4], expr#15=[=($t0, $t14)], expr#16=[5], expr#17=[=($t0, $t16)], expr#18=[6], " +
                        "expr#19=[=($t0, $t18)], expr#20=[7], expr#21=[=($t0, $t20)], expr#22=[8], expr#23=[=($t0, $t22)], " +
                        "expr#24=[9], expr#25=[=($t0, $t24)], expr#26=[10], expr#27=[=($t0, $t26)], expr#28=[11], " +
                        "expr#29=[=($t0, $t28)], expr#30=[12], expr#31=[=($t0, $t30)], expr#32=[13], expr#33=[=($t0, $t32)], " +
                        "expr#34=[14], expr#35=[=($t0, $t34)], expr#36=[15], expr#37=[=($t0, $t36)], expr#38=[16], " +
                        "expr#39=[=($t0, $t38)], expr#40=[17], expr#41=[=($t0, $t40)], expr#42=[18], expr#43=[=($t0, $t42)], " +
                        "expr#44=[19], expr#45=[=($t0, $t44)], expr#46=[20], expr#47=[=($t0, $t46)], expr#48=[21], " +
                        "expr#49=[=($t0, $t48)], expr#50=[OR($t7, $t9, $t11, $t13, $t15, $t17, $t19, $t21, $t23, $t25, $t27, " +
                        "$t29, $t31, $t33, $t35, $t37, $t39, $t41, $t43, $t45, $t47, $t49)], proj#0..5=[{exprs}], $condition=[$t50])\n" +
                        "  VoltLogicalTableScan(table=[[public, R1]])\n").pass();

        m_tester.sql("select * from r3 where ii in(pk, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)")
                .transform("VoltLogicalCalc(expr#0..2=[{inputs}], expr#3=[=($t2, $t0)], expr#4=[1], expr#5=[=($t2, $t4)], " +
                        "expr#6=[2], expr#7=[=($t2, $t6)], expr#8=[3], expr#9=[=($t2, $t8)], expr#10=[4], expr#11=[=($t2, $t10)], " +
                        "expr#12=[5], expr#13=[=($t2, $t12)], expr#14=[6], expr#15=[=($t2, $t14)], expr#16=[7], " +
                        "expr#17=[=($t2, $t16)], expr#18=[8], expr#19=[=($t2, $t18)], expr#20=[9], expr#21=[=($t2, $t20)], " +
                        "expr#22=[10], expr#23=[=($t2, $t22)], expr#24=[11], expr#25=[=($t2, $t24)], expr#26=[12], " +
                        "expr#27=[=($t2, $t26)], expr#28=[13], expr#29=[=($t2, $t28)], expr#30=[14], expr#31=[=($t2, $t30)], " +
                        "expr#32=[15], expr#33=[=($t2, $t32)], expr#34=[16], expr#35=[=($t2, $t34)], expr#36=[17], " +
                        "expr#37=[=($t2, $t36)], expr#38=[18], expr#39=[=($t2, $t38)], expr#40=[19], expr#41=[=($t2, $t40)], " +
                        "expr#42=[20], expr#43=[=($t2, $t42)], expr#44=[21], expr#45=[=($t2, $t44)], expr#46=[OR($t3, $t5, $t7, " +
                        "$t9, $t11, $t13, $t15, $t17, $t19, $t21, $t23, $t25, $t27, $t29, $t31, $t33, $t35, $t37, $t39, $t41, $t43, $t45)], " +
                        "proj#0..2=[{exprs}], $condition=[$t46])\n" +
                        "  VoltLogicalTableScan(table=[[public, R3]])\n").pass();
    }

    public void testENG15245() {
        m_tester.sql("select CAST(border as VARCHAR) from R5")
                .transform("VoltLogicalCalc(expr#0..4=[{inputs}], expr#5=[CAST($t3):VARCHAR(2048) CHARACTER SET " +
                        "\"UTF-8\" COLLATE \"UTF-8$en_US$primary\"], EXPR$0=[$t5])\n" +
                        "  VoltLogicalTableScan(table=[[public, R5]])\n")
                .pass();

        m_tester.sql("select CAST(point as VARCHAR) from R5")
                .transform("VoltLogicalCalc(expr#0..4=[{inputs}], expr#5=[CAST($t4):VARCHAR(2048) CHARACTER SET " +
                        "\"UTF-8\" COLLATE \"UTF-8$en_US$primary\"], EXPR$0=[$t5])\n" +
                        "  VoltLogicalTableScan(table=[[public, R5]])\n")
                .pass();
    }

    public void testProjectionPushDown() {
        m_tester.sql("select R1.i from R1 inner join R2 on R1.v = R2.v")
                .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0])\n" +
                        "  VoltLogicalJoin(condition=[=($1, $2)], joinType=[inner])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], V=[$t5])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], V=[$t5])\n" +
                        "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }

    public void testLimit0() {
        m_tester.sql("select I from R1 limit 0")
                .transform("VoltLogicalValues(tuples=[[]])\n")
                .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn() {
        m_tester.sql("select i from R1 full join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[IS NOT NULL($t1)], expr#3=[CASE($t2, $t1, $t0)], I=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn1() {
        m_tester.sql("select max(r1.i), i from R1 full join R2 using(i) group by i")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], I=[$t0])\n" +
                    "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                    "    VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[IS NOT NULL($t6)], expr#13=[CASE($t12, $t6, $t0)], I=[$t13], $f1=[$t0])\n" +
                    "      VoltLogicalJoin(condition=[=($0, $6)], joinType=[full])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn2() {
        m_tester.sql("select max(r1.i), i from R1 full join R2 using(i) group by i having i > 0")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)], EXPR$0=[$t1], I=[$t0], $condition=[$t3])\n" +
                    "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                    "    VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[IS NOT NULL($t6)], expr#13=[CASE($t12, $t6, $t0)], I=[$t13], $f1=[$t0])\n" +
                    "      VoltLogicalJoin(condition=[=($0, $6)], joinType=[full])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn3() {
        m_tester.sql("select max(i) from R1 full join R2 using(i)")
        .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[MAX($0)])\n" +
                    "  VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[IS NOT NULL($t6)], expr#13=[CASE($t12, $t6, $t0)], $f0=[$t13])\n" +
                    "    VoltLogicalJoin(condition=[=($0, $6)], joinType=[full])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn4() {
        m_tester.sql("select r1.i from R1 full join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn5() {
        m_tester.sql("select case when r1.i is not null then r1.i else r2.i end as i "
                + " from R1 full join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[IS NOT NULL($t0)], expr#3=[CASE($t2, $t0, $t1)], I=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn6() {
        m_tester.sql("select case when R1.si > 0 is not null then i else -i end as sialias "
                + " from R1 full join R2 using(i)")
                .transform("VoltLogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t1, $t3)], expr#5=[IS NOT NULL($t4)], " +
                        "expr#6=[IS NOT NULL($t2)], expr#7=[CASE($t6, $t2, $t0)], expr#8=[-($t7)], expr#9=[CASE($t5, $t7, $t8)], SIALIAS=[$t9])\n" +
                        "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[full])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn7() {
        m_tester.sql("select RANK() OVER ( ORDER BY i) AS rnk, i "
                + "from R1 full join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[IS NOT NULL($t1)], expr#3=[CASE($t2, $t1, $t0)], " +
                    "expr#4=[RANK() OVER (ORDER BY $t3 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)], RNK=[$t4], I=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn8() {
        m_tester.sql("select I from R1 full join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[IS NOT NULL($t1)], expr#3=[CASE($t2, $t1, $t0)], I=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn9() {
        m_tester.sql("select I as ii from R1 full join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[IS NOT NULL($t1)], expr#3=[CASE($t2, $t1, $t0)], II=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn10() {
        m_tester.sql("select max(R1.si), i as ii from R1 full join R2 using(i) group by ii")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], II=[$t0])\n" +
                    "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                    "    VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[IS NOT NULL($t6)], expr#13=[CASE($t12, $t6, $t0)], II=[$t13], SI=[$t1])\n" +
                    "      VoltLogicalJoin(condition=[=($0, $6)], joinType=[full])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullUsingJoinWithAmbiguousSelectColumn11() {
        m_tester.sql("select i from R1 full join R2 using(i) where i > 0")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[IS NOT NULL($t1)], expr#3=[CASE($t2, $t1, $t0)], " +
                        "expr#4=[0], expr#5=[>($t3, $t4)], I=[$t3], $condition=[$t5])\n" +
                        "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[full])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                        "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .pass();
    }

    public void testInnerUsingJoinWithAmbiguousSelectColumn() {
        m_tester.sql("select i from R1 join R2 using(i)")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testInnerUsingJoinWithAmbiguousSelectColumn1() {
        m_tester.sql("select i from R1 join R2 using(i) order by i")
        .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "  VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t1])\n" +
                    "    VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    // Non-integral numeric literals should be parsed as DOUBLEs to represent a larger range than DECIMAL
    public void testNumericLiteralDataType() {
        // 5.0 is parsed as a DOUBLE and 2 is parsed as an INT
        // si(SMALLINT) requires a cast to double to be compared to 5.0.
        // f(FLOAT) does not need a cast because the underlying type being used for FLOAT is actually DOUBLE
        m_tester.sql("select * from R2 where si = 5.0 and i = 2 and f = 5.0")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):DOUBLE], expr#7=[5.0], expr#8=[=($t6, $t7)], " +
                    "expr#9=[2], expr#10=[=($t0, $t9)], expr#11=[5.0], expr#12=[=($t4, $t11)], expr#13=[AND($t8, $t10, $t12)], "  +
                    "proj#0..5=[{exprs}], $condition=[$t13])\n" +
                    "  VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    // Unicode characters that are not part of the 'ISO-8859-1' character set should be correctly interpreted
    public void testCharSet() {
        m_tester.sql("select * from R1 where v like '你好' or v like 'foo'")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['你好'], expr#7=[LIKE($t5, $t6)], expr#8=['foo'], " +
                    "expr#9=[LIKE($t5, $t8)], expr#10=[OR($t7, $t9)], proj#0..5=[{exprs}], $condition=[$t10])\n" +
                    "  VoltLogicalTableScan(table=[[public, R1]])\n")
        .pass();
    }

    // Test OUTER Join simplification (from FilterJoinTransposeRule)
    public void testRightToInnerJoin1() {
        m_tester.sql("select R2.i, R1.i from R1 right join R2 using(i) where R1.I = 0")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t1], I0=[$t0])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testRightToInnerJoin2() {
        m_tester.sql("select R2.i, R1.i from R1 right join R2 using(i) where R1.I = 0 and R1.bi is null")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t2], I0=[$t0])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t0, $t6)], expr#8=[IS NULL($t3)], expr#9=[AND($t7, $t8)], I=[$t0], BI=[$t3], $condition=[$t9])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testNotSimplifiedRightJoin1() {
        m_tester.sql("select R2.i, R1.i from R1 right join R2 using(i) where R1.bi is null")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], expr#3=[IS NULL($t1)], I=[$t2], I0=[$t0], $condition=[$t3])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[right])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testNotSimplifiedRightJoin2() {
        m_tester.sql("SELECT R1.I, R2.si FROM R1 RIGHT JOIN R2 ON R1.I = R2.I" +
                " where COALESCE(R1.I, 10) > 4")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], expr#3=[IS NOT NULL($t0)], expr#4=[10], expr#5=[CASE($t3, $t0, $t4)], expr#6=[4], expr#7=[>($t5, $t6)], I=[$t0], SI=[$t2], $condition=[$t7])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[right])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testLeftToInnerJoin1() {
        m_tester.sql("select R2.i, R1.i from R1 left join R2 using(i) where R2.I = 0")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t1], I0=[$t0])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testNotSimplifiedLeftJoin1() {
        m_tester.sql("select R2.i, R1.i from R1 left join R2 using(i) where R2.I = 0 or R2.si is null")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[=($t1, $t3)], expr#5=[IS NULL($t2)], expr#6=[OR($t4, $t5)], I=[$t1], I0=[$t0], $condition=[$t6])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[left])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullToRightJoin() {
        m_tester.sql("select R2.i, R1.i from R1 full join R2 using(i) where R2.si = 5")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t1], I0=[$t0])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[right])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[5], expr#8=[=($t6, $t7)], proj#0..1=[{exprs}], $condition=[$t8])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullToLeftJoin() {
        m_tester.sql("select R2.i, R1.i from R1 full join R2 using(i) where R1.si = 5")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t2], I0=[$t0])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[left])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[5], expr#8=[=($t6, $t7)], proj#0..1=[{exprs}], $condition=[$t8])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

    public void testFullToInnerJoin() {
        m_tester.sql("select R2.i, R1.i from R1 full join R2 using(i) where R1.si = 5 and R2.si = 8")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t2], I0=[$t0])\n" +
                   "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[5], expr#8=[=($t6, $t7)], proj#0..1=[{exprs}], $condition=[$t8])\n" +
                   "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                   "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[8], expr#8=[=($t6, $t7)], proj#0..1=[{exprs}], $condition=[$t8])\n" +
                   "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();
    }

}
