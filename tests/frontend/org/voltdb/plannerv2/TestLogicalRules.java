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

import org.apache.calcite.plan.RelTraitSet;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rules.PlannerRules.Phase;

public class TestLogicalRules extends Plannerv2TestCase {

    TransformationTester m_tester = new TransformationTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        RelTraitSet requiredOutputTraits = getEmptyTraitSet().replace(VoltLogicalRel.CONVENTION);
        m_tester.traitSet(requiredOutputTraits)
                .phase(Phase.LOGICAL);
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSimpleSeqScan() {
        m_tester.sql("select si from Ri1")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], SI=[$t1])\n" +
                           "  VoltLogicalTableScan(table=[[public, RI1]])\n")
                .test();
    }

    public void testSeqScan() {
        m_tester.sql("select * from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithProjection() {
        m_tester.sql("select i, si from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithProjectionExpr() {
        m_tester.sql("select i * 5 from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[*($t0, $t6)], EXPR$0=[$t7])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithFilter() {
        m_tester.sql("select i from R1 where i = 5")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithFilterParam() {
        m_tester.sql("select i from R1 where i = ? and v = ?")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[?0], expr#7=[=($t0, $t6)], expr#8=[?1], expr#9=[=($t5, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], $condition=[$t10])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithStringFilter() {
        m_tester.sql("select i from R1 where v = 'FOO1'")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['FOO1'], expr#7=[=($t5, $t6)], I=[$t0], $condition=[$t7])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithFilterWithTypeConversion() {
        m_tester.sql("select i from R1 where si = 5")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[5], expr#8=[=($t6, $t7)], I=[$t0], $condition=[$t8])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    // Partition info are introduced in the physical plan phase. In logical plan phase there is no difference.
    public void testSeqScanPartitioned() {
        m_tester.sql("select * from P1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                           "  VoltLogicalTableScan(table=[[public, P1]])\n")
                .test();

        m_tester.sql("select i from P1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                           "  VoltLogicalTableScan(table=[[public, P1]])\n")
                .test();
    }

    // Index info are introduced in the physical plan phase. In logical plan phase there is no difference.
    public void testIndexScan() {
        m_tester.sql("select bi from RI1 where i > 45 and ti > 3")
                .transform("VoltLogicalCalc(expr#0..3=[{inputs}], expr#4=[45], expr#5=[>($t0, $t4)], expr#6=[3], expr#7=[>($t3, $t6)], expr#8=[AND($t5, $t7)], BI=[$t2], $condition=[$t8])\n" +
                           "  VoltLogicalTableScan(table=[[public, RI1]])\n")
                .test();
    }

    public void testSeqScanWithLimit() {
        m_tester.sql("select i from R1 limit 5")
                .transform("VoltLogicalLimit(limit=[5])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithLimitParam() {
        m_tester.sql("select i from R1 limit ?")
                .transform("VoltLogicalLimit(limit=[?0])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithFilterAndLimit() {
        m_tester.sql("select i from R1 where si > 3 limit 5")
                .transform("VoltLogicalLimit(limit=[5])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], I=[$t0], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithOrderByAndLimitAndFilter() {
        m_tester.sql("select si, i from RI1 where I > 3 order by si limit 3")
                .transform("VoltLogicalLimit(limit=[3])\n" +
                           "  VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                           "    VoltLogicalCalc(expr#0..3=[{inputs}], expr#4=[3], expr#5=[>($t0, $t4)], SI=[$t1], I=[$t0], $condition=[$t5])\n" +
                           "      VoltLogicalTableScan(table=[[public, RI1]])\n")
                .test();
    }

    public void testSeqScanWithOffset() {
        m_tester.sql("select i from R1 offset 1")
                .transform("VoltLogicalLimit(offset=[1])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithLimitOffset() {
        m_tester.sql("select i from R1 limit 5 offset 1")
                .transform("VoltLogicalLimit(limit=[5], offset=[1])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithLimitOffsetSort() {
        m_tester.sql("select i from R1 order by bi limit 5 offset 1")
                .transform("VoltLogicalLimit(limit=[5], offset=[1])\n" +
                           "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithOrderByAndFilter() {
        m_tester.sql("select * from R1 where si > 3 order by i")
                .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select i, bi, si from R1 where si > 3 order by i")
                .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], I=[$t0], BI=[$t3], SI=[$t1], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithOrderBy() {
        m_tester.sql("select si from R1 order by i, si desc")
                .transform("VoltLogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select bi, i, si from R1 order by i, si desc")
                .transform("VoltLogicalSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[DESC])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], BI=[$t3], I=[$t0], SI=[$t1])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithOrderByExpr() {
        m_tester.sql("select bi, i, si from R1 order by i, si + 1 desc")
                .transform("VoltLogicalSort(sort0=[$1], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[+($t1, $t6)], BI=[$t3], I=[$t0], SI=[$t1], EXPR$3=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testSeqScanWithOrderByAndLimit() {
        m_tester.sql("select bi, i, si from R1 order by i limit 5")
                .transform("VoltLogicalLimit(limit=[5])\n" +
                           "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], BI=[$t3], I=[$t0], SI=[$t1])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testIndexScanWithLimitOffset() {
        m_tester.sql("select si, i from RI1 where I > 3 limit 3 offset 4")
                .transform("VoltLogicalLimit(limit=[3], offset=[4])\n" +
                           "  VoltLogicalCalc(expr#0..3=[{inputs}], expr#4=[3], expr#5=[>($t0, $t4)], SI=[$t1], I=[$t0], $condition=[$t5])\n" +
                           "    VoltLogicalTableScan(table=[[public, RI1]])\n")
                .test();
    }

    public void testConstIntExpr() {
        m_tester.sql("select 5 from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], EXPR$0=[$t6])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testBinaryIntExpr() {
        m_tester.sql("select 5 + i from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[+($t6, $t0)], EXPR$0=[$t7])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testConstBinaryIntExpr() {
        m_tester.sql("select 5 + 5 from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[+($t6, $t6)], EXPR$0=[$t7])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testConcatStringExpr() {
        m_tester.sql("select '55' || '22' from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['55'], expr#7=['22'], expr#8=[||($t6, $t7)], EXPR$0=[$t8])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select v || '22' from R1")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['22'], expr#7=[||($t5, $t6)], EXPR$0=[$t7])\n" +
                           "  VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testDatetimeConstExpr() {
        m_tester.sql("select TIMESTAMP '1969-07-20 20:17:40' from RTYPES")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1969-07-20 20:17:40], EXPR$0=[$t9])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();
    }

    public void testBinaryDatetimeExpr() {
        m_tester.sql("select ts - INTERVAL '1' DAY from RTYPES")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[86400000], expr#10=[-($t6, $t9)], EXPR$0=[$t10])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();
    }

    public void testCompareInExpr() {
        // Calcite transforms the IN expression into ORs
        m_tester.sql("select 1 from RTYPES where i IN (1, 2)")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[=($t3, $t9)], expr#11=[2], expr#12=[=($t3, $t11)], expr#13=[OR($t10, $t12)], EXPR$0=[$t9], $condition=[$t13])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();

        m_tester.sql("select 1 from RTYPES where i IN (1, ?, 3)")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[=($t3, $t9)], expr#11=[?0], expr#12=[=($t3, $t11)], expr#13=[3], expr#14=[=($t3, $t13)], expr#15=[OR($t10, $t12, $t14)], EXPR$0=[$t9], $condition=[$t15])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();
    }

    public void testCompareLikeExpr() {
        m_tester.sql("select 1 from RTYPES where vc LIKE 'ab%c'")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=['ab%c'], expr#11=[LIKE($t8, $t10)], EXPR$0=[$t9], $condition=[$t11])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();

        m_tester.sql("select 1 from RTYPES where vc LIKE ?")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[?0], expr#11=[LIKE($t8, $t10)], EXPR$0=[$t9], $condition=[$t11])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();
    }

    public void testAbsExpr() {
        m_tester.sql("select abs(i) from RTYPES")
                .transform("VoltLogicalCalc(expr#0..8=[{inputs}], expr#9=[ABS($t3)], EXPR$0=[$t9])\n" +
                           "  VoltLogicalTableScan(table=[[public, RTYPES]])\n")
                .test();
    }

    public void testAggr() {
        m_tester.sql("select avg(ti) from R1")
                .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[AVG($0)])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], TI=[$t2])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select avg(ti) from R1 group by i")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                           "  VoltLogicalAggregate(group=[{0}], EXPR$0=[AVG($1)])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select count(i) from R1 where ti > 3")
                .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t2, $t6)], I=[$t0], $condition=[$t7])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select count(*) from R1")
                .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], $f0=[$t6])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select max(TI) from R1 group by SI having SI > 0")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)], EXPR$0=[$t1], $condition=[$t3])\n" +
                           "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], TI=[$t2])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select max(TI), SI, min(TI), I from R1 group by SI, I having avg(BI) > max(BI)")
                .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[>($t4, $t5)], EXPR$0=[$t2], SI=[$t0], EXPR$2=[$t3], I=[$t1], $condition=[$t6])\n" +
                           "  VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)], EXPR$2=[MIN($2)], agg#2=[AVG($3)], agg#3=[MAX($3)])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0], TI=[$t2], BI=[$t3])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select max(TI), SI, I, min(TI) from R1 group by I, SI having avg(BI) > 0 and si > 0")
                .transform("VoltLogicalCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[>($t4, $t5)], expr#7=[>($t1, $t5)], expr#8=[AND($t6, $t7)], EXPR$0=[$t2], SI=[$t1], I=[$t0], EXPR$3=[$t3], $condition=[$t8])\n" +
                           "  VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)], EXPR$3=[MIN($2)], agg#2=[AVG($3)])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..3=[{exprs}])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select max(TI), SI from R1 where I > 0 group by SI, I limit 3")
                .transform("VoltLogicalLimit(limit=[3])\n" +
                           "  VoltLogicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2], SI=[$t0])\n" +
                           "    VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)])\n" +
                           "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t0, $t6)], SI=[$t1], I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                           "        VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select max(TI), SI from R1 where I > 0 group by SI, I order by SI limit 3")
                .transform("VoltLogicalLimit(limit=[3])\n" +
                           "  VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                           "    VoltLogicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2], SI=[$t0])\n" +
                           "      VoltLogicalAggregate(group=[{0, 1}], EXPR$0=[MAX($2)])\n" +
                           "        VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t0, $t6)], SI=[$t1], I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                           "          VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testDistinct() {
        m_tester.sql("select distinct TI, I from R1")
                .transform("VoltLogicalAggregate(group=[{0, 1}])\n" +
                           "  VoltLogicalCalc(expr#0..5=[{inputs}], TI=[$t2], I=[$t0])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select distinct max(TI) from R1 group by I")
                .transform("VoltLogicalAggregate(group=[{0}])\n" +
                           "  VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                           "    VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                           "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                           "        VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();

        m_tester.sql("select max (distinct (TI)) from R1 group by I")
                .transform("VoltLogicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                           "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX(DISTINCT $1)])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n")
                .test();
    }

    public void testJoin() {
        m_tester.sql("select R1.i, R2.v from R1, R2 where R2.si = R1.i and R2.v = 'foo'")
                .transform("VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[CAST($t7):INTEGER], expr#13=[=($t12, $t0)], expr#14=['foo'], expr#15=[=($t11, $t14)], expr#16=[AND($t13, $t15)], I=[$t0], V=[$t11], $condition=[$t16])\n" +
                           "  VoltLogicalJoin(condition=[true], joinType=[inner])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .test();

        m_tester.sql("select R1.i, R2.v from R1 inner join R2 on R2.si = R1.i where R2.v = 'foo'")
                .transform("VoltLogicalCalc(expr#0..12=[{inputs}], expr#13=['foo'], expr#14=[=($t11, $t13)], I=[$t0], V=[$t11], $condition=[$t14])\n" +
                           "  VoltLogicalJoin(condition=[=($12, $0)], joinType=[inner])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], proj#0..6=[{exprs}])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .test();

        m_tester.sql("select R2.si, R1.i from R1 inner join " +
                        "R2 on R2.i = R1.si where R2.v = 'foo' and R1.si > 4 and R1.ti > R2.i")
                .transform("VoltLogicalCalc(expr#0..12=[{inputs}], expr#13=['foo'], expr#14=[=($t12, $t13)], expr#15=[4], expr#16=[>($t1, $t15)], expr#17=[>($t2, $t7)], expr#18=[AND($t14, $t16, $t17)], SI=[$t8], I=[$t0], $condition=[$t18])\n" +
                           "  VoltLogicalJoin(condition=[=($7, $6)], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], proj#0..6=[{exprs}])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .test();

        m_tester.sql("select R1.i from R1 inner join " +
                        "R2  on R1.si = R2.si where R1.I + R2.ti = 5")
                .transform("VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[+($t0, $t8)], expr#13=[5], expr#14=[=($t12, $t13)], I=[$t0], $condition=[$t14])\n" +
                           "  VoltLogicalJoin(condition=[=($1, $7)], joinType=[inner])\n" +
                           "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalTableScan(table=[[public, R2]])\n")
                .test();
    }

    public void testThreeWayJoin() {
        m_tester.sql("select R1.i from R1 inner join " +
                        "R2  on R1.si = R2.i inner join " +
                        "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'")
                .transform("VoltLogicalCalc(expr#0..15=[{inputs}], expr#16=[4], expr#17=[>($t1, $t16)], expr#18=['foo'], expr#19=[<>($t14, $t18)], expr#20=[AND($t17, $t19)], I=[$t0], $condition=[$t20])\n" +
                           "  VoltLogicalJoin(condition=[=($12, $14)], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..12=[{inputs}], expr#13=[CAST($t12):VARCHAR(256) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], proj#0..5=[{exprs}], I0=[$t7], SI1=[$t8], TI0=[$t9], BI0=[$t10], F0=[$t11], V0=[$t12], V00=[$t13])\n" +
                           "      VoltLogicalJoin(condition=[=($6, $7)], joinType=[inner])\n" +
                           "        VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], proj#0..6=[{exprs}])\n" +
                           "          VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "        VoltLogicalTableScan(table=[[public, R2]])\n" +
                           "    VoltLogicalTableScan(table=[[public, R3]])\n")
                .test();
    }

    public void testSubqueriesJoin() {
        m_tester.sql("select t1.v, t2.v "
                        + "from "
                        + "  (select * from R1 where v = 'foo') as t1 "
                        + "  inner join "
                        + "  (select * from R2 where f = 30.3) as t2 "
                        + "on t1.i = t2.i "
                        + "where t1.i = 3")
                .transform("VoltLogicalCalc(expr#0..11=[{inputs}], expr#12=[3], expr#13=[=($t0, $t12)], V=[$t5], V0=[$t11], $condition=[$t13])\n" +
                           "  VoltLogicalJoin(condition=[=($0, $6)], joinType=[inner])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                           "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                           "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t4):DOUBLE NOT NULL], expr#7=[30.3], expr#8=[=($t6, $t7)], proj#0..5=[{exprs}], $condition=[$t8])\n" +
                           "      VoltLogicalTableScan(table=[[public, R2]])\n")
                .test();
    }
}
