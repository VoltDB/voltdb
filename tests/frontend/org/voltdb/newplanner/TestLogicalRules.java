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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.CatalogAdapter;
import org.voltdb.newplanner.rules.PlannerPhase;
import org.voltdb.types.CalcitePlannerType;

public class TestLogicalRules extends PlanRulesTestCase {
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
        RelNode node = CalcitePlanner.transform(CalcitePlannerType.VOLCANO, PlannerPhase.LOGICAL,
                root.rel, logicalTraits);

        String actualPlan = RelOptUtil.toString(node);
        assertEquals(expectedPlan, actualPlan);
    }

    public void testSimpleSeqScan() {
        assertPlanMatch("select si from Ri1",
                "VoltDBLCalc(expr#0..3=[{inputs}], SI=[$t1])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RI1]])\n");
    }

    public void testSeqScan() {
        assertPlanMatch("select * from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithProjection() {
        assertPlanMatch("select i, si from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithProjectionExpr() {
        assertPlanMatch("select i * 5 from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[*($t0, $t6)], EXPR$0=[$t7])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithFilter() {
        assertPlanMatch("select i from R1 where i = 5",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithFilterParam() {
        assertPlanMatch("select i from R1 where i = ? and v = ?",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[?0], expr#7=[=($t0, $t6)], expr#8=[?1], expr#9=[=($t5, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], $condition=[$t10])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithStringFilter() {
        assertPlanMatch("select i from R1 where v = 'FOO1'",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=['FOO1'], expr#7=[=($t5, $t6)], I=[$t0], $condition=[$t7])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithFilterWithTypeConversion() {
        assertPlanMatch("select i from R1 where si = 5",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[5], expr#8=[=($t6, $t7)], I=[$t0], $condition=[$t8])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    // Partition info are introduced in the physical plan phase. In logical plan phase there is no difference.
    public void testSeqScanPartitioned() {
        assertPlanMatch("select * from P1",
                "VoltDBLCalc(expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "  VoltDBLTableScan(table=[[catalog, P1]])\n");

        assertPlanMatch("select i from P1",
                "VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "  VoltDBLTableScan(table=[[catalog, P1]])\n");
    }

    // Index info are introduced in the physical plan phase. In logical plan phase there is no difference.
    public void testIndexScan() {
        assertPlanMatch("select bi from RI1 where i > 45 and ti > 3",
                "VoltDBLCalc(expr#0..3=[{inputs}], expr#4=[45], expr#5=[>($t0, $t4)], expr#6=[3], expr#7=[>($t3, $t6)], expr#8=[AND($t5, $t7)], BI=[$t2], $condition=[$t8])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RI1]])\n");
    }

    public void testSeqScanWithLimit() {
        assertPlanMatch("select i from R1 limit 5",
                "VoltDBLLimit(limit=[5])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithLimitParam() {
        assertPlanMatch("select i from R1 limit ?",
                "VoltDBLLimit(limit=[?0])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithFilterAndLimit() {
        assertPlanMatch("select i from R1 where si > 3 limit 5",
                "VoltDBLLimit(limit=[5])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], I=[$t0], $condition=[$t7])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithOrderByAndLimitAndFilter() {
        assertPlanMatch("select si, i from RI1 where I > 3 order by si limit 3",
                "VoltDBLLimit(limit=[3])\n" +
                        "  VoltDBLSort(sort0=[$0], dir0=[ASC])\n" +
                        "    VoltDBLCalc(expr#0..3=[{inputs}], expr#4=[3], expr#5=[>($t0, $t4)], SI=[$t1], I=[$t0], $condition=[$t5])\n" +
                        "      VoltDBLTableScan(table=[[catalog, RI1]])\n");
    }

    public void testSeqScanWithOffset() {
        assertPlanMatch("select i from R1 offset 1",
                "VoltDBLLimit(offset=[1])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithLimitOffset() {
        assertPlanMatch("select i from R1 limit 5 offset 1",
                "VoltDBLLimit(limit=[5], offset=[1])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithLimitOffsetSort() {
        assertPlanMatch("select i from R1 order by bi limit 5 offset 1",
                "VoltDBLLimit(limit=[5], offset=[1])\n" +
                        "  VoltDBLSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithOrderByAndFilter() {
        assertPlanMatch("select * from R1 where si > 3 order by i",
                "VoltDBLSort(sort0=[$0], dir0=[ASC])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select i, bi, si from R1 where si > 3 order by i",
                "VoltDBLSort(sort0=[$0], dir0=[ASC])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t1, $t6)], I=[$t0], BI=[$t3], SI=[$t1], $condition=[$t7])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithOrderBy() {
        assertPlanMatch("select si from R1 order by i, si desc",
                "VoltDBLSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select bi, i, si from R1 order by i, si desc",
                "VoltDBLSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[DESC])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], BI=[$t3], I=[$t0], SI=[$t1])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithOrderByExpr() {
        assertPlanMatch("select bi, i, si from R1 order by i, si + 1 desc",
                "VoltDBLSort(sort0=[$1], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[+($t1, $t6)], BI=[$t3], I=[$t0], SI=[$t1], EXPR$3=[$t7])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testSeqScanWithOrderByAndLimit() {
        assertPlanMatch("select bi, i, si from R1 order by i limit 5",
                "VoltDBLLimit(limit=[5])\n" +
                        "  VoltDBLSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], BI=[$t3], I=[$t0], SI=[$t1])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testIndexScanWithLimitOffset() {
        assertPlanMatch("select si, i from RI1 where I > 3 limit 3 offset 4",
                "VoltDBLLimit(limit=[3], offset=[4])\n" +
                        "  VoltDBLCalc(expr#0..3=[{inputs}], expr#4=[3], expr#5=[>($t0, $t4)], SI=[$t1], I=[$t0], $condition=[$t5])\n" +
                        "    VoltDBLTableScan(table=[[catalog, RI1]])\n");
    }

    public void testConstIntExpr() {
        assertPlanMatch("select 5 from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[5], EXPR$0=[$t6])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testBinaryIntExpr() {
        assertPlanMatch("select 5 + i from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[+($t6, $t0)], EXPR$0=[$t7])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testConstBinaryIntExpr() {
        assertPlanMatch("select 5 + 5 from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[+($t6, $t6)], EXPR$0=[$t7])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testConcatStringExpr() {
        assertPlanMatch("select '55' || '22' from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=['55'], expr#7=['22'], expr#8=[||($t6, $t7)], EXPR$0=[$t8])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select v || '22' from R1",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=['22'], expr#7=[||($t5, $t6)], EXPR$0=[$t7])\n" +
                        "  VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testDatetimeConstExpr() {
        assertPlanMatch("select TIMESTAMP '1969-07-20 20:17:40' from RTYPES",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[1969-07-20 20:17:40], EXPR$0=[$t9])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");
    }

    public void testBinaryDatetimeExpr() {
        assertPlanMatch("select ts - INTERVAL '1' DAY from RTYPES",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[86400000], expr#10=[-($t6, $t9)], EXPR$0=[$t10])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");
    }

    public void testCompareInExpr() {
        // Calcite transforms the IN expression into ORs
        assertPlanMatch("select 1 from RTYPES where i IN (1, 2)",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[=($t3, $t9)], expr#11=[2], expr#12=[=($t3, $t11)], expr#13=[OR($t10, $t12)], EXPR$0=[$t9], $condition=[$t13])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");

        assertPlanMatch("select 1 from RTYPES where i IN (1, ?, 3)",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[=($t3, $t9)], expr#11=[?0], expr#12=[=($t3, $t11)], expr#13=[3], expr#14=[=($t3, $t13)], expr#15=[OR($t10, $t12, $t14)], EXPR$0=[$t9], $condition=[$t15])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");
    }

    public void testCompareLikeExpr() {
        assertPlanMatch("select 1 from RTYPES where vc LIKE 'ab%c'",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=['ab%c'], expr#11=[LIKE($t8, $t10)], EXPR$0=[$t9], $condition=[$t11])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");

        assertPlanMatch("select 1 from RTYPES where vc LIKE ?",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[1], expr#10=[?0], expr#11=[LIKE($t8, $t10)], EXPR$0=[$t9], $condition=[$t11])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");
    }

    public void testAbsExpr() {
        assertPlanMatch("select abs(i) from RTYPES",
                "VoltDBLCalc(expr#0..8=[{inputs}], expr#9=[ABS($t3)], EXPR$0=[$t9])\n" +
                        "  VoltDBLTableScan(table=[[catalog, RTYPES]])\n");
    }

    public void testAggr() {
        assertPlanMatch("select avg(ti) from R1",
                "VoltDBLAggregate(group=[{}], EXPR$0=[AVG($0)])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], TI=[$t2])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select avg(ti) from R1 group by i",
                "VoltDBLCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltDBLAggregate(group=[{0}], EXPR$0=[AVG($1)])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select count(i) from R1 where ti > 3",
                "VoltDBLAggregate(group=[{}], EXPR$0=[COUNT($0)])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t2, $t6)], I=[$t0], $condition=[$t7])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select count(*) from R1",
                "VoltDBLAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[0], $f0=[$t6])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select max(TI) from R1 group by SI having SI > 0",
                "VoltDBLCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)], EXPR$0=[$t1], $condition=[$t3])\n" +
                        "  VoltDBLAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], SI=[$t1], TI=[$t2])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select max(TI), SI, min(TI), I from R1 group by SI, I having avg(BI) > max(BI)",
                "VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[>($t4, $t5)], EXPR$0=[$t2], SI=[$t0], EXPR$2=[$t3], I=[$t1], $condition=[$t6])\n" +
                        "  VoltDBLAggregate(group=[{0, 1}], EXPR$0=[MAX($2)], EXPR$2=[MIN($2)], agg#2=[AVG($3)], agg#3=[MAX($3)])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0], TI=[$t2], BI=[$t3])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select max(TI), SI, I, min(TI) from R1 group by I, SI having avg(BI) > 0 and si > 0",
                "VoltDBLCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[>($t4, $t5)], expr#7=[>($t1, $t5)], expr#8=[AND($t6, $t7)], EXPR$0=[$t2], SI=[$t1], I=[$t0], EXPR$3=[$t3], $condition=[$t8])\n" +
                        "  VoltDBLAggregate(group=[{0, 1}], EXPR$0=[MAX($2)], EXPR$3=[MIN($2)], agg#2=[AVG($3)])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select max(TI), SI from R1 where I > 0 group by SI, I limit 3",
                "VoltDBLLimit(limit=[3])\n" +
                        "  VoltDBLCalc(expr#0..2=[{inputs}], EXPR$0=[$t2], SI=[$t0])\n" +
                        "    VoltDBLAggregate(group=[{0, 1}], EXPR$0=[MAX($2)])\n" +
                        "      VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t0, $t6)], SI=[$t1], I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                        "        VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select max(TI), SI from R1 where I > 0 group by SI, I order by SI limit 3",
                "VoltDBLLimit(limit=[3])\n" +
                        "  VoltDBLSort(sort0=[$1], dir0=[ASC])\n" +
                        "    VoltDBLCalc(expr#0..2=[{inputs}], EXPR$0=[$t2], SI=[$t0])\n" +
                        "      VoltDBLAggregate(group=[{0, 1}], EXPR$0=[MAX($2)])\n" +
                        "        VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[>($t0, $t6)], SI=[$t1], I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                        "          VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testDistinct() {
        assertPlanMatch("select distinct TI, I from R1",
                "VoltDBLAggregate(group=[{0, 1}])\n" +
                        "  VoltDBLCalc(expr#0..5=[{inputs}], TI=[$t2], I=[$t0])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select distinct max(TI) from R1 group by I",
                "VoltDBLAggregate(group=[{0}])\n" +
                        "  VoltDBLCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "    VoltDBLAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                        "      VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "        VoltDBLTableScan(table=[[catalog, R1]])\n");

        assertPlanMatch("select max (distinct (TI)) from R1 group by I",
                "VoltDBLCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltDBLAggregate(group=[{0}], EXPR$0=[MAX(DISTINCT $1)])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n");
    }

    public void testJoin() {
        assertPlanMatch("select R1.i, R2.v from R1, R2 " +
                        "where R2.si = R1.i and R2.v = 'foo'",
                "VoltDBLCalc(expr#0..11=[{inputs}], expr#12=[CAST($t7):INTEGER], expr#13=[=($t12, $t0)], expr#14=['foo'], expr#15=[=($t11, $t14)], expr#16=[AND($t13, $t15)], I=[$t0], V=[$t11], $condition=[$t16])\n" +
                        "  VoltDBLJoin(condition=[true], joinType=[inner])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R2]])\n");

        assertPlanMatch("select R1.i, R2.v from R1 inner join R2 " +
                        "on R2.si = R1.i where R2.v = 'foo'",
                "VoltDBLCalc(expr#0..12=[{inputs}], expr#13=['foo'], expr#14=[=($t11, $t13)], I=[$t0], V=[$t11], $condition=[$t14])\n" +
                        "  VoltDBLJoin(condition=[=($12, $0)], joinType=[inner])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], proj#0..6=[{exprs}])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R2]])\n");

        assertPlanMatch("select R2.si, R1.i from R1 inner join " +
                        "R2 on R2.i = R1.si where R2.v = 'foo' and R1.si > 4 and R1.ti > R2.i",
                "VoltDBLCalc(expr#0..12=[{inputs}], expr#13=['foo'], expr#14=[=($t12, $t13)], expr#15=[4], expr#16=[>($t1, $t15)], expr#17=[>($t2, $t7)], expr#18=[AND($t14, $t16, $t17)], SI=[$t8], I=[$t0], $condition=[$t18])\n" +
                        "  VoltDBLJoin(condition=[=($7, $6)], joinType=[inner])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], proj#0..6=[{exprs}])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R2]])\n");

        assertPlanMatch("select R1.i from R1 inner join " +
                        "R2  on R1.si = R2.si where R1.I + R2.ti = 5",
                "VoltDBLCalc(expr#0..11=[{inputs}], expr#12=[+($t0, $t8)], expr#13=[5], expr#14=[=($t12, $t13)], I=[$t0], $condition=[$t14])\n" +
                        "  VoltDBLJoin(condition=[=($1, $7)], joinType=[inner])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R1]])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R2]])\n");
    }

    public void testThreeWayJoin() {
        assertPlanMatch("select R1.i from R1 inner join " +
                        "R2  on R1.si = R2.i inner join " +
                        "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'",
                "VoltDBLCalc(expr#0..15=[{inputs}], expr#16=[4], expr#17=[>($t1, $t16)], expr#18=['foo'], expr#19=[<>($t14, $t18)], expr#20=[AND($t17, $t19)], I=[$t0], $condition=[$t20])\n" +
                        "  VoltDBLJoin(condition=[=($12, $14)], joinType=[inner])\n" +
                        "    VoltDBLCalc(expr#0..12=[{inputs}], expr#13=[CAST($t12):VARCHAR(256) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"], proj#0..5=[{exprs}], I0=[$t7], SI1=[$t8], TI0=[$t9], BI0=[$t10], F0=[$t11], V0=[$t12], V00=[$t13])\n" +
                        "      VoltDBLJoin(condition=[=($6, $7)], joinType=[inner])\n" +
                        "        VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], proj#0..6=[{exprs}])\n" +
                        "          VoltDBLTableScan(table=[[catalog, R1]])\n" +
                        "        VoltDBLTableScan(table=[[catalog, R2]])\n" +
                        "    VoltDBLTableScan(table=[[catalog, R3]])\n");
    }

    public void testSubqueriesJoin() {
        assertPlanMatch("select t1.v, t2.v "
                        + "from "
                        + "  (select * from R1 where v = 'foo') as t1 "
                        + "  inner join "
                        + "  (select * from R2 where f = 30.3) as t2 "
                        + "on t1.i = t2.i "
                        + "where t1.i = 3",
                "VoltDBLCalc(expr#0..11=[{inputs}], expr#12=[3], expr#13=[=($t0, $t12)], V=[$t5], V0=[$t11], $condition=[$t13])\n" +
                        "  VoltDBLJoin(condition=[=($0, $6)], joinType=[inner])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R1]])\n" +
                        "    VoltDBLCalc(expr#0..5=[{inputs}], expr#6=[CAST($t4):DOUBLE NOT NULL], expr#7=[30.3], expr#8=[=($t6, $t7)], proj#0..5=[{exprs}], $condition=[$t8])\n" +
                        "      VoltDBLTableScan(table=[[catalog, R2]])\n");
    }
}
