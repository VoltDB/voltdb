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

import org.voltdb.plannerv2.rules.PlannerRules;

public class TestPhysicalMergeJoin extends Plannerv2TestCase {

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

    public void testMergeJoinUniqueIndex() {
        // MJ using PRIMARY / UNIQUE indexes
        m_tester.sql("SELECT RI1.i FROM RI1 INNER JOIN RI2 ON RI1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($0, $1)], joinType=[inner], " +
                        "outerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin1() {
        // TODO: ambiguous plan generated
        m_tester.sql("select RI1.ti from RI1 inner join RI2 on RI1.TI = RI2.TI where RI1.si > 0")
                // inner index on RI2_IND1
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($1, $2)], joinType=[inner], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t1, $t4)], SI=[$t1], TI=[$t3], $condition=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND1_ASCEQ0_0])\n")
                // inner index on RI2_IND5_HASH
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($1, $2)], joinType=[inner], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t1, $t4)], SI=[$t1], TI=[$t3], $condition=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin2() {
        // TODO: plan ambiguity
        m_tester.sql("select RI1.ti from RI1 inner join RI2 on RI1.TI = RI2.TI where RI2.si > 0")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t0])\n" +
                            "  VoltPhysicalMergeJoin(condition=[=($0, $2)], joinType=[inner], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], TI=[$t3])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t1, $t4)], SI=[$t1], TI=[$t3], $condition=[$t5])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND1_ASCEQ0_0])\n")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t0])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($0, $2)], joinType=[inner], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t1, $t4)], SI=[$t1], TI=[$t3], $condition=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin3() {
        // RI3.II is a HASH index  but since HASH indexes are actually replaced with BINARY TREE types
        // a NLIJ with HASH index looses to a MJ with a regular scan index
        m_tester.sql("select RI1.ti from RI1 inner join RI3 on RI3.II = RI1.I")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($2, $0)], joinType=[inner], " +
                        "outerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I], innerIndex=[RI3_IND1_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], II=[$t2])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI3]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[RI3_IND1_HASH_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin4() {
        // For a MJ the unique RI4_IND1 and the non-unique RI4_IND2 indexes should have the same cost (M + N)
        m_tester.sql("select RI1.ti from RI1 inner join RI4 on RI1.I = RI4.I")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($0, $2)], joinType=[inner], " +
                        "outerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I], innerIndex=[RI4_IND1])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI4]], expr#0..1=[{inputs}], " +
                        "proj#0..1=[{exprs}], index=[RI4_IND1_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin5() {
        m_tester.sql("select RI1.ti from RI1 inner join RI2 on RI1.I = RI2.I offset 4")
                .transform("VoltPhysicalLimit(offset=[4], pusheddown=[false])\n" +
                        "  VoltPhysicalCalc(expr#0..2=[{inputs}], TI=[$t1])\n" +
                        "    VoltPhysicalMergeJoin(condition=[=($0, $2)], joinType=[inner], " +
                        "outerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "      VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "        VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_ASCEQ0_0])\n" +
                        "      VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "        VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin6() {
        m_tester.sql("select RI2.I from RI2 inner join RI1 on RI2.I = RI1.BI and RI2.BI = RI1.SI")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], I=[$t0])\n" +
                        "  VoltPhysicalMergeJoin(condition=[AND(=($2, $3), =($1, $4))], joinType=[inner], " +
                        "outerIndex=[RI2_IND2], innerIndex=[RI1_IND2])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t0):BIGINT], I=[$t0], BI=[$t2], " +
                        "I0=[$t4])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[RI2_IND2_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t1):BIGINT], BI=[$t2], SI0=[$t4])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[RI1_IND2_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoin7() {
        // MJ  RI2_IND2 ON RI2 (i, bi) covers RI1.BI = RI2.I
        m_tester.sql("SELECT RI2.BI FROM RI1 INNER JOIN RI2  ON RI1.BI = RI2.I")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], BI0=[$t1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($0, $2)], joinType=[inner], " +
                        "outerIndex=[RI1_IND2], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], BI=[$t2])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[RI1_IND2_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t0):BIGINT], BI=[$t2], I0=[$t4])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_ASCEQ0_0])\n")
                .pass();
    }

    public void testReplicatedMergeJoinNotAplicable() {
        // NLIJ because  RI1.TI > RI3.PK gets pushed to the ON condition and the join is not an equi one anymore.
        m_tester.sql("select RI1.ti from RI1 inner join RI3 on RI1.I = RI3.II where RI1.TI > RI3.PK")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], TI=[$t1])\n" +
                            "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $3), >($1, $2))], joinType=[inner], innerIndex=[RI3_IND1_HASH])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], PK=[$t0], II=[$t2])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI3]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI3_IND1_HASH_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testReplicatedMergeJoinNotAplicable1() {
        // Not an equi join.
        m_tester.sql("select RI1.ti from RI1 inner join RI3 on RI1.I = RI3.II and RI1.TI > RI3.PK")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], TI=[$t1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $3), >($1, $2))], joinType=[inner], " +
                        "innerIndex=[RI3_IND1_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, RI1]], " +
                        "expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], PK=[$t0], II=[$t2])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI3]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[RI3_IND1_HASH_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testReplicatedMergeJoinNotAplicable2() {
        // NLIJ because non of the indexes covers the whole predicate
        m_tester.sql("select RI2.I from RI2 inner join RI3 on RI2.I = RI3.II and RI2.BI = RI3.PK")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[inner], " +
                        "innerIndex=[RI3_IND1_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], BI=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, RI2]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t0):BIGINT], II=[$t2], " +
                        "PK0=[$t4])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI3]], expr#0..3=[{inputs}], " +
                        "proj#0..3=[{exprs}], index=[RI3_IND1_HASH_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testReplicatedMergeJoinNotAplicable3() {
        // NLIJ. RI1 and RI2 index collations do not match
        // RI1_IND2 ON RI1 (bi, si);
        // RI2_IND2 ON RI2 (i, bi); --- the order is reversed BI and I
        m_tester.sql("select RI2.I from RI2 inner join RI1 on RI2.BI = RI1.BI and RI2.I = RI1.SI")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t2])\n" +
                            "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($3, $0), =($2, $1))], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t1):INTEGER], BI=[$t2], SI0=[$t4])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], BI=[$t2])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testReplicatedMergeJoinNotAplicable4() {
        // NLIJ. RI2_IND2 ON RI2 (i, bi) does not cover RI1.BI = RI2.I
        m_tester.sql("SELECT RI2.BI FROM RI1 INNER JOIN RI2  ON RI1.BI = RI2.BI")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], BI=[$t0])\n" +
                            "  VoltPhysicalNestLoopIndexJoin(condition=[=($1, $0)], joinType=[inner], innerIndex=[RI1_IND2])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], BI=[$t2])\n" +
                            "      VoltPhysicalTableSequentialScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                            "    VoltPhysicalCalc(expr#0..3=[{inputs}], BI=[$t2])\n" +
                            "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND2_INVALIDGTE1_1])\n")
                .pass();
    }
}
