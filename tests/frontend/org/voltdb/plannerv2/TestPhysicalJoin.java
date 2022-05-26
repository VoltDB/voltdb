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
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($1, $0)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testNLIJ2() {
        m_tester.sql("SELECT * FROM RI1 INNER JOIN R2 ON RI1.i = R2.i")
                .transform("VoltPhysicalCalc(expr#0..9=[{inputs}], I=[$t6], SI=[$t7], BI=[$t8], TI=[$t9], I0=[$t0], SI0=[$t1], TI0=[$t2], BI0=[$t3], F=[$t4], V=[$t5])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($6, $0)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testNLIJ3() {
        m_tester.sql("SELECT RI1.i FROM R2 INNER JOIN RI1 ON RI1.i = R2.i")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($1, $0)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testMultiTableNLIJ() {
        m_tester.sql("SELECT R1.si, R2.bi, RI1.ti FROM R1 INNER JOIN R2 ON R1.i = R2.i "
                + "INNER JOIN RI1 ON RI1.I = R1.I")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1], BI=[$t3], TI=[$t5])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($4, $0)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalNestLoopJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                        "      VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testMultiTableNLIJ1() {
        m_tester.sql("SELECT R1.si, R2.bi, RI1.ti FROM R1 INNER JOIN RI1 ON R1.si = RI1.si "
                + "INNER JOIN R2 ON R2.I = RI1.I")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t5], BI=[$t1], TI=[$t4])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($5, $3)], joinType=[inner])\n" +
                        "    VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "      VoltPhysicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], TI=[$t3])\n" +
                        "        VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    // There test case differs from the previous one that the first join condition
    // R1.i = RI1.si require a CAST expression (SMALL INT = (CAST) INT)
    // and that forces a Project node in between the Joins
    //              Join 2
    //        Project  Scan
    //          Join 1
    // and the VoltPJoinPushThroughJoinRule is not firing
    public void testNoMultiTableNLIJ() {
        m_tester.sql("SELECT R1.si, R2.bi, RI1.ti FROM R1 INNER JOIN RI1 ON R1.i = RI1.si "
                + "INNER JOIN R2 ON R2.I = RI1.I")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], SI=[$t0], BI=[$t4], TI=[$t2])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($3, $1)], joinType=[inner])\n" +
                        "    VoltPhysicalCalc(expr#0..4=[{inputs}], SI=[$t1], I0=[$t2], TI0=[$t3])\n" +
                        "      VoltPhysicalNestLoopJoin(condition=[=($0, $4)], joinType=[inner])\n" +
                        "        VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "          VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "        VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t1):INTEGER], I=[$t0], TI=[$t3], SI0=[$t4])\n" +
                        "          VoltPhysicalTableSequentialScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testMultiTableNLIJ3() {
        m_tester.sql("SELECT * FROM R1 INNER JOIN R2 ON R1.i = R2.i "
                + "INNER JOIN RI1 ON RI1.I = R1.I")
                .transform("VoltPhysicalCalc(expr#0..15=[{inputs}], proj#0..15=[{exprs}])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($12, $0)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalNestLoopJoin(condition=[=($0, $6)], joinType=[inner])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    // Too many JOINS ( > 5) to apply Join commute rules
    public void testTooManyMultiTableNLJ() {
        m_tester.sql("SELECT R1.si, R2.bi, RI1.ti FROM RI1 "
                + "INNER JOIN R1 ON R1.i = RI1.i "
                + "INNER JOIN R2 ON R2.I = RI1.I "
                + "INNER JOIN R3 ON R3.PK = R2.I "
                + "INNER JOIN R4 ON R4.PK = R3.PK "
                + "INNER JOIN R5 ON R5.PK = R4.PK "
                + "INNER JOIN RI3 ON RI3.PK = R4.PK ")
                .transform("VoltPhysicalCalc(expr#0..9=[{inputs}], SI=[$t3], BI=[$t5], TI=[$t1])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($9, $7)], joinType=[inner])\n" +
                        "    VoltPhysicalNestLoopJoin(condition=[=($8, $7)], joinType=[inner])\n" +
                        "      VoltPhysicalNestLoopJoin(condition=[=($7, $6)], joinType=[inner])\n" +
                        "        VoltPhysicalNestLoopJoin(condition=[=($6, $4)], joinType=[inner])\n" +
                        "          VoltPhysicalNestLoopJoin(condition=[=($4, $0)], joinType=[inner])\n" +
                        "            VoltPhysicalNestLoopJoin(condition=[=($2, $0)], joinType=[inner])\n" +
                        "              VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "                VoltPhysicalTableSequentialScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "              VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "                VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "            VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3])\n" +
                        "              VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "          VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0])\n" +
                        "            VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "        VoltPhysicalCalc(expr#0..3=[{inputs}], PK=[$t0])\n" +
                        "          VoltPhysicalTableSequentialScan(table=[[public, R4]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "      VoltPhysicalCalc(expr#0..4=[{inputs}], PK=[$t0])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R5]], expr#0..4=[{inputs}], proj#0..4=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], PK=[$t0])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, RI3]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 LEFT JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[left], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 FULL JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[full], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullMJ() {
        m_tester.sql("SELECT RI1.SI, RI2.I FROM RI1 FULL JOIN RI2 ON RI2.TI = RI1.TI")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND1_ASCEQ0_0])\n")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .pass();
    }

    public void testFullNLIJWithPredicates1() {
        // The r1.i < 2 must stay at the join level - full join
        m_tester.sql("select r1.i from R1 full join ri3 on r1.i = ri3.ii and ri3.ii > 4 and r1.i < 2")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $2), $3, $1)], joinType=[full], innerIndex=[RI3_IND1_HASH], postPredicate=[$1])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[2], expr#7=[<($t0, $t6)], I=[$t0], $f6=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[4], expr#5=[>($t2, $t4)], II=[$t2], $f4=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI3]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI3_IND1_HASH_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullNLIJWithPredicates2() {
        m_tester.sql("select 1 from r3 full join ri1 on r3.pk = ri1.i where r3.ii is null and ri1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[1], EXPR$0=[$t4])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[full], whereCondition=[AND(IS NULL($1), IS NULL($3))], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0], II=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullNLIJWithPredicates3() {
        m_tester.sql("select 1 from r3 full join ri1 on r3.pk = ri1.i and ri1.bi = 5 where r3.ii is null and ri1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $2), =($3, 5))], joinType=[full], whereCondition=[AND(IS NULL($1), IS NULL($4))], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0], II=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], BI=[$t2], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testLeftlNLIJWithPredicates1() {
        // r3.ii > 9 stays as pre-join predicate. The rest goes to the inner index scan
        m_tester.sql("select 1 from r3 left join ri1 on r3.pk = ri1.i and r3.ii > 9 and ri1.si < 6 and r3.ii + ri1.i =9")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $3), $2, $4, =(+($1, $3), 9))], joinType=[left], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I], postPredicate=[$2])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $f3=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[6], expr#5=[<($t1, $t4)], I=[$t0], $f4=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testLeftNLIJWithPredicates2() {
        m_tester.sql("select 1 from r3 left join ri1 on r3.pk = ri1.i where r3.ii >0 and ri1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[1], EXPR$0=[$t4])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[left], whereCondition=[IS NULL($3)], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $condition=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testLeftNLIJWithPredicates3() {
        m_tester.sql("select 1 from r3 left join ri1 on r3.pk = ri1.i and ri1.bi = 5 where r3.ii is null and ri1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[left], whereCondition=[IS NULL($4)], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[IS NULL($t2)], PK=[$t0], II=[$t2], $condition=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[5], expr#5=[=($t2, $t4)], I=[$t0], BI=[$t2], TI=[$t3], $condition=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testLeftToInnerNLIJWithPredicates1() {
        m_tester.sql("select 1 from r3 left join ri1 on r3.pk = ri1.i where r3.ii is null and ri1.ti > 5")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[1], EXPR$0=[$t4])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[IS NULL($t2)], PK=[$t0], II=[$t2], $condition=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[5], expr#5=[>($t3, $t4)], I=[$t0], TI=[$t3], $condition=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testInnerNLIJWithPredicates1() {
        // r3.ii > 9 stays as a pre join predicate though can be pushed down to outer node
        m_tester.sql("select 1 from r3  join ri1 on r3.pk = ri1.i and r3.ii > 9 and ri1.si < 6 and r3.ii + ri1.i =9")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $3), $2, $4, =(+($1, $3), 9))], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I], postPredicate=[$2])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $f3=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[6], expr#5=[<($t1, $t4)], I=[$t0], $f4=[$t5])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testInnerNLIJWithPredicates2() {
        m_tester.sql("select 1 from r3  join ri1 on r3.pk = ri1.i and r3.ii > 9 and ri1.si < 6 and r3.ii + ri1.i =9 " +
                        "where ri1.bi > 0 and r3.ii < 11 and r3.ii + ri1.i < 11")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[1], EXPR$0=[$t6])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $3), =(+($1, $3), 9), <(+($1, $3), 11))], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], expr#5=[11], expr#6=[<($t2, $t5)], expr#7=[AND($t6, $t4)], PK=[$t0], II=[$t2], $f3=[$t4], $condition=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[6], expr#5=[<($t1, $t4)], expr#6=[0], expr#7=[>($t2, $t6)], expr#8=[AND($t7, $t5)], I=[$t0], BI=[$t2], $f4=[$t5], $condition=[$t8])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullNLJWithPredicates1() {
        m_tester.sql("select 1 from r3 full join r1 on r3.pk = r1.i and r3.ii > 9 and r1.si < 6 and r3.ii + r1.i =9")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[AND(=($0, $3), $2, $4, =(+($1, $3), 9))], joinType=[full])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $f3=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[6], expr#7=[<($t1, $t6)], I=[$t0], $f6=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testFullNLJWithPredicates2() {
        m_tester.sql("select 1 from r3 full join r1 on r3.pk = r1.i where r3.ii is null and r1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[1], EXPR$0=[$t4])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($0, $2)], joinType=[full], whereCondition=[AND(IS NULL($1), IS NULL($3))])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0], II=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testFullNLJWithPredicates3() {
        m_tester.sql("select 1 from r3 full join r1 on r3.pk = r1.i and r1.bi = 5 where r3.ii is null and r1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[AND(=($0, $2), =($4, 5))], joinType=[full], whereCondition=[AND(IS NULL($1), IS NULL($3))])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0], II=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2], BI=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLJWithPredicates1() {
        // r3.ii > 9 stays as a pre join predicate though can be pushed down to outer node
        m_tester.sql("select 1 from r3 left join r1 on r3.pk = r1.i and r3.ii > 9 and r1.si < 6 and r3.ii + r1.i =9")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[AND(=($0, $3), $2, $4, =(+($1, $3), 9))], joinType=[left])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $f3=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[6], expr#7=[<($t1, $t6)], I=[$t0], $f6=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLJWithPredicates2() {
        m_tester.sql("select 1 from r3 left join r1 on r3.pk = r1.i and r1.si < 6")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[1], EXPR$0=[$t3])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[AND(=($0, $1), $2)], joinType=[left])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[6], expr#7=[<($t1, $t6)], I=[$t0], $f6=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLJWithPredicates3() {
        m_tester.sql("select 1 from r3 left join r1 on r3.pk = r1.i where r3.ii >0 and r1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[1], EXPR$0=[$t4])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($0, $2)], joinType=[left], whereCondition=[IS NULL($3)])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $condition=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], TI=[$t2])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLJWithPredicates4() {
        m_tester.sql("select 1 from r3 left join r1 on r3.pk = r1.i and r1.bi = 5 where r3.ii is null and r1.ti is null")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($0, $2)], joinType=[left], whereCondition=[IS NULL($3)])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[IS NULL($t2)], PK=[$t0], II=[$t2], $condition=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[=($t3, $t6)], I=[$t0], TI=[$t2], BI=[$t3], $condition=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testLeftToInnerNLJWithPredicates1() {
        m_tester.sql("select 1 from r3 left join r1 on r3.pk = r1.i where r3.ii is null and r1.ti > 5")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[1], EXPR$0=[$t4])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[IS NULL($t2)], PK=[$t0], II=[$t2], $condition=[$t3])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[>($t2, $t6)], I=[$t0], TI=[$t2], $condition=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testInnerNLJWithPredicates1() {
        // r3.ii > 9 and r1.si < 6 stay at the join level though could be pushed down
        m_tester.sql("select 1 from r3  join r1 on r3.pk = r1.i and r3.ii > 9 and r1.si < 6 and r3.ii + r1.i =9")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], expr#5=[1], EXPR$0=[$t5])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[AND(=($0, $3), $2, $4, =(+($1, $3), 9))], joinType=[inner])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], PK=[$t0], II=[$t2], $f3=[$t4])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[6], expr#7=[<($t1, $t6)], I=[$t0], $f6=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testInnerNLJWithPredicates2() {
        m_tester.sql("select 1 from r3  join r1 on r3.pk = r1.i and r3.ii > 9 and r1.si < 6 and r3.ii + r1.i =9 " +
                        " where r1.bi > 0 and r3.ii < 11 and r3.ii + r1.i < 11")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[1], EXPR$0=[$t6])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[AND(=($0, $3), =(+($1, $3), 9), <(+($1, $3), 11))], joinType=[inner])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], expr#5=[11], expr#6=[<($t2, $t5)], expr#7=[AND($t6, $t4)], PK=[$t0], II=[$t2], $f3=[$t4], $condition=[$t7])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R3]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[6], expr#7=[<($t1, $t6)], expr#8=[0], expr#9=[>($t3, $t8)], expr#10=[AND($t9, $t7)], I=[$t0], BI=[$t3], $f6=[$t7], $condition=[$t10])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLIJWithPostPredicate() {
        // verify the column types with EE
        // The ri3.ii > 4 can be pushed down because it's on the inner join side
        // and r1.i < 2 must stay at the join
        // @TODO
        m_tester.sql("select r1.i from R1 left join ri3 on r1.i = ri3.ii and ri3.ii > 4 and r1.i < 2")
                .pass();
    }

    public void testInnerNLIJWithPostPredicate() {
        // verify the column types with EE
        // The ri3.ii > 4 and r1.i < 2 can be pushed down
        // @TODO
        m_tester.sql("select r1.i from R1 join ri3 on r1.i = ri3.ii and ri3.ii > 4 and r1.i < 2")
                .pass();
    }

    public void testLeftNLIJWithWherePredicate() {
        // verify the column types with EE
        // The ri3.ii > 4 and where ri3.iii + ri3.ii = 3 and r1.si is NULL can be pushed down because it's on the inner join side
        // and r1.i < 2 must stay at the join
        // @TODO
        m_tester.sql("select r1.i from R1 left join ri3 on r1.i = ri3.ii and ri3.ii > 4 and r1.i < 2 where ri3.iii + ri3.ii = 3 and r1.si is NULL")
                .pass();
    }

    public void testNLIJWithWherePredicate() {
        // verify the column types with EE
        // The post predicate ri3.ii > 4 and r1.i < 2 can be pushed down
        // WHERE where ri3.iii + ri3.ii = 3 and r1.si = 3 also can be pushed down
        m_tester.sql("select r1.i from R1 join ri3 on r1.i = ri3.ii and ri3.ii > 4 and r1.i < 2 where ri3.iii + ri3.ii = 3 and r1.si = 3")
                .pass();
    }
}
