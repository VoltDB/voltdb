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
        m_tester.sql("SELECT * FROM RI1 INNER JOIN R2 ON RI1.i = R2.i")
                .transform("VoltPhysicalCalc(expr#0..9=[{inputs}], I=[$t6], SI=[$t7], BI=[$t8], TI=[$t9], I0=[$t0], SI0=[$t1], TI0=[$t2], BI0=[$t3], F=[$t4], V=[$t5], split=[1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($6, $0)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testNLIJ3() {
        m_tester.sql("SELECT RI1.i FROM R2 INNER JOIN RI1 ON RI1.i = R2.i")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t1], split=[1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($1, $0)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testMultiTableNLIJ() {
        m_tester.sql("SELECT R1.si, R2.bi, RI1.ti FROM R1 INNER JOIN R2 ON R1.i = R2.i "
                + "INNER JOIN RI1 ON RI1.I = R1.I")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1], BI=[$t5], TI=[$t3], split=[1])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($0, $4)], joinType=[inner], split=[1])\n" +
                        "    VoltPhysicalNestLoopIndexJoin(condition=[=($2, $0)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "      VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}], split=[1])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "      VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3], split=[1])\n" +
                        "        VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testMultiTableNLIJ1() {
        m_tester.sql("SELECT R1.si, R2.bi, RI1.ti FROM R1 INNER JOIN RI1 ON R1.si = RI1.si "
                + "INNER JOIN R2 ON R2.I = RI1.I")
                .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t5], BI=[$t1], TI=[$t4], split=[1])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($5, $3)], joinType=[inner], split=[1])\n" +
                        "    VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3], split=[1])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "      VoltPhysicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], TI=[$t3], split=[1])\n" +
                        "        VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
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
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], SI=[$t0], BI=[$t4], TI=[$t2], split=[1])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($3, $1)], joinType=[inner], split=[1])\n" +
                        "    VoltPhysicalCalc(expr#0..4=[{inputs}], SI=[$t1], I0=[$t2], TI0=[$t3], split=[1])\n" +
                        "      VoltPhysicalNestLoopJoin(condition=[=($0, $4)], joinType=[inner], split=[1])\n" +
                        "        VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}], split=[1])\n" +
                        "          VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "        VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[CAST($t1):INTEGER], I=[$t0], TI=[$t3], SI0=[$t4], split=[1])\n" +
                        "          VoltPhysicalTableSequentialScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
                .pass();
    }

    public void testMultiTableNLIJ3() {
        m_tester.sql("SELECT * FROM R1 INNER JOIN R2 ON R1.i = R2.i "
                + "INNER JOIN RI1 ON RI1.I = R1.I")
                .transform("VoltPhysicalCalc(expr#0..15=[{inputs}], proj#0..15=[{exprs}], split=[1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($12, $0)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI1_I])\n" +
                        "    VoltPhysicalNestLoopJoin(condition=[=($0, $6)], joinType=[inner], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ1_1])\n")
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
                .transform("VoltPhysicalCalc(expr#0..9=[{inputs}], SI=[$t3], BI=[$t5], TI=[$t1], split=[1])\n" +
                        "  VoltPhysicalNestLoopJoin(condition=[=($9, $7)], joinType=[inner], split=[1])\n" +
                        "    VoltPhysicalNestLoopJoin(condition=[=($8, $7)], joinType=[inner], split=[1])\n" +
                        "      VoltPhysicalNestLoopJoin(condition=[=($7, $6)], joinType=[inner], split=[1])\n" +
                        "        VoltPhysicalNestLoopJoin(condition=[=($6, $4)], joinType=[inner], split=[1])\n" +
                        "          VoltPhysicalNestLoopJoin(condition=[=($4, $0)], joinType=[inner], split=[1])\n" +
                        "            VoltPhysicalNestLoopJoin(condition=[=($2, $0)], joinType=[inner], split=[1])\n" +
                        "              VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3], split=[1])\n" +
                        "                VoltPhysicalTableSequentialScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "              VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}], split=[1])\n" +
                        "                VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "            VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3], split=[1])\n" +
                        "              VoltPhysicalTableSequentialScan(table=[[public, R2]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "          VoltPhysicalCalc(expr#0..2=[{inputs}], PK=[$t0], split=[1])\n" +
                        "            VoltPhysicalTableSequentialScan(table=[[public, R3]], split=[1], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "        VoltPhysicalCalc(expr#0..3=[{inputs}], PK=[$t0], split=[1])\n" +
                        "          VoltPhysicalTableSequentialScan(table=[[public, R4]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n" +
                        "      VoltPhysicalCalc(expr#0..4=[{inputs}], PK=[$t0], split=[1])\n" +
                        "        VoltPhysicalTableSequentialScan(table=[[public, R5]], split=[1], expr#0..4=[{inputs}], proj#0..4=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], PK=[$t0], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, RI3]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n")
                .pass();
    }

    public void testLeftNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 LEFT JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], split=[1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[left], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 FULL JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], split=[1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[full], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}], split=[1])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testFullMJ() {
        m_tester.sql("SELECT RI1.SI, RI2.I FROM RI1 FULL JOIN RI2 ON RI2.TI = RI1.TI")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2], split=[1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], split=[1], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND1_ASCEQ0_0])\n")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2], split=[1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], split=[1], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], TI=[$t3], split=[1])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .pass();
    }

}
