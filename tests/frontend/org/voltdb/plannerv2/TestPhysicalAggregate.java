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

public class TestPhysicalAggregate extends Plannerv2TestCase {

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

    // Single GROUB BY column I  matches index (I)
    public void testSerailAggreagteNoGroupBy() {
        m_tester.sql("SELECT avg(R1.si) FROM R1 ")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[AVG($0)], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                        "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
                .pass();
    }

    // Single GROUB BY column I  matches index (I)
    public void testSerailAggreagteWithSeqScan1() {
        m_tester.sql("SELECT max(RI1.si), i FROM RI1 group by I")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], I=[$t0])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0}], EXPR$0=[MAX($1)], coordinator=[false], type=[serial])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ0_0])\n"
                )
                .pass();
    }

    // HASH aggregate with IndexScan(I) wins over Serial Aggregate with IndexScan(BI, SI)??
    public void testSerailAggreagteWithSeqScan2() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where I > 0 group by BI, SI")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2])\n" +
                        "  VoltPhysicalHashAggregate(group=[{0, 1}], EXPR$0=[MAX($1)], coordinator=[false], type=[hash])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t0, $t4)], BI=[$t2], SI=[$t1], $condition=[$t5], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // Single GROUB BY column I  matches index (I)
    public void testSerailAggreagteWithIndexScan1() {
        m_tester.sql("SELECT max(RI1.si), i FROM RI1 where I > 0 group by I")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], I=[$t0])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0}], EXPR$0=[MAX($1)], coordinator=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t0, $t4)], proj#0..1=[{exprs}], $condition=[$t5], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY columns (SI, BI) match index (BI, SI)
    public void testSerailAggreagteWithIndexScan2() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by SI, BI")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0, 1}], EXPR$0=[MAX($0)], coordinator=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], SI=[$t1], BI=[$t2], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY column (SI) does not match index (BI, SI) -
    // The first index column BI is not part of GROUP BY columns
    public void testHashAggreagteWithIndexScan3() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by SI")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($0)], coordinator=[false], type=[hash])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], SI=[$t1], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY columns (BI) matches index (BI, SI) -
    public void testSerialAggreagteWithIndexScan4() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by BI")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0}], EXPR$0=[MAX($1)], coordinator=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], BI=[$t2], SI=[$t1], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY columns (SI, BI, I) does not match index (BI, SI) -
    // Column I is not part of the (BI, SI) index
    public void testHashAggreagteWithIndexScan5() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by SI, BI, I")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], EXPR$0=[$t3])\n" +
                        "  VoltPhysicalHashAggregate(group=[{0, 1, 2}], EXPR$0=[MAX($0)], coordinator=[false], type=[hash])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], SI=[$t1], BI=[$t2], I=[$t0], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

}