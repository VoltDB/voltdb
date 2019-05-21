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

public class TestPhysicalIndexSelection extends Plannerv2TestCase {

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

    public void testUniqueWin() {
        // Unique Index RI5_UNIQUE_IND_I wins over the non-unique one RI5_IND_I
        m_tester.sql("SELECT * FROM RI5 where I = 8")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], split=[1], expr#0..2=[{inputs}], expr#3=[8], expr#4=[=($t0, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_UNIQUE_IND_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testUniqueWin1() {
        // Unique Index RI5_UNIQUE_IND_I wins over the non-unique one RI5_IND_I
        m_tester.sql("SELECT * FROM RI5 where I > 8")
        .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], split=[1], expr#0..2=[{inputs}], expr#3=[8], expr#4=[>($t0, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_UNIQUE_IND_I_INVALIDGT1_0])\n")
                .pass();
    }

    public void testMultiColumnWin() {
        // Two column Index RI5_IND_II_III wins over the single column RI5_IND_III
        m_tester.sql("SELECT * FROM RI5 where II = 3 and III = 4")
        .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], split=[1], expr#0..2=[{inputs}], expr#3=[3], expr#4=[=($t1, $t3)], expr#5=[4], expr#6=[=($t2, $t5)], expr#7=[AND($t4, $t6)], proj#0..2=[{exprs}], $condition=[$t7], index=[RI5_IND_II_III_INVALIDEQ2_2])\n")
                .pass();
    }

    public void testMultiColumnWin1() {
        // Single column RI5_IND_III wins over not-fully covered Two column Index RI5_IND_II_III
        m_tester.sql("SELECT * FROM RI5 where II = 3")
        .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], split=[1], expr#0..2=[{inputs}], expr#3=[3], expr#4=[=($t1, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_IND_II_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testPartialWin() {
        // Partial RI5_IND_II_PART wins over aregular one RI5_IND_II
        m_tester.sql("SELECT * FROM RI5 where II = 3 and ABS(I) > 8")
        .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], split=[1], expr#0..2=[{inputs}], expr#3=[3], expr#4=[=($t1, $t3)], expr#5=[ABS($t0)], expr#6=[8], expr#7=[>($t5, $t6)], expr#8=[AND($t4, $t7)], proj#0..2=[{exprs}], $condition=[$t8], index=[RI5_IND_II_PART_INVALIDEQ1_1])\n")
                .pass();
    }

}
