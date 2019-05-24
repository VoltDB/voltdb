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

import org.junit.Test;
import org.voltdb.plannerv2.rules.PlannerRules.Phase;

public class TestPhysicalIndexRules extends Plannerv2TestCase {
    private PhysicalConversionRulesTester m_tester = new PhysicalConversionRulesTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(Phase.PHYSICAL_CONVERSION);
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testSimpleIndexReplicated() {
        m_tester.sql("select si from RI1 where i > 0")      // This one uses primary key index
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], expr#4=[0], " +
                        "expr#5=[>($t0, $t4)], SI=[$t1], $condition=[$t5], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n")
                .pass();
        m_tester.sql("select max(i) - min(si) from (select si, i from RI1 where i > 0) as foo")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[-($t0, $t1)], EXPR$0=[$t2], split=[1])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{}], agg#0=[MAX($0)], agg#1=[MIN($1)], split=[1], " +
                        "coordinator=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], " +
                        "expr#4=[0], expr#5=[>($t0, $t4)], proj#0..1=[{exprs}], $condition=[$t5], " +
                        "index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n")
                .pass();
        m_tester.sql("select * from RI1")                   // but this is just sequential scan
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], proj#0..3=[{exprs}], split=[1])\n" +
                        "  VoltPhysicalTableSequentialScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n")
                .pass();
    }

    @Test
    public void testAggregationUsingIndex() {
        m_tester.sql("select count(si) from RI1 where i > 0")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT($0)], split=[1], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], " +
                        "expr#4=[0], expr#5=[>($t0, $t4)], SI=[$t1], $condition=[$t5], " +
                        "index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n")
                .pass();
    }

    @Test
    public void testIndexInlineRules() {
        // CREATE INDEX RI2_IND1 ON RI2(ti)
        m_tester.sql("select ti from ri2 where ti > 5 and i + si > 4 limit 3")
                .transform("VoltPhysicalLimit(split=[1], limit=[3])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], " +
                        "expr#4=[5], expr#5=[>($t3, $t4)], expr#6=[+($t0, $t1)], expr#7=[4], expr#8=[>($t6, $t7)], " +
                        "expr#9=[AND($t5, $t8)], TI=[$t3], $condition=[$t9], index=[RI2_IND1_INVALIDGT1_0])\n")
                .pass();
    }

    @Test
    public void testSimpleIndexPartitioned() {
        // TODO: enable this with MP support in Calcite
        /*
        // CREATE INDEX PI1_IND1 ON PI1(ii)
        m_tester.sql("select i from pi1 WHERE i > 0")
                .transform("")
                .pass();
        */
    }

    @Test
    public void testExpressionIndex() {
        // CREATE INDEX RI1_IND3_EXPR ON RI1(ti + 1)
        m_tester.sql("select ti from ri1 where ti + 1 = 3")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], " +
                        "expr#4=[1], expr#5=[+($t3, $t4)], expr#6=[3], expr#7=[=($t5, $t6)], TI=[$t3], " +
                        "$condition=[$t7], index=[RI1_IND3_EXPR_INVALIDEQ1_1])\n")
                .pass();
    }

    @Test
    public void testPartialIndex() {
        // CREATE INDEX RI1_IND3_EXPR ON RI1(ti + 1) WHERE si IS NULL
        m_tester.sql("SELECT ti FROM RI1 WHERE ti + 1 = 3")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], " +
                        "expr#4=[1], expr#5=[+($t3, $t4)], expr#6=[3], expr#7=[=($t5, $t6)], TI=[$t3], $condition=[$t7], " +
                        "index=[RI1_IND3_EXPR_INVALIDEQ1_1])\n")
                .pass();
        // CREATE INDEX RI1_IND4_PART ON RI1(si) WHERE ti * 2 > 10
        m_tester.sql("SELECT ti FROM RI1 WHERE si > 4 AND ti * 2 > 10")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], " +
                        "expr#4=[4], expr#5=[>($t1, $t4)], expr#6=[2], expr#7=[*($t3, $t6)], expr#8=[10], " +
                        "expr#9=[>($t7, $t8)], expr#10=[AND($t5, $t9)], TI=[$t3], $condition=[$t10], " +
                        "index=[RI1_IND4_PART_INVALIDGT1_0])\n")
                .pass();
        // negative test
        m_tester.sql("SELECT ti FROM RI1 WHERE si > 4 AND ti * 3 > 10")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[4], expr#5=[>($t1, $t4)], expr#6=[3], " +
                        "expr#7=[*($t3, $t6)], expr#8=[10], expr#9=[>($t7, $t8)], expr#10=[AND($t5, $t9)], TI=[$t3], " +
                        "$condition=[$t10], split=[1])\n" +
                        "  VoltPhysicalTableSequentialScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n")
                .pass();
    }
}