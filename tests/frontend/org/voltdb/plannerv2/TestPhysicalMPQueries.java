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

}
