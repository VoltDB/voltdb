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

import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlanChecker extends CalcitePlannerTestCase {
    private static Pattern EXPR_PATTERN = Pattern.compile("EXPR\\$\\d+");

    private void initWithDDL() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
    }

    private void checkPlan(String sql) {
        CompiledPlan calcitePlan;
        CompiledPlan voltdbPlan;
        try {
            calcitePlan = compileAdHocCalcitePlan(sql, true, true, DeterminismMode.SAFER);
        } catch (Exception e) {
            // The SQL is invalid in VOLT, stop checking and print nothing
            return;
        }
        try {
            voltdbPlan = compileAdHocPlan(sql, true, true);
        } catch (Exception e) {
            // The SQL is not supported in Calcite planner, stop checking and print nothing
            return;
        }

        // Compare roots
        comparePlanTree(calcitePlan.rootPlanGraph, voltdbPlan.rootPlanGraph, sql);
        // Compare lower fragments if any
        if (calcitePlan.subPlanGraph != null && voltdbPlan.subPlanGraph != null) {
            comparePlanTree(calcitePlan.subPlanGraph, voltdbPlan.subPlanGraph, sql);
        } else if (calcitePlan.subPlanGraph != null || voltdbPlan.subPlanGraph != null) {
            fail(String.format("Two-part MP plans mismatch.\n\nVoltDB plan: %s\n\nCalcite plan:%s\n", voltdbPlan.subPlanGraph.toExplainPlanString(), calcitePlan.subPlanGraph.toExplainPlanString()));
        }
        // Compare CompiledPlan attributes
        try {
            compareCompiledPlans(calcitePlan, voltdbPlan);
        } catch (AssertionError ae) {
            System.err.println(sql);
            System.err.println(ae.getMessage());
        }
    }

    private void comparePlanTree(AbstractPlanNode calcitePlanNode, AbstractPlanNode voltdbPlanNode, String sql) {
        PlanNodeTree calcitePlanTree = new PlanNodeTree(calcitePlanNode);
        PlanNodeTree voltPlanTree = new PlanNodeTree(voltdbPlanNode);

        String calcitePlanTreeJSON = normalizeCalcitePlan(calcitePlanTree.toJSONString());
        String voltPlanTreeJSON = voltPlanTree.toJSONString();

        if (!voltPlanTreeJSON.equals(calcitePlanTreeJSON)) {
            System.err.println(sql);
            System.err.println("Expected: " + voltPlanTreeJSON);
            System.err.println("Actual: " + calcitePlanTreeJSON);
        }
    }

    private String normalizeCalcitePlan(String plan) {
        Matcher m = EXPR_PATTERN.matcher(plan);
        StringBuffer sb = new StringBuffer();
        // Calcite and Volt have difference names for anonymous columns
        // replace EXPR$0 to C1, EXPR$1 to C2, ...
        while (m.find()) {
            int newIndex = 1 + Integer.parseInt(m.group().replace("EXPR$", ""));
            m.appendReplacement(sb, "C" + newIndex);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        PlanChecker planChecker = new PlanChecker();
        planChecker.initWithDDL();
        planChecker.checkPlan("select i * 5 from R1");
    }
}