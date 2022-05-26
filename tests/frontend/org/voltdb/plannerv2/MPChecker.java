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

import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.CompiledPlan;

/**
 * A command line tool to check that the query that Calcite planner considers as MP is indeed MP and vice versa.
 * You can use the script at tests/sqlgrammar/mp-checker to run it.
 *
 * usage: mp-checker
 *  -d,--ddl <arg>     File path for the DDLs.
 *  -f,--file <arg>    The input file path that contains SQL query statements
 *                     (If -q/--query is used, this option will be ignored).
 *  -q,--query <arg>   Single SQL query statement string.
 */
public class MPChecker extends PlanChecker {

    /**
     * Check if the MP/SP is consistent between Calcite plan and Volt plan for a single SQL statement.
     *
     * @param sql
     */
    void checkPlan(String sql) {
        // remove the semi colon at the end of the sql statement
        sql = sql.replaceAll(";$", "");

        if (!isValid(sql)) return;

        CompiledPlan calcitePlan = null;
        CompiledPlan voltdbPlan = null;
        try {
            voltdbPlan = compileAdHocPlan(sql, true, true);
            calcitePlan = compileAdHocCalcitePlan(sql, true, true, DeterminismMode.SAFER);
        } catch (Exception e) {
            if (e.getMessage().contains("MP query not supported in Calcite planner") &&
                    voltdbPlan != null && voltdbPlan.subPlanGraph == null) {
                // if the SQL is fallback as MP in Calcite but SP in Volt, print the SQL statement.
                System.err.println(sql);
            }
            // The SQL is invalid in VOLT or is not supported in Calcite planner, stop checking and print nothing
            return;
        }

        if ((calcitePlan.subPlanGraph == null && voltdbPlan.subPlanGraph != null) ||
                (calcitePlan.subPlanGraph != null && voltdbPlan.subPlanGraph == null)) {
            // if the MP/SP is inconsistent between Calcite plan and Volt plan, print the SQL statement.
            System.err.println(sql);
        }
        // if Calcite make the right decision on SP/MP, print nothing.
    }

    public static void main(String[] args) throws Exception {
        // we can reuse the same cli config as plan-checker.
        final PlanCheckerConfig cfg = new PlanCheckerConfig();
        cfg.parse("mp-checker", args);
        MPChecker mpChecker = new MPChecker();
        mpChecker.initWithDDL(cfg.ddlFile);
        if (!cfg.query.isEmpty()) {
            mpChecker.checkPlan(cfg.query);
        } else {
            mpChecker.checkPlans(cfg.file);
        }
    }
}
