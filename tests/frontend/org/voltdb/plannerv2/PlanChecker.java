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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.CLIConfig;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.voltdb.plannerv2.SqlBatchImpl.CALCITE_CHECKS;

/**
 * A command line tool for Calcite planning comparison.
 * You can use the script at tests/sqlgrammar/plan-checker to run it.
 *
 * usage: plan-checker
 *  -d,--ddl <arg>     File path for the DDLs.
 *  -f,--file <arg>    The input file path that contains SQL query statements
 *                     (If -q/--query is used, this option will be ignored).
 *  -q,--query <arg>   Single SQL query statement string.
 */
public class PlanChecker extends CalcitePlannerTestCase {
    private static Pattern EXPR_PATTERN = Pattern.compile("EXPR\\$\\d+");

    /**
     * Initialize the database catalog with a DDL file.
     * @param path The path of the DDL file.
     * @throws Exception
     */
    void initWithDDL(String path) throws Exception {
        setupSchema(new File(path).toURI().toURL(), "testcalcite", false);
        init();
    }

    /**
     * Check and compare a single SQL statement.
     * @param sql
     */
    void checkPlan(String sql) {
        // remove the semi colon at the end of the sql statement
        sql = sql.replaceAll(";$", "");

        if (!isValid(sql)) return;

        CompiledPlan calcitePlan;
        CompiledPlan voltdbPlan;
        try {
            calcitePlan = compileAdHocCalcitePlan(sql, true, true, DeterminismMode.SAFER);
            voltdbPlan = compileAdHocPlan(sql, true, true);
        } catch (Exception e) {
            // The SQL is invalid in VOLT or is not supported in Calcite planner, stop checking and print nothing
            return;
        }

        // Compare roots
        comparePlanTree(calcitePlan.rootPlanGraph, voltdbPlan.rootPlanGraph, sql);
        // Compare lower fragments if any
        if (calcitePlan.subPlanGraph != null && voltdbPlan.subPlanGraph != null) {
            comparePlanTree(calcitePlan.subPlanGraph, voltdbPlan.subPlanGraph, sql);
        } else if (calcitePlan.subPlanGraph != null || voltdbPlan.subPlanGraph != null) {
            fail(String.format("Two-part MP plans mismatch.\n\nVoltDB plan: %s\n\nCalcite plan:%s\n",
                    voltdbPlan.subPlanGraph.toExplainPlanString(),
                    calcitePlan.subPlanGraph.toExplainPlanString()));
        }
        // Compare CompiledPlan attributes
        try {
            compareCompiledPlans(calcitePlan, voltdbPlan);
        } catch (AssertionError ae) {
            System.err.println(sql);
            System.err.println(ae.getMessage());
        }
    }

    /**
     * Check if the SQL statement is not a DDL and can pass the compatibility check
     *
     * @param sql
     * @return True if the SQL statement is not a DDL and can pass the compatibility check,
     * otherwise False.
     */
    boolean isValid(String sql) {
        try {
            if (VoltFastSqlParser.parse(sql).isA(SqlKind.DDL)) {
                return false;
            }
            if (!CALCITE_CHECKS.check(sql)) {
                // The query cannot pass the compatibility check, we just ignore it.
                return false;
            }
        } catch (SqlParseException e) {
            return false;
        }
        return true;
    }

    /**
     * Check and compare all SQL statements contains in the file.
     * @param filePath The path of the sql file to compare.
     */
    void checkPlans(String filePath) {
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(this::checkPlan);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void comparePlanTree(AbstractPlanNode calcitePlanNode, AbstractPlanNode voltdbPlanNode, String sql) {
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
        // Calcite and Volt have different names for anonymous columns
        // replace EXPR$0 to C1, EXPR$1 to C2, ...
        while (m.find()) {
            int newIndex = 1 + Integer.parseInt(m.group().replace("EXPR$", ""));
            m.appendReplacement(sb, "C" + newIndex);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static class PlanCheckerConfig extends CLIConfig {
        @Option(shortOpt = "d", opt = "ddl", desc = "File path for the DDLs.")
        String ddlFile = "";

        @Option(shortOpt = "q", opt = "query", desc = "Single SQL query statement string.")
        String query = "";

        @Option(shortOpt = "f", opt = "file", desc = "The input file path that contains SQL query statements " +
                "(If -q/--query is used, this option will be ignored).")
        String file = "";

        @Override
        public void validate() {
            if (ddlFile.isEmpty()) {
                exitWithMessageAndUsage("DDL path must be provided.");
            }
            File checkFile = new File(ddlFile);
            if (!checkFile.isFile()) {
                exitWithMessageAndUsage("Invalid DDL file path.");
            }
            if (query.isEmpty() && file.isEmpty()) {
                exitWithMessageAndUsage("SQL string or input file must be provided.");
            }
            if (!file.isEmpty()) {
                checkFile = new File(file);
                if (!checkFile.isFile()) {
                    exitWithMessageAndUsage("Invalid input file path.");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final PlanCheckerConfig cfg = new PlanCheckerConfig();
        cfg.parse("plan-checker", args);
        PlanChecker planChecker = new PlanChecker();
        planChecker.initWithDDL(cfg.ddlFile);
        if (!cfg.query.isEmpty()) {
            planChecker.checkPlan(cfg.query);
        } else {
            planChecker.checkPlans(cfg.file);
        }
    }
}
