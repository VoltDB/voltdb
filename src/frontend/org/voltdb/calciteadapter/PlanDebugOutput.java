/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.BuildDirectoryUtils;

public class PlanDebugOutput {

    private static final String BASE_DIR_NAME = "statement-calcite/";

    static void outputCalcitePlanningDetails(String sql, SqlNode parse, SqlNode validate,
            RelNode convert, RelNode calciteTransform, RelNode voltDBTransform,
            String dirName, String fileName) {
        outputCalcitePlanningDetails(sql, parse, validate,
                convert, calciteTransform, voltDBTransform,
                dirName, null, fileName);
    }

    static void outputCalcitePlanningDetails(String sql, SqlNode parse, SqlNode validate, RelNode convert,
            RelNode calciteTransform, RelNode voltDBTransform,
            String dirName, String errMsg, String fileName) {
        if (!VoltCompiler.DEBUG_MODE) {
            return;
        }

        StringBuffer sb = new StringBuffer();
        sb.append("\n*****************************************\n");
        sb.append("Calcite Planning Details\n");
        sb.append("\n**** Original stmt ****\n" + sql + "\n");
        if (parse != null) {
            sb.append("\n**** Parsed stmt ****\n" + parse + "\n");
        } else {
            sb.append("\n**** Failed to parse stmt ****\n");
        }
        if (validate != null) {
            sb.append("\n**** Validated stmt ****\n" + validate + "\n");
        } else {
            sb.append("\n**** Failed to validate stmt ****\n");
        }
        if (convert != null) {
            sb.append("\n**** Converted relational expression ****\n" +
                RelOptUtil.toString(convert) + "\n");
        } else {
            sb.append("\n**** Failed to convert relational expression ****\n");
        }
        if (calciteTransform != null) {
            sb.append("**** Calcite Optimized relational expression ****\n" +
                RelOptUtil.toString(calciteTransform) + "\n");
        } else {
            sb.append("**** Failed to apply Calcite relational expression ****\n");
        }
        if (voltDBTransform != null) {
            sb.append("**** VoltDB Optimized relational expression ****\n" +
                RelOptUtil.toString(voltDBTransform) + "\n");
        } else {
            sb.append("**** Failed to apply VoltDB relational expression ****\n");
        }
        if (errMsg != null) {
            sb.append("**** Calcite Error Message ****\n");
            sb.append(errMsg);
        }
        sb.append("\n*****************************************\n\n");

        BuildDirectoryUtils.writeFile(BASE_DIR_NAME + dirName,
                                      fileName + "-calcite.txt",
                                      sb.toString(),
                                      true);
    }

    private static String outputPlanDebugString(AbstractPlanNode planGraph) throws JSONException {
        PlanNodeList nodeList = new PlanNodeList(planGraph);

        // get the json serialized version of the plan
        String json = null;

        String crunchJson = nodeList.toJSONString();
        //System.out.println(crunchJson);
        //System.out.flush();
        JSONObject jobj = new JSONObject(crunchJson);
        json = jobj.toString(4);
        return json;
    }

    /**
     * @param plan
     * @param planGraph
     * @param filename
     * @return error message if any
     */
    static void outputPlanFullDebug(CompiledPlan plan, AbstractPlanNode planGraph,
            String dirName, String fileName) {

        if (!VoltCompiler.DEBUG_MODE) {
            return;
        }

        String json;
        try {
            json = outputPlanDebugString(planGraph);
        } catch (JSONException e2) {
            // Any plan that can't be serialized to JSON to
            // write to debugging output is also going to fail
            // to get written to the catalog, to sysprocs, etc.
            // Just bail.
            String errorMsg = "Plan for sql: '" + plan.sql +
                               "' can't be serialized to JSON";
            throw new CalcitePlanningException(errorMsg);
        }
        // output a description of the parsed stmt
        json = "PLAN:\n" + json;
        json = "COST: " + String.valueOf(plan.cost) + "\n" + json;
        assert (plan.sql != null);
        json = "SQL: " + plan.sql + "\n" + json;

        // write json to disk
        BuildDirectoryUtils.writeFile(BASE_DIR_NAME + dirName,
                                      fileName + "-json.txt",
                                      json,
                                      true);

    }

    /**
     * @param plan
     * @param filename
     */
    static void outputExplainedPlan(CompiledPlan plan, String dirName, String fileName) {
        if (VoltCompiler.DEBUG_MODE) {
            BuildDirectoryUtils.writeFile(BASE_DIR_NAME + dirName ,
                                      fileName + "-plan.txt",
                                      plan.explainedPlan,
                                      true);
        }
    }

}
