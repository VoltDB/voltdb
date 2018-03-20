/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter;

import java.util.Map;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PlannerTestCase;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeTree;
import org.voltdb.types.PlannerType;

public abstract class TestCalciteBase extends PlannerTestCase {

    private CatalogMap<Table> getCatalogTables() {
        return getDatabase().getTables();
    }

    protected SchemaPlus schemaPlusFromDDL() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        for (Table table : getCatalogTables()) {
            rootSchema.add(table.getTypeName(), new VoltDBTable(table));
        }

        return rootSchema;
    }

    protected void comparePlans(String sql) {
        comparePlans(sql, null);
    }

    protected void comparePlans(String sql, Map<String, String> ignoreMap) {

        CompiledPlan calcitePlan = compileAdHocCalcitePlan(sql, true, true, DeterminismMode.SAFER);
        System.out.println(calcitePlan.explainedPlan);
        CompiledPlan voltdbPlan = compileAdHocPlan(sql, true, true);

        // Compare roots
        comparePlanTree(calcitePlan.rootPlanGraph, voltdbPlan.rootPlanGraph, ignoreMap);
        // Compare lower fragments if any
        if (calcitePlan.subPlanGraph != null && voltdbPlan.subPlanGraph != null) {
            comparePlanTree(calcitePlan.subPlanGraph, voltdbPlan.subPlanGraph, ignoreMap);
        } else if  (calcitePlan.subPlanGraph != null || voltdbPlan.subPlanGraph != null) {
            fail("Two-part MP plans mismatch");
        }
        // Compare CompiledPlan attributes
        compareCompiledPlans(calcitePlan, voltdbPlan);
    }

    public void compareJSONPlans(String calciteJSON, String voltdbJSON, Map<String, String> ignoreMap) {
        System.out.println("VoltDB : " + voltdbJSON);
        System.out.println("Calcite: " + calciteJSON);
        if (ignoreMap != null) {
            for (Map.Entry<String, String> ignore : ignoreMap.entrySet()) {
                calciteJSON = calciteJSON.replace(ignore.getKey(), ignore.getValue());
            }
            System.out.println("Calcite: " + calciteJSON);
        }
        assertEquals(voltdbJSON, calciteJSON);

    }

    private void comparePlanTree(AbstractPlanNode calcitePlanNode, AbstractPlanNode voltdbPlanNode, Map<String, String> ignoreMap) {
        PlanNodeTree calcitePlanTree = new PlanNodeTree(calcitePlanNode);
        PlanNodeTree voltdbPlanTree = new PlanNodeTree(voltdbPlanNode);

        String calcitePlanTreeJSON = calcitePlanTree.toJSONString();
        String voltdbPlanTreeJSON = voltdbPlanTree.toJSONString();

        compareJSONPlans(calcitePlanTreeJSON, voltdbPlanTreeJSON, ignoreMap);
    }

    private void compareCompiledPlans(CompiledPlan calcitePlan, CompiledPlan voltdbPlan) {
        // Compare LIMIT/OFFSET
        assertEquals(voltdbPlan.hasLimitOrOffset(), calcitePlan.hasLimitOrOffset());
        // Determinism
//        assertEquals(voltdbPlan.hasDeterministicStatement(), calcitePlan.hasDeterministicStatement());
        // Params
        ParameterValueExpression[] voltdbParams = voltdbPlan.getParameters();
        ParameterValueExpression[] calciteParams = calcitePlan.getParameters();
        assertEquals(voltdbParams.length, calciteParams.length);
        for (int i = 0; i < voltdbParams.length; ++i) {
            assertEquals(voltdbParams[i].getParameterIndex(), calciteParams[i].getParameterIndex());
        }
    }

    protected String testPlan(String sql, PlannerType plannerType) {
        CompiledPlan compiledPlan = (plannerType == PlannerType.CALCITE) ?
                compileAdHocCalcitePlan(sql, true, true, DeterminismMode.SAFER) :
                    compileAdHocPlan(sql, true, true);
        assert(compiledPlan.rootPlanGraph != null);
        PlanNodeTree planTree = new PlanNodeTree(compiledPlan.rootPlanGraph);
        String planTreeJSON = planTree.toJSONString();
        if (compiledPlan.subPlanGraph != null) {
            PlanNodeTree subPlanTree = new PlanNodeTree(compiledPlan.subPlanGraph);
            String subPlanTreeJSON = subPlanTree.toJSONString();
            planTreeJSON += subPlanTreeJSON;
        }
        System.out.println(plannerType.toString() + " : " + planTreeJSON);
        return planTreeJSON;
    }

    protected FrameworkConfig createConfig(SchemaPlus schema, Program program) {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(schema)
                .programs(program)
                .build();
        return config;
    }

    protected RelBuilder createBuilder(FrameworkConfig config) {
        return RelBuilder.create(config);
    }

    protected Planner createPlanner(FrameworkConfig config) {
        return Frameworks.getPlanner(config);
    }

    protected void verifyRelNode(String expectedRelNodeStr, RelNode relNode) {
        String relNodeStr = RelOptUtil.toString(relNode);
        System.out.println(relNodeStr);
        relNodeStr = relNodeStr.replaceAll("\\s", "");
        assertEquals(expectedRelNodeStr, relNodeStr);
    }
}
