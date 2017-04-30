/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PlannerTestCase;
import org.voltdb.plannodes.PlanNodeTree;

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

        PlanNodeTree calcitePlanTree = new PlanNodeTree(calcitePlan.rootPlanGraph);
        PlanNodeTree voltdbPlanTree = new PlanNodeTree(voltdbPlan.rootPlanGraph);

        String calcitePlanTreeJSON = calcitePlanTree.toJSONString();
        String voltdbPlanTreeJSON = voltdbPlanTree.toJSONString();
        System.out.println("VoltDB : " + voltdbPlanTreeJSON);
        System.out.println("Calcite: " + calcitePlanTreeJSON);

        if (ignoreMap != null) {
            for (Map.Entry<String, String> ignore : ignoreMap.entrySet()) {
                calcitePlanTreeJSON = calcitePlanTreeJSON.replace(ignore.getKey(), ignore.getValue());
            }
            System.out.println("Calcite: " + calcitePlanTreeJSON);
        }
        assertEquals(voltdbPlanTreeJSON, calcitePlanTreeJSON);
    }

}
