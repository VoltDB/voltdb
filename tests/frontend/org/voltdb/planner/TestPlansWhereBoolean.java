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

package org.voltdb.planner;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class TestPlansWhereBoolean extends PlannerTestCase {

    private void checkWhereBooleanPlan(AbstractPlanNode pn,
                                       boolean conditionTrue,
                                       boolean tablePartitioned) {
        assertTrue(pn instanceof SendPlanNode);
        pn = pn.getChild(0);
        if (tablePartitioned) {
            if (pn instanceof ProjectionPlanNode) {
                pn = pn.getChild(0);
            }
        }
        else {
            assertTrue(pn instanceof SeqScanPlanNode);
            pn = pn.getInlinePlanNode(PlanNodeType.LIMIT);
        }
        if (! conditionTrue) {
            assertNotNull(pn);
            assertTrue(pn instanceof LimitPlanNode);
            assertEquals(0, ((LimitPlanNode)pn).getLimit());
            assertEquals(0, ((LimitPlanNode)pn).getOffset());
        }
    }

    public void testPlanWhereBoolean() {
        int tableCount = 2;
        String[] conditions = {"1=0", "FALSE", "1>2", "1=1", "6-1>=0", "TRUE"};
        String[] limitoffsets = {"", "LIMIT 2", "OFFSET 2", "LIMIT 2 OFFSET 2"};
        String sqlBody = "SELECT * FROM %s WHERE %s %s";
        String sql = null;
        AbstractPlanNode pn = null;
        for (int condId=0; condId<conditions.length; condId++) {
            for (String limitoffset : limitoffsets) {
                for (int tableId=1; tableId<=tableCount; tableId++) {
                    sql = String.format(sqlBody, "T" + tableId, conditions[condId], limitoffset);
                    pn = compile(sql);
                    System.out.println(sql);
                    // checkWhereBooleanPlan(planNode, conditionTrue, tablePartitioned)
                    checkWhereBooleanPlan(pn, condId>2, tableId>1);
                }
            }
        }
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-whereboolean-ddl.sql"), "WhereBoolean", false);
    }
}
