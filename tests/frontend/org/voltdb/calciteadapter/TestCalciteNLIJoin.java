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

import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.types.PlannerType;

public class TestCalciteNLIJoin extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteNLIJoin.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testReplicatedJoin() throws Exception {
        String sql;
//        sql = "select RI1.i from RI3 inner join  RI1  on RI3.ii = RI1.si;";
        sql = "select RI1.i from RI1 inner join  RI3  on RI3.ii = RI1.si;";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"NESTLOOP\",\"CHILDREN_IDS\":[4,6],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"SI0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}},\"WHERE_PREDICATE\":null},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":9,\"VALUE_SIZE\":32,\"COLUMN_IDX\":5},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":9,\"VALUE_SIZE\":1048576,\"ISNULL\":false,\"VALUE\":\"foo\"}},\"TARGET_TABLE_NAME\":\"R2\",\"TARGET_TABLE_ALIAS\":\"R2\"},{\"ID\":6,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"SI0\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":4}},\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
//        String voltPlan = testPlan(sql, PlannerType.VOLTDB);
//        if (voltPlan != null);
//        assertEquals(expectedPlan, calcitePlan);

    }

}
