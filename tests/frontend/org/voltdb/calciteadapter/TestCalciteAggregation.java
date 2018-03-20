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

import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.types.PlannerType;

public class TestCalciteAggregation extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteAggregation.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testAggr() throws Exception {
        String sql;
        sql = "select i, avg(ti), max(i) from R1 group by i";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$2\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_AVG\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr1() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select avg(ti) from R1";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_AVG\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr11() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select avg(ti) from R1 group by i";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_AVG\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr2() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select count(i) from R1 where ti > 3";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr3() throws Exception {
        String sql;
        sql = "select count(*) from R1";

        //comparePlans(sql, ignores);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT_STAR\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"$f0\",\"EXPRESSION\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr4() throws Exception {
        String sql;
        // Calcite produces a SeqScan here
        sql = "select count(*) from R1 where ti > 3";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT_STAR\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"$f0\",\"EXPRESSION\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr5() {
        String sql;
        sql = "select max(TI), SI, min(TI), I from R1 group by SI, I having avg(BI) > max(BI) ";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$2\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"EXPR$2\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"$f4\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":4}},{\"COLUMN_NAME\":\"$f5\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":5}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"AGGREGATE_TYPE\":\"AGGREGATE_MIN\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":3,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"AGGREGATE_TYPE\":\"AGGREGATE_AVG\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":4,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":5,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}],\"POST_PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":4},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":5}}},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr6() {
        String sql;
        sql = "select max(TI), SI, I, min(TI) from R1 group by I, SI having avg(BI) > 0 and si > 0";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$3\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"EXPR$3\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"$f4\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":4}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"AGGREGATE_TYPE\":\"AGGREGATE_MIN\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":3,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"AGGREGATE_TYPE\":\"AGGREGATE_AVG\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":4,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}],\"POST_PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":4},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}}},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}},\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr7() {
        String sql;
        sql = "select max(TI) from R1 group by SI having SI > 0";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}},\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testAggr8() {
        String sql;
        sql = "select max(TI), SI from R1 group by SI, I";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testDistinct() throws Exception {
        String sql;
        sql = "select distinct TI, I from R1 ";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testDistinct1() throws Exception {
        String sql;
        sql = "select distinct max(TI) from R1 group by I";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[5],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}]},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testDistinct2() throws Exception {
        String sql;
        sql = "select max (distinct (TI)) from R1 group by I";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":1,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    /**
     * COUNT for partitioned table is C SUM / F COUNT
     * @throws Exception
     */
    public void testPartitionedAggr() throws Exception {
        String sql;
        sql = "select count(ti) from P1";

        //comparePlans(sql);
        // Aggr non-partitioning column COUNT = SUM on coordinator
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"$f0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[6]},{\"ID\":6,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[7],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testPartitionedAggr1() throws Exception {
        String sql;
        sql = "select sum(ti) from P1";

        //comparePlans(sql);
        // Aggr non-partitioning column
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}]}]}{\"PLAN_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[5]},{\"ID\":5,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[6],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}]},{\"ID\":6,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    /**
     * AVG for partitioned table is Coordinator (SUM/COUNT) and Fragment (AVG)
     * @throws Exception
     */
    public void testPartitionedAggr2() throws Exception {
        String sql;
        sql = "select sum(i), avg(ti) from P1";

        //comparePlans(sql);
        // Aggr non-partitioning column AVG = SUM / COUNT
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":3,\"LEFT\":{\"TYPE\":4,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"$f1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"$f2\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]}]}{\"PLAN_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[6]},{\"ID\":6,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[7],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":0,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"AGGREGATE_TYPE\":\"AGGREGATE_AVG\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    /**
     * AVG for partitioned table is Coordinator (SUM/COUNT) and Fragment (AVG)
     * @throws Exception
     */
    public void testPartitionedAggr3() throws Exception {
        String sql;
        sql = "select i, count(ti) from P1 group by i";

        //comparePlans(sql);
        // Aggr non-partitioning column AVG = SUM / COUNT
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"$f1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}}]}]}{\"PLAN_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[6]},{\"ID\":6,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[7],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    /**
     * @throws Exception
     */
    public void testPartitionedAggr35() throws Exception {
        String sql;
        sql = "select count(*), i+ti from P1 group by i,ti";

        //comparePlans(sql);
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"EXPR$1\",\"EXPRESSION\":{\"TYPE\":1,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[5],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"$f2\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_SUM\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}]},{\"ID\":5,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}]}]}{\"PLAN_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[7]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[8],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_COUNT_STAR\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}]},{\"ID\":8,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testSerialAggr1() throws Exception {
        String sql;
        sql = "select max(ti) from RI1 where TI > 3 group by TI";

        //comparePlans(sql);
        // Serial Aggregation Index on TI RI1_IND1
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"AGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"RI1_IND1\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testSerialAggr2() throws Exception {
        String sql;
        sql = "select max(ti) from RI1 where BI > 3 group by SI, BI";

        //comparePlans(sql);
        // Serial Aggregation Index on (BI,SI) RI1_IND2
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"AGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":2,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0},{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":1}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"RI1_IND2\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testSerialAggr3() throws Exception {
        String sql;
        sql = "select max(ti) from RI1 where BI > 3 group by SI";

        //comparePlans(sql);
        // Hash Aggregation Index on (BI,SI) RI1_IND2 - the first index column BI is not in GROUP BY
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"HASHAGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"RI1_IND2\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testSerialAggr4() throws Exception {
        String sql;
        sql = "select max(ti) from RI1 where BI > 3 group by BI";

        //comparePlans(sql);
        // Serial Aggregation Index on (BI,SI) RI1_IND2
        String expectedPlan = "{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"AGGREGATE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"EXPR$0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"AGGREGATE_COLUMNS\":[{\"AGGREGATE_TYPE\":\"AGGREGATE_MAX\",\"AGGREGATE_DISTINCT\":0,\"AGGREGATE_OUTPUT_COLUMN\":1,\"AGGREGATE_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}],\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":0}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}},\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"GT\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"RI1_IND2\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":3}],\"COMPARE_NOTDISTINCT\":[false],\"SKIP_NULL_PREDICATE\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}}]}";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

    public void testPartialAggr() throws Exception {
        String sql;
        sql = "select max(ti) from RI1 where TI > 3 group by SI, TI";

        comparePlans(sql);
        // Partial Aggregation Index on TI RI1_IND1
        String expectedPlan = "";
        String calcitePlan = testPlan(sql, PlannerType.CALCITE);
        assertEquals(expectedPlan, calcitePlan);
    }

}
