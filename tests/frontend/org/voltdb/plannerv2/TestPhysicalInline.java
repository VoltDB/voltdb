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

import org.junit.Test;
import org.voltdb.plannerv2.rules.PlannerRules;

public class TestPhysicalInline extends Plannerv2TestCase {

    private PhysicalConversionRulesTester m_tester = new InlineRulesTester();

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(PlannerRules.Phase.INLINE);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 INNER JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[inner], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], " +
                        "index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOPINDEX\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_RI2_I\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}],\"COMPARE_NOTDISTINCT\":[false],\"END_EXPRESSION\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":null,\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testMJ() {
        // TODO: ambiguous plan generated
        m_tester.sql("SELECT RI1.SI, RI2.I FROM RI1 INNER JOIN RI2 ON RI2.TI = RI1.TI")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[inner], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND1_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":" +
                        "[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]}," +
                        "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND1\"}]," +
                        "\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null," +
                        "\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3," +
                        "\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null," +
                        "\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}," +
                        "\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":" +
                        "{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[inner], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                // RI2_IND5_HASH vs RI2_IND5 index
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND1\"}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND5_HASH\"}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testRightNLJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 RIGHT JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t3], TI=[$t1])\n" +
                           "  VoltPhysicalNestLoopJoin(condition=[=($2, $0)], joinType=[left])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOP\",\"CHILDREN_IDS\":[3,5],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"LEFT\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\"},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[3,5,2,1,7],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testLeftNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 LEFT JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3])\n" +
                           "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[left], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                           "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOPINDEX\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_RI2_I\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}],\"COMPARE_NOTDISTINCT\":[false],\"END_EXPRESSION\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"LEFT\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":null,\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

//    public void testMJWithTwoIndexColumns() {
//        m_tester.sql("SELECT RI2.I, RI2.si, RI2.BI, RI5.I, RI5.II FROM RI2 FULL JOIN RI5 " +
//                     " ON RI2.I = RI5.I AND RI5.II = RI2.BI")
//        .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..4=[{exprs}])\n" +
//                    "  VoltPhysicalMergeJoin(condition=[AND(=($0, $3), =($5, $2))], joinType=[full], outerIndex=[RI2_IND2], innerIndex=[RI5_IND_I_II_III])\n" +
//                    "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..2=[{exprs}], index=[RI2_IND2_ASCEQ0_0])\n" +
//                    "    VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[CAST($t1):BIGINT], proj#0..1=[{exprs}], II0=[$t3], index=[RI5_IND_I_II_III_ASCEQ0_0])\n")
//        .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":"
//                + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
//                + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},"
//                + "{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},"
//                + "{\"COLUMN_NAME\":\"I0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":3}},"
//                + "{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":4}}]},"
//                + "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":"
//                + "[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\","
//                + "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"II\","
//                + "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"II0\",\"EXPRESSION\":"
//                + "{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}}]}],"
//                + "\"TARGET_TABLE_NAME\":\"RI5\",\"TARGET_TABLE_ALIAS\":\"RI5\",\"LOOKUP_TYPE\":\"EQ\","
//                + "\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI5_IND_I_II_III\"}],\"CHILDREN_IDS\""
//                + ":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5"
//                + ",\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
//                + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,"
//                + "\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
//                + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,"
//                + "\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"II0\",\"EXPRESSION\":"
//                + "{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,"
//                + "\"TABLE_IDX\":1}}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":"
//                + "{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":"
//                + "{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
//                + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":"
//                + "{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}},"
//                + "\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":"
//                + "{\"TYPE\":21,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
//                + "\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":"
//                + "{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
//                + "\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":"
//                + "{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":7,"
//                + "\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}}}}},"
//                + "{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":"
//                + "\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
//                + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},"
//                + "{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}]}],"
//                + "\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\","
//                + "\"TARGET_INDEX_NAME\":\"RI2_IND2\"}],\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
//        .pass();
//
//    }
//
    public void testIndexScanWithOrderBy() {
        m_tester.sql("SELECT * FROM RI5 WHERE ii = 2 ORDER BY I, III")
        .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[2], " +
                "expr#4=[=($t1, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_IND_I_II_III_ASCEQ0_0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"III\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":2}},\"TARGET_TABLE_NAME\":\"RI5\",\"TARGET_TABLE_ALIAS\":\"RI5\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI5_IND_I_II_III\"}],\"EXECUTE_LIST\":[1,3],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testIndexScan() {
        m_tester.sql("SELECT II FROM RI5 WHERE ii = 2 and I - II > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[2], expr#4=[=($t1, $t3)], expr#5=[-($t0, $t1)], expr#6=[0], expr#7=[>($t5, $t6)], expr#8=[AND($t4, $t7)], II=[$t1], $condition=[$t8], index=[RI5_IND_II_INVALIDEQ1_1])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}]}],\"PREDICATE\":{\"TYPE\":13,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":2,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":0}},\"TARGET_TABLE_NAME\":\"RI5\",\"TARGET_TABLE_ALIAS\":\"RI5\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"RI5_IND_II\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":2}],\"COMPARE_NOTDISTINCT\":[false],\"END_EXPRESSION\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":2}}}],\"EXECUTE_LIST\":[1,3],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testMJWithTwoIndexColumns() {
        m_tester.sql("SELECT RI2.I, RI2.si, RI2.BI, RI5.I, RI5.II FROM RI2 FULL JOIN RI5 " +
                     " ON RI2.I = RI5.I AND RI5.II = RI2.BI")
        .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..4=[{exprs}])\n" +
                    "  VoltPhysicalMergeJoin(condition=[AND(=($0, $3), =($5, $2))], joinType=[full], outerIndex=[RI2_IND2], innerIndex=[RI5_IND_I_II_III])\n" +
                    "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], proj#0..2=[{exprs}], index=[RI2_IND2_ASCEQ0_0])\n" +
                    "    VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[CAST($t1):BIGINT], proj#0..1=[{exprs}], II0=[$t3], index=[RI5_IND_I_II_III_ASCEQ0_0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"I0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":4}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"II0\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}}]}],\"TARGET_TABLE_NAME\":\"RI5\",\"TARGET_TABLE_ALIAS\":\"RI5\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI5_IND_I_II_III\"}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"II0\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":21,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}}}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND2\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
        .pass();

    }

    public void testFullMJ() {
        // TODO: ambiguous plan generated
        m_tester.sql("SELECT RI1.SI, RI2.I FROM RI1 FULL JOIN RI2 ON RI2.TI = RI1.TI")
                // Using inner index RI2_IND5_HASH
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":" +
                        "[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\"," +
                        "\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\"," +
                        "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\"," +
                        "\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND5_HASH\"}]," +
                        "\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4," +
                        "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}]," +
                        "\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23," +
                        "\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":" +
                        "{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}," +
                        "\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":" +
                        "{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                // Using inner index RI2_IND1
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND1_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND1\"}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
                // TODO: investiage why sometimes pick up RI2_IND5_HASH index instead of RI2_IND1
                .json("{\"PLAN_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND5_HASH\"}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],\"EXECUTE_LIST\":[3,2,1,7],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitioned1() {
        m_tester.sql("select i from PI1")
                .transform("VoltPhysicalTableSequentialScan(table=[[public, PI1]], expr#0..5=[{inputs}], I=[$t0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\"}],\"EXECUTE_LIST\":[1,3,4,5],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitioned2() {
        m_tester.sql("select i from PI1 where I = 6")
                .transform("VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], expr#6=[6], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7], index=[VOLTDB_AUTOGEN_IDX_PK_PI1_I_INVALIDEQ1_1])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_PI1_I\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":6}],\"COMPARE_NOTDISTINCT\":[false],\"END_EXPRESSION\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":6}}}],\"EXECUTE_LIST\":[1,3],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitioned3() {
        m_tester.sql("select i from P1 where I = 6")
                .transform("VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], expr#6=[6], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":6}},\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}],\"EXECUTE_LIST\":[1,3],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitionedSort1() {
        m_tester.sql("select i from PI1 where I = 6 order by ii")
                .transform("VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], expr#6=[6], expr#7=[=($t0, $t6)], I=[$t0], II=[$t2], $condition=[$t7], index=[PI1_IND1_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":6}},\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"PI1_IND1\"}],\"EXECUTE_LIST\":[1,3],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitionedSort2() {
        m_tester.sql("select si, i from PI1 order by I")
                .transform("VoltPhysicalMergeExchange(distribution=[single], collation=[[1]])\n" +
                            "  VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], SI=[$t1], I=[$t0], index=[VOLTDB_AUTOGEN_IDX_PK_PI1_I_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"MERGERECEIVE\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1},\"SORT_DIRECTION\":\"ASC\"}]}],\"CHILDREN_IDS\":[3]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_PI1_I\"}],\"EXECUTE_LIST\":[1,3,4,6],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitionedSort3() {
        m_tester.sql("select i from PI1 order by I limit 4")
                .transform("VoltPhysicalMergeExchange(distribution=[single], collation=[[0]], limit=[4])\n" +
                            "  VoltPhysicalTableIndexScan(table=[[public, PI1]], expr#0..5=[{inputs}], I=[$t0], limit=[4], index=[VOLTDB_AUTOGEN_IDX_PK_PI1_I_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[5]},{\"ID\":5,\"PLAN_NODE_TYPE\":\"MERGERECEIVE\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"INLINE_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"LIMIT\",\"OFFSET\":0,\"LIMIT\":4,\"OFFSET_PARAM_IDX\":-1,\"LIMIT_PARAM_IDX\":-1,\"LIMIT_EXPRESSION\":null}],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"SORT_DIRECTION\":\"ASC\"}]}],\"CHILDREN_IDS\":[4]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"LIMIT\",\"OFFSET\":0,\"LIMIT\":4,\"OFFSET_PARAM_IDX\":-1,\"LIMIT_PARAM_IDX\":-1,\"LIMIT_EXPRESSION\":null}],\"TARGET_TABLE_NAME\":\"PI1\",\"TARGET_TABLE_ALIAS\":\"PI1\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_PI1_I\"}],\"EXECUTE_LIST\":[1,4,5,8],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testPartitionedSetOp1() {
        m_tester.sql("select I from p1 except select I from r1")
        .transform("VoltPhysicalMinus(all=[false])\n" +
                    "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "  VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], I=[$t0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"UNION\",\"CHILDREN_IDS\":[5,6],\"UNION_TYPE\":\"EXCEPT\"},{\"ID\":5,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"},{\"ID\":6,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[2,6,4,5,1,9],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testPartitionedSetOp2() {
        m_tester.sql("select I from p1 intersect select I from r1")
        .transform("VoltPhysicalIntersect(all=[false])\n" +
                    "  VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "  VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], I=[$t0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"UNION\",\"CHILDREN_IDS\":[5,6],\"UNION_TYPE\":\"INTERSECT\"},{\"ID\":5,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[4],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[2]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"},{\"ID\":6,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[2,6,4,5,1,9],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testPartitionedSetOp3() {
        m_tester.sql("select I from p1 where i = 9 union all select I from p2 where i = 9")
        .transform("VoltPhysicalUnion(all=[true])\n" +
                   "  VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                   "  VoltPhysicalTableSequentialScan(table=[[public, P2]], expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":8,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"UNION\",\"CHILDREN_IDS\":[2,5],\"UNION_TYPE\":\"UNION_ALL\"},{\"ID\":2,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":9}},\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":7,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":9}},\"TARGET_TABLE_NAME\":\"P2\",\"TARGET_TABLE_ALIAS\":\"P2\"}],\"EXECUTE_LIST\":[2,5,1,8],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testOuterJoinPartitionedTable2() {
        m_tester.sql("select p1.i from r1 left join p1 on p1.si = r1.i and p1.i = 8")
        .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], I0=[$t1])\n" +
                    "  VoltPhysicalNestLoopJoin(condition=[=($2, $0)], joinType=[left])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[8], expr#8=[=($t0, $t7)], I=[$t0], SI0=[$t6], $condition=[$t8])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOP\",\"CHILDREN_IDS\":[3,8],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"SI0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"LEFT\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"},{\"ID\":8,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[7],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[5]},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI0\",\"EXPRESSION\":{\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}}]}],\"PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":8}},\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}],\"EXECUTE_LIST\":[3,5,7,8,2,1,9],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testOuterJoinPartitionedTable3() {
        m_tester.sql("select p1.i from p2 full join p1 on p1.i = p2.i and p1.i = 8")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t1])\n" +
                    "  VoltPhysicalNestLoopJoin(condition=[AND(=($1, $0), =($1, 8))], joinType=[full])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P2]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], I=[$t0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[8]},{\"ID\":8,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[7],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOP\",\"CHILDREN_IDS\":[3,5],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"RIGHT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":8}}},\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P2\",\"TARGET_TABLE_ALIAS\":\"P2\"},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"}],\"EXECUTE_LIST\":[3,5,2,1,7,8,9],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testLimit1() {
        m_tester.sql("select I from R1 limit ? offset ?")
        .transform("VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], I=[$t0], limit=[?0], offset=[?1])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\",\"OFFSET\":0,\"LIMIT\":-1,\"OFFSET_PARAM_IDX\":1,\"LIMIT_PARAM_IDX\":0,\"LIMIT_EXPRESSION\":null}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[1,4],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    @Test
    public void testMutlipleSetOps() {
        m_tester.sql("SELECT I FROM P1 UNION SELECT I FROM R1 EXCEPT SELECT I FROM R2")
        .transform("VoltPhysicalMinus(all=[false])\n" +
                    "  VoltPhysicalUnion(all=[false])\n" +
                    "    VoltPhysicalExchange(distribution=[hash[0]])\n" +
                    "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "  VoltPhysicalTableSequentialScan(table=[[public, R2]], expr#0..5=[{inputs}], I=[$t0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":13,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"UNION\",\"CHILDREN_IDS\":[2,10],\"UNION_TYPE\":\"EXCEPT\"},{\"ID\":2,\"PLAN_NODE_TYPE\":\"UNION\",\"CHILDREN_IDS\":[6,7],\"UNION_TYPE\":\"UNION\"},{\"ID\":6,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[5],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[3]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"},{\"ID\":10,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":12,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"R2\",\"TARGET_TABLE_ALIAS\":\"R2\"}],\"EXECUTE_LIST\":[3,7,10,5,6,2,1,13],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testSelectWithCoalesce() {
        m_tester.sql("SELECT * FROM R1 ORDER BY COALESCE(R1.SI, 10)")
        .transform("VoltPhysicalSort(sort0=[$6], dir0=[ASC], pusheddown=[false])\n" +
                    "  VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], expr#6=[IS NOT NULL($t1)], expr#7=[CAST($t1):INTEGER], expr#8=[10], expr#9=[CASE($t6, $t7, $t8)], proj#0..5=[{exprs}], EXPR$6=[$t9])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[3]},{\"ID\":3,\"PLAN_NODE_TYPE\":\"ORDERBY\",\"CHILDREN_IDS\":[1],\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":6},\"SORT_DIRECTION\":\"ASC\"}]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":2,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"F\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":8,\"COLUMN_IDX\":4}},{\"COLUMN_NAME\":\"V\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":9,\"VALUE_SIZE\":32,\"COLUMN_IDX\":5}},{\"COLUMN_NAME\":\"EXPR$6\",\"EXPRESSION\":{\"TYPE\":300,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":8,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}},\"RIGHT\":{\"TYPE\":301,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},\"RIGHT\":{\"TYPE\":30,\"VALUE_TYPE\":5,\"ISNULL\":false,\"VALUE\":10}}}}]}],\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[1,3,4],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

    public void testNLNotDistinct() {
        m_tester.sql("SELECT P1.I FROM P1 FULL JOIN P2 ON P1.I IS NOT DISTINCT FROM P2.I")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "  VoltPhysicalNestLoopJoin(condition=[CASE(IS NULL($0), IS NULL($1), IS NULL($1), IS NULL($0), =($0, $1))], joinType=[full])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltPhysicalTableSequentialScan(table=[[public, P2]], expr#0..5=[{inputs}], I=[$t0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":9,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[8]},{\"ID\":8,\"PLAN_NODE_TYPE\":\"RECEIVE\",\"CHILDREN_IDS\":[7],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":7,\"PLAN_NODE_TYPE\":\"SEND\",\"CHILDREN_IDS\":[1]},{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOP\",\"CHILDREN_IDS\":[3,5],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":300,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"RIGHT\":{\"TYPE\":301,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":300,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":301,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":9,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"RIGHT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}}}}},\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P1\",\"TARGET_TABLE_ALIAS\":\"P1\"},{\"ID\":5,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}]}],\"TARGET_TABLE_NAME\":\"P2\",\"TARGET_TABLE_ALIAS\":\"P2\"}],\"EXECUTE_LIST\":[3,5,2,1,7,8,9],\"IS_LARGE_QUERY\":false}")
        .pass();
    }

//    public void testFullJoinWithUsing() {
//        m_tester.sql("SELECT * FROM R1 FULL JOIN R2 USING (I, SI)")
//        .json("")
//        .pass();
//    }

}
