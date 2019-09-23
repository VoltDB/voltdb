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
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], split=[1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[inner], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                        "    VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], " +
                        "index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":[" +
                        "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}," +
                        "{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOPINDEX\",\"INLINE_NODES\":[" +
                        "{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\"," +
                        "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}]," +
                        "\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\"," +
                        "\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":\"VOLTDB_AUTOGEN_IDX_PK_RI2_I\"," +
                        "\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}],\"COMPARE_NOTDISTINCT\":[false]," +
                        "\"END_EXPRESSION\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0," +
                        "\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}}],\"CHILDREN_IDS\":[3]," +
                        "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}]," +
                        "\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":null,\"WHERE_PREDICATE\":null}," +
                        "{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"," +
                        "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}]," +
                        "\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testMJ() {
        // TODO: ambiguous plan generated
        m_tester.sql("SELECT RI1.SI, RI2.I FROM RI1 INNER JOIN RI2 ON RI2.TI = RI1.TI")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2], split=[1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[inner], split=[1], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND1_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":" +
                        "[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]}," +
                        "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":" +
                        "{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND1\"}],\"CHILDREN_IDS\":[3]," +
                        "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}]," +
                        "\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23," +
                        "\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12," +
                        "\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}," +
                        "\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":" +
                        "{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}]," +
                        "\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2], split=[1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[inner], split=[1], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":" +
                        "[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]}," +
                        "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":" +
                        "{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND5_HASH\"}],\"CHILDREN_IDS\":[3]," +
                        "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}," +
                        "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}}," +
                        "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}]," +
                        "\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23," +
                        "\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32," +
                        "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12," +
                        "\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}," +
                        "\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}},{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\"," +
                        "\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\"," +
                        "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":" +
                        "{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\"," +
                        "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}]," +
                        "\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testRightNLJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 RIGHT JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t3], TI=[$t1], split=[1])\n" +
                           "  VoltPhysicalNestLoopJoin(condition=[=($2, $0)], joinType=[left], split=[1])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],"
                        + "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                        + "\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":1}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOP\",\"CHILDREN_IDS\":[3,5],"
                        + "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                        + "\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                        + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":4,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\","
                        + "\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":"
                        + "{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":"
                        + "\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":"
                        + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],"
                        + "\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\"},{\"ID\":5,\"PLAN_NODE_TYPE\":"
                        + "\"SEQSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":"
                        + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],"
                        + "\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[3,5,2,1],"
                        + "\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testLeftNLIJ() {
        m_tester.sql("SELECT R1.si, RI2.ti FROM R1 LEFT JOIN RI2 ON R1.i = RI2.i")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], split=[1])\n" +
                           "  VoltPhysicalNestLoopIndexJoin(condition=[=($0, $2)], joinType=[left], split=[1], innerIndex=[VOLTDB_AUTOGEN_IDX_PK_RI2_I])\n" +
                           "    VoltPhysicalTableSequentialScan(table=[[public, R1]], split=[1], expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                           "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[VOLTDB_AUTOGEN_IDX_PK_RI2_I_INVALIDEQ1_1])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],"
                        + "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                        + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3}}]},{\"ID\":2,\"PLAN_NODE_TYPE\":\"NESTLOOPINDEX\",\"INLINE_NODES\":"
                        + "[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":"
                        + "\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":"
                        + "\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"INVALID\",\"TARGET_INDEX_NAME\":"
                        + "\"VOLTDB_AUTOGEN_IDX_PK_RI2_I\",\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":5,"
                        + "\"COLUMN_IDX\":0}],\"COMPARE_NOTDISTINCT\":[false],\"END_EXPRESSION\":{\"TYPE\":10,\"VALUE_TYPE\""
                        + ":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1},\"RIGHT\":"
                        + "{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}}}],\"CHILDREN_IDS\":[3],\"OUTPUT_SCHEMA\":"
                        + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},"
                        + "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,"
                        + "\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,"
                        + "\"JOIN_PREDICATE\":null,\"WHERE_PREDICATE\":null},{\"ID\":3,\"PLAN_NODE_TYPE\":\"SEQSCAN\","
                        + "\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":"
                        + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}}]}],"
                        + "\"TARGET_TABLE_NAME\":\"R1\",\"TARGET_TABLE_ALIAS\":\"R1\"}],\"EXECUTE_LIST\":[3,2,1],"
                        + "\"IS_LARGE_QUERY\":false}")
                .pass();
    }

    public void testMJWithTwoIndexColumns() {
        m_tester.sql("SELECT RI2.I, RI2.si, RI2.BI, RI5.I, RI5.II FROM RI2 FULL JOIN RI5 " +
                     " ON RI2.I = RI5.I AND RI5.II = RI2.BI")
        .transform("VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..4=[{exprs}], split=[1])\n" +
                    "  VoltPhysicalMergeJoin(condition=[AND(=($0, $3), =($5, $2))], joinType=[full], split=[1], outerIndex=[RI2_IND2], innerIndex=[RI5_IND_I_II_III])\n" +
                    "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], proj#0..2=[{exprs}], index=[RI2_IND2_ASCEQ0_0])\n" +
                    "    VoltPhysicalTableIndexScan(table=[[public, RI5]], split=[1], expr#0..2=[{inputs}], expr#3=[CAST($t1):BIGINT], proj#0..1=[{exprs}], II0=[$t3], index=[RI5_IND_I_II_III_ASCEQ0_0])\n")
        .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\":"
                + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},"
                + "{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}},"
                + "{\"COLUMN_NAME\":\"I0\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":3}},"
                + "{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":4}}]},"
                + "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":"
                + "[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\","
                + "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"II\","
                + "\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"II0\",\"EXPRESSION\":"
                + "{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1}}}]}],"
                + "\"TARGET_TABLE_NAME\":\"RI5\",\"TARGET_TABLE_ALIAS\":\"RI5\",\"LOOKUP_TYPE\":\"EQ\","
                + "\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI5_IND_I_II_III\"}],\"CHILDREN_IDS\""
                + ":[3],\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5"
                + ",\"COLUMN_IDX\":0}},{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,"
                + "\"COLUMN_IDX\":2}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"II\",\"EXPRESSION\":{\"TYPE\":32,"
                + "\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"II0\",\"EXPRESSION\":"
                + "{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,"
                + "\"TABLE_IDX\":1}}}],\"JOIN_TYPE\":\"FULL\",\"PRE_JOIN_PREDICATE\":null,\"JOIN_PREDICATE\":"
                + "{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":"
                + "{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":"
                + "{\"TYPE\":7,\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}},"
                + "\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}},\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":"
                + "{\"TYPE\":21,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                + "\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":"
                + "{\"TYPE\":20,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                + "\"COLUMN_IDX\":0},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},\"RIGHT\":"
                + "{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2},\"RIGHT\":{\"TYPE\":7,"
                + "\"VALUE_TYPE\":6,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":1,\"TABLE_IDX\":1}}}}}},"
                + "{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":"
                + "\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                + "{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}},"
                + "{\"COLUMN_NAME\":\"BI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":6,\"COLUMN_IDX\":2}}]}],"
                + "\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\","
                + "\"TARGET_INDEX_NAME\":\"RI2_IND2\"}],\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
        .pass();

    }


    public void testFullMJ() {
        // TODO: ambiguous plan generated
        m_tester.sql("SELECT RI1.SI, RI2.I FROM RI1 FULL JOIN RI2 ON RI2.TI = RI1.TI")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2], split=[1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], split=[1], outerIndex=[RI1_IND1], innerIndex=[RI2_IND1])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND1_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\""
                        + ":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},"
                        + "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":"
                        + "\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":"
                        + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],"
                        + "\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\","
                        + "\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND1\"}],\"CHILDREN_IDS\":[3],"
                        + "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                        + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                        + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,"
                        + "\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},"
                        + "\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}}"
                        + ",{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":"
                        + "\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                        + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\","
                        + "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],"
                        + "\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                // Using inner index RI2_IND5_HASH
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], SI=[$t0], I=[$t2], split=[1])\n" +
                        "  VoltPhysicalMergeJoin(condition=[=($3, $1)], joinType=[full], split=[1], outerIndex=[RI1_IND1], innerIndex=[RI2_IND5_HASH])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], split=[1], expr#0..3=[{inputs}], SI=[$t1], TI=[$t3], index=[RI1_IND1_ASCEQ0_0])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI2]], split=[1], expr#0..3=[{inputs}], I=[$t0], TI=[$t3], index=[RI2_IND5_HASH_ASCEQ0_0])\n")
                .json("{\"PLAN_NODES\":[{\"ID\":1,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"CHILDREN_IDS\":[2],\"OUTPUT_SCHEMA\""
                        + ":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":2}}]},"
                        + "{\"ID\":2,\"PLAN_NODE_TYPE\":\"MERGEJOIN\",\"INLINE_NODES\":[{\"ID\":5,\"PLAN_NODE_TYPE\":"
                        + "\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":6,\"PLAN_NODE_TYPE\":\"PROJECTION\",\"OUTPUT_SCHEMA\":"
                        + "[{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,\"COLUMN_IDX\":0}},"
                        + "{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}]}],"
                        + "\"TARGET_TABLE_NAME\":\"RI2\",\"TARGET_TABLE_ALIAS\":\"RI2\",\"LOOKUP_TYPE\":\"EQ\","
                        + "\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI2_IND5_HASH\"}],\"CHILDREN_IDS\":[3],"
                        + "\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                        + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3}},{\"COLUMN_NAME\":\"I\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":5,"
                        + "\"COLUMN_IDX\":0,\"TABLE_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1}}],\"JOIN_TYPE\":\"INNER\",\"PRE_JOIN_PREDICATE\":null,"
                        + "\"JOIN_PREDICATE\":{\"TYPE\":10,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}},"
                        + "\"WHERE_PREDICATE\":null,\"LESS_JOIN_PREDICATE\":{\"TYPE\":12,\"VALUE_TYPE\":23,\"LEFT\":{\"TYPE\":32,"
                        + "\"VALUE_TYPE\":3,\"COLUMN_IDX\":3,\"TABLE_IDX\":1},\"RIGHT\":{\"TYPE\":32,\"VALUE_TYPE\":3,\"COLUMN_IDX\":3}}}"
                        + ",{\"ID\":3,\"PLAN_NODE_TYPE\":\"INDEXSCAN\",\"INLINE_NODES\":[{\"ID\":4,\"PLAN_NODE_TYPE\":"
                        + "\"PROJECTION\",\"OUTPUT_SCHEMA\":[{\"COLUMN_NAME\":\"SI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":4,"
                        + "\"COLUMN_IDX\":1}},{\"COLUMN_NAME\":\"TI\",\"EXPRESSION\":{\"TYPE\":32,\"VALUE_TYPE\":3,"
                        + "\"COLUMN_IDX\":3}}]}],\"TARGET_TABLE_NAME\":\"RI1\",\"TARGET_TABLE_ALIAS\":\"RI1\","
                        + "\"LOOKUP_TYPE\":\"EQ\",\"SORT_DIRECTION\":\"ASC\",\"TARGET_INDEX_NAME\":\"RI1_IND1\"}],"
                        + "\"EXECUTE_LIST\":[3,2,1],\"IS_LARGE_QUERY\":false}")
                .pass();
    }

}
