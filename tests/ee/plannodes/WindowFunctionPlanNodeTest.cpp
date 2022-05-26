/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * WindowFunctionPlanNode.cpp
 *
 * Test the WindowFunctionPlanNode.  There is not much semantics here,
 * so we just test the json reading.
 */
#include <cstdarg>
#include "plannodes/windowfunctionnode.h"
#include "common/PlannerDomValue.h"
#include "expressions/tuplevalueexpression.h"

#include "harness.h"
using namespace voltdb;

namespace {
const char *jsonStrings[] = {
    "{ \"AGGREGATE_COLUMNS\":\n"
    "  [{ \"AGGREGATE_DISTINCT\": 0,\n"
    "     \"AGGREGATE_EXPRESSIONS\": [],\n"
    "     \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
    "     \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_RANK\" }\n"
    "  ],\n"
    "  \"CHILDREN_IDS\": [3],\n"
    "  \"ID\": 2,\n"
    "  \"OUTPUT_SCHEMA\": [\n"
    "      { \"COLUMN_NAME\": \"C1\",\n"
    "        \"EXPRESSION\": {\n"
    "            \"COLUMN_IDX\": 0,\n"
    "            \"TYPE\": 32,\n"
    "            \"VALUE_TYPE\": 6 } },\n"
    "      { \"COLUMN_NAME\": \"A\",\n"
    "        \"EXPRESSION\": {\n"
    "            \"COLUMN_IDX\": 0,\n"
    "            \"TYPE\": 32,\n"
    "            \"VALUE_TYPE\":5 } },\n"
    "      { \"COLUMN_NAME\": \"B\",\n"
    "        \"EXPRESSION\": {\n"
    "            \"COLUMN_IDX\": 1,\n"
    "            \"TYPE\": 32,\n"
    "            \"VALUE_TYPE\": 5 } } ],\n"
    "  \"PARTITIONBY_EXPRESSIONS\": [\n"
    "      { \"COLUMN_IDX\": 0,\n"
    "        \"TYPE\": 32,\n"
    "        \"VALUE_TYPE\": 5 }],\n"
    "  \"PLAN_NODE_TYPE\": \"WINDOWFUNCTION\",\n"
    "  \"SORT_COLUMNS\": [\n"
    "      {\"SORT_EXPRESSION\": {\n"
    "          \"COLUMN_IDX\": 1,\n"
    "          \"TYPE\": 32,\n"
    "          \"VALUE_TYPE\": 5 }}]\n"
    "}\n",
    "{\n"
    "    \"AGGREGATE_COLUMNS\": [{\n"
    "        \"AGGREGATE_DISTINCT\": 0,\n"
    "        \"AGGREGATE_EXPRESSIONS\": [],\n"
    "        \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
    "        \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_DENSE_RANK\"\n"
    "    }],\n"
    "    \"CHILDREN_IDS\": [5],\n"
    "    \"ID\": 4,\n"
    "    \"OUTPUT_SCHEMA\": [\n"
    "        {\n"
    "            \"COLUMN_NAME\": \"R\",\n"
    "            \"EXPRESSION\": {\n"
    "                \"COLUMN_IDX\": 0,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 6\n"
    "            }\n"
    "        },\n"
    "        {\n"
    "            \"COLUMN_NAME\": \"A\",\n"
    "            \"EXPRESSION\": {\n"
    "                \"COLUMN_IDX\": 0,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }\n"
    "        },\n"
    "        {\n"
    "            \"COLUMN_NAME\": \"B\",\n"
    "            \"EXPRESSION\": {\n"
    "                \"COLUMN_IDX\": 1,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }\n"
    "        },\n"
    "        {\n"
    "            \"COLUMN_NAME\": \"C\",\n"
    "            \"EXPRESSION\": {\n"
    "                \"COLUMN_IDX\": 2,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }\n"
    "        }\n"
    "    ],\n"
    "    \"PARTITIONBY_EXPRESSIONS\": [{\n"
    "        \"COLUMN_IDX\": 0,\n"
    "        \"TYPE\": 32,\n"
    "        \"VALUE_TYPE\": 5\n"
    "    }],\n"
    "    \"PLAN_NODE_TYPE\": \"WINDOWFUNCTION\",\n"
    "    \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
    "        \"COLUMN_IDX\": 1,\n"
    "        \"TYPE\": 32,\n"
    "        \"VALUE_TYPE\": 5\n"
    "    }}]\n"
    "}\n",
    (const char *)0
};
struct aggDescription {
    ExpressionType     aggType;
    int                aDistinct;
    int                aOutputColumn;
    int                nAggArgs;
};

struct OSchema {
    OSchema(const char *name0, ...)
    : m_numColumns(0) {
      va_list args;
      va_start(args, name0);

      m_columns[m_numColumns++] = name0;
      for (const char *cname = va_arg(args, const char *);
           cname != NULL;
           cname = va_arg(args, const char *)) {
        m_columns[m_numColumns++] = cname;
      }
      va_end(args);
    }
    const char * const*getColumns() const {
        return m_columns;
    }
    int getNumColumns() const {
    return m_numColumns;
    }
private:
    int         m_numColumns;
    const char *m_columns[100];
};

struct jsonDescription {
    int                 nAggs;
    aggDescription      aggDescr;
    int                 nPartitionByExprs;
    int                 nOrderByExprs;
    int                 nOutputColumns;
    OSchema             colDescriptions;
} jsonDescrs[] = {
    {
        1, /* nAggs */
        {EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK, 0, 0, 0},
        1, /* nPartitionByExprs */
        1, /* nOrderByExprs     */
        3, /* nOutputColumns    */
        OSchema(("C1"),
                ("A"),
                ("B"),
                NULL)
    },
    {
        1,  /* nAggs */
        {EXPRESSION_TYPE_AGGREGATE_WINDOWED_DENSE_RANK, 0, 0, 0},
        1,  /* nPartitionByExprs */
        1,  /* nOrderByExprs     */
        4,  /* nOutputColumns    */
        OSchema(("R"),
                ("A"),
                ("B"),
                ("C"),
                NULL)
    }
};

class WindowFunctionPlanNodeTest : public Test {
public:
    WindowFunctionPlanNodeTest()
    {
    }
};
}
//
// Test that the WindowFunctionPlanNode seems to have
// what we expect.
//
TEST_F(WindowFunctionPlanNodeTest, TestJSON)
{
    for (int jsonIdx = 0; jsonStrings[jsonIdx]; jsonIdx += 1) {
        std::cout << "Test " << jsonIdx << "\n";
        const char *jsonString = jsonStrings[jsonIdx];
        jsonDescription *jsonDescr = &jsonDescrs[jsonIdx];
        PlannerDomRoot root(jsonString);
        PlannerDomValue obj(root());
        // If the json string is busted this will be true.
        EXPECT_FALSE(root.isNull());
        boost::shared_ptr<voltdb::WindowFunctionPlanNode>
            pn(dynamic_cast<WindowFunctionPlanNode*>(AbstractPlanNode::fromJSONObject(obj).release()));
        EXPECT_TRUE(pn.get() != NULL);
        EXPECT_EQ(jsonDescr->nAggs, pn->getAggregates().size());
        for (int aggIdx = 0; aggIdx < pn->getAggregates().size(); aggIdx += 1) {
            aggDescription &aggDescr = jsonDescr->aggDescr;
            EXPECT_EQ(aggDescr.aggType, pn->getAggregates()[aggIdx]);
            EXPECT_EQ(aggDescr.nAggArgs, pn->getAggregateInputExpressions()[aggIdx].size());
        }
        EXPECT_EQ(jsonDescr->nPartitionByExprs, pn->getPartitionByExpressions().size());
        EXPECT_EQ(jsonDescr->nOrderByExprs, pn->getOrderByExpressions().size());
        EXPECT_EQ(jsonDescr->nOutputColumns, pn->getOutputSchema().size());
        EXPECT_EQ(jsonDescr->nOutputColumns, jsonDescr->colDescriptions.getNumColumns());
        auto columns = jsonDescr->colDescriptions.getColumns();
        for (int ocIdx = 0; ocIdx < jsonDescr->nOutputColumns; ocIdx += 1) {
            EXPECT_EQ(columns[ocIdx],
                      pn->getOutputSchema()[ocIdx]->getColumnName());
        }
    }
}


int main()
{
    return TestSuite::globalInstance()->runAll();
}
