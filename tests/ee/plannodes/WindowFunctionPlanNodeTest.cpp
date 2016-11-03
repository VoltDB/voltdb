/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
#include "plannodes/windowfunctionnode.h"
#include "common/PlannerDomValue.h"
#include "expressions/tuplevalueexpression.h"

#include "harness.h"
using namespace voltdb;

namespace {
const char *jsonStrings[] = {
    "        {\n"
    "            \"AGGREGATE_COLUMNS\": [{\n"
    "                \"AGGREGATE_DISTINCT\": 0,\n"
    "                \"AGGREGATE_EXPRESSIONS\": [],\n"
    "                \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
    "                \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_RANK\",\n"
    "                \"PARTITIONBY_EXPRESSIONS\": [{\n"
    "                    \"COLUMN_IDX\": 0,\n"
    "                    \"TYPE\": 32,\n"
    "                    \"VALUE_TYPE\": 5\n"
    "                }],\n"
    "                \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
    "                    \"COLUMN_IDX\": 1,\n"
    "                    \"TYPE\": 32,\n"
    "                    \"VALUE_TYPE\": 5\n"
    "                }}]\n"
    "            }],\n"
    "            \"CHILDREN_IDS\": [4],\n"
    "            \"ID\": 3,\n"
    "            \"OUTPUT_SCHEMA\": [\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"R\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 6\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"A\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"B\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 1,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"C\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 2,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                }\n"
    "            ],\n"
    "            \"PLAN_NODE_TYPE\": \"WINDOWFUNCTION\"\n"
    "        }\n",
    (const char *)0
};
}

class WindowFunctionPlanNodeTest : public Test {
public:
    WindowFunctionPlanNodeTest()
    {
    }
};

//
// There is not much here to test.  The only difference between a
// WindowFunctionPlanNode and any other Aggregate node is that the
// WindowFunctionPLanNode generates one output row per input row.
TEST_F(WindowFunctionPlanNodeTest, TestJSON)
{
    for (int jsonIdx = 0; jsonStrings[jsonIdx]; jsonIdx += 1) {
        const char *jsonString = jsonStrings[jsonIdx];
        PlannerDomRoot root(jsonString);
        PlannerDomValue obj(root.rootObject());
        // If the json string is busted this will be true.
        EXPECT_FALSE(root.isNull());
        boost::shared_ptr<voltdb::WindowFunctionPlanNode> pn(dynamic_cast<WindowFunctionPlanNode*>(AbstractPlanNode::fromJSONObject(obj)));
        EXPECT_TRUE(pn.get() != NULL);
        EXPECT_EQ(1, pn->getAggregates().size());
        for (int aggIdx = 0; aggIdx < pn->getAggregates().size(); aggIdx += 1) {
        	EXPECT_EQ(EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK, pn->getAggregates()[aggIdx]);
        	EXPECT_EQ(1, pn->getPartitionByExpressions()[aggIdx].size());
        	EXPECT_EQ(1, pn->getOrderByExpressions()[aggIdx].size());
        	EXPECT_EQ(0, pn->getAggregateInputExpressions()[aggIdx].size());
        }
        EXPECT_EQ(4, pn->getOutputSchema().size());
        EXPECT_EQ("R", pn->getOutputSchema()[0]->getColumnName());
        EXPECT_EQ("A", pn->getOutputSchema()[1]->getColumnName());
        EXPECT_EQ("B", pn->getOutputSchema()[2]->getColumnName());
        EXPECT_EQ("C", pn->getOutputSchema()[3]->getColumnName());
        std::cout << "Window function node:\n"
        		  << pn->debug("")
				  << std::endl;
    }
}


int main()
{
    return TestSuite::globalInstance()->runAll();
}
