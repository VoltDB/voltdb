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
 * PartitionByPlanNode.cpp
 *
 * Test the PartitionByPlanNode.  There is not much semantics here,
 * so we just test the json reading.
 */
#include "plannodes/partitionbynode.h"
#include "common/PlannerDomValue.h"
#include "expressions/tuplevalueexpression.h"

#include "harness.h"
using namespace voltdb;

namespace {
const char *jsonStrings[] = {
                "{\"ID\":1, "
                 "\"PLAN_NODE_TYPE\":\"PARTITIONBY\", "
                 "\"AGGREGATE_COLUMNS\":[], "
                 "\"GROUPBY_EXPRESSIONS\":[{\"TYPE\":32, "
                                           "\"VALUE_TYPE\":8, "
                                           "\"VALUE_SIZE\":8, "
                                           "\"COLUMN_IDX\":1}, "
                                          "{\"TYPE\":32, "
                                          "\"VALUE_TYPE\":5, "
                                          "\"VALUE_SIZE\":4, "
                                          "\"COLUMN_IDX\":2}], "
                 "\"SORT_COLUMNS\":[{\"SORT_EXPRESSION\": "
                                       "{\"TYPE\":32, "
                                       "\"VALUE_TYPE\":8, "
                                       "\"VALUE_SIZE\":8, "
                                       "\"COLUMN_IDX\":1}, "
                                     "\"SORT_DIRECTION\":\"ASC\"}, "
                                    "{\"SORT_EXPRESSION\": "
                                       "{\"TYPE\":32, "
                                       "\"VALUE_TYPE\":5, "
                                       "\"VALUE_SIZE\":4, "
                                       "\"COLUMN_IDX\":2}, "
                                      "\"SORT_DIRECTION\":\"DESC\"}]}",
                (const char *)0
};
}

class PartitionByPlanNodeTest : public Test {
public:
    PartitionByPlanNodeTest()
    {
    }
};

TEST_F(PartitionByPlanNodeTest, TestJSON)
{
    for (int idx = 0; jsonStrings[idx]; idx += 1) {
        const char *jsonString = jsonStrings[idx];
        PlannerDomRoot root(jsonString);
        PlannerDomValue obj(root.rootObject());
        boost::shared_ptr<voltdb::PartitionByPlanNode> pn(dynamic_cast<PartitionByPlanNode*>(AbstractPlanNode::fromJSONObject(obj)));
        EXPECT_TRUE(pn.get() != NULL);
        const std::vector<AbstractExpression*> &partitionByExprs = pn->getGroupByExpressions();
        const std::vector<AbstractExpression*> &sortExprs = pn->getSortExpressions();
        const std::vector<SortDirectionType> &sortDirs = pn->getSortDirections();
        EXPECT_TRUE(partitionByExprs.size() == 2);
        for (int idx = 0; idx < partitionByExprs.size(); idx += 1) {
            TupleValueExpression *tve = dynamic_cast<TupleValueExpression*>(partitionByExprs[idx]);
            EXPECT_TRUE(tve != NULL);
            // This three are all true because of collusion in the
            // construction of the JSON.
            EXPECT_TRUE(tve->getColumnId() == idx + 1);
            EXPECT_TRUE(tve->getValueType() == ((idx == 0) ? 8 : 5));
            EXPECT_TRUE(tve->getValueSize() == ((idx == 0) ? 8 : 4));
        }
        EXPECT_TRUE(sortExprs.size() == sortDirs.size());
        EXPECT_TRUE(sortDirs[0] == SORT_DIRECTION_TYPE_ASC);
        EXPECT_TRUE(sortDirs[1] == SORT_DIRECTION_TYPE_DESC);
        for (int idx = 0; idx < partitionByExprs.size(); idx += 1) {
            TupleValueExpression *tve = dynamic_cast<TupleValueExpression*>(sortExprs[idx]);
            EXPECT_TRUE(tve != NULL);
            // This is true, again, because of collusion in the
            // construction of the JSON.
            EXPECT_TRUE(tve->getColumnId() == idx + 1);
            EXPECT_TRUE(tve->getValueType() == ((idx == 0) ? 8 : 5));
            EXPECT_TRUE(tve->getValueSize() == ((idx == 0) ? 8 : 4));
        }
    }
}


int main()
{
    return TestSuite::globalInstance()->runAll();
}
