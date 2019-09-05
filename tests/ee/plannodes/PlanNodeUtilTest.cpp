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

#include <memory>
#include <vector>

#include <boost/foreach.hpp>

#include "harness.h"

#include "plannodes/plannodeutil.h"



using namespace voltdb;

class PlanNodeUtilTest : public Test
{
};

/**
 * This is just a simple unit test that instantiates an empty plan
 * node for each node type.  If there are memory errors here
 * (such as uninitialized reads when the empty nodes are destructed)
 * then valgrind should catch them.
 */
TEST_F(PlanNodeUtilTest, getEmptyPlanNode) {

    std::vector<PlanNodeType> nodeTypes{
            PLAN_NODE_TYPE_INVALID,

            PLAN_NODE_TYPE_SEQSCAN,
            PLAN_NODE_TYPE_INDEXSCAN,
            PLAN_NODE_TYPE_INDEXCOUNT,
            PLAN_NODE_TYPE_TABLECOUNT,
            PLAN_NODE_TYPE_MATERIALIZEDSCAN,
            PLAN_NODE_TYPE_TUPLESCAN,

            PLAN_NODE_TYPE_NESTLOOP,
            PLAN_NODE_TYPE_NESTLOOPINDEX,

            PLAN_NODE_TYPE_UPDATE,
            PLAN_NODE_TYPE_INSERT,
            PLAN_NODE_TYPE_DELETE,
            PLAN_NODE_TYPE_SWAPTABLES,

            PLAN_NODE_TYPE_SEND,
            PLAN_NODE_TYPE_RECEIVE,
            PLAN_NODE_TYPE_MERGERECEIVE,

            PLAN_NODE_TYPE_AGGREGATE,
            PLAN_NODE_TYPE_HASHAGGREGATE,
            PLAN_NODE_TYPE_UNION,
            PLAN_NODE_TYPE_ORDERBY,
            PLAN_NODE_TYPE_PROJECTION,
            PLAN_NODE_TYPE_MATERIALIZE,
            PLAN_NODE_TYPE_LIMIT,
            PLAN_NODE_TYPE_PARTIALAGGREGATE,
            PLAN_NODE_TYPE_WINDOWFUNCTION,
            PLAN_NODE_TYPE_COMMONTABLE
    };

    BOOST_FOREACH(PlanNodeType pnt, nodeTypes) {
        try {
            std::unique_ptr<AbstractPlanNode> node(plannodeutil::getEmptyPlanNode(pnt));
            ASSERT_NE(PLAN_NODE_TYPE_INVALID, pnt);
            ASSERT_NE(NULL, node.get());
        }
        catch (const voltdb::SerializableEEException& se) {
            ASSERT_EQ(PLAN_NODE_TYPE_INVALID, pnt);
        }
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
