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
            PlanNodeType::Invalid,

            PlanNodeType::SeqScan,
            PlanNodeType::IndexScan,
            PlanNodeType::IndexCount,
            PlanNodeType::TableCount,
            PlanNodeType::MaterializedScan,
            PlanNodeType::TupleScan,

            PlanNodeType::Nestloop,
            PlanNodeType::NestloopIndex,

            PlanNodeType::Update,
            PlanNodeType::Insert,
            PlanNodeType::Delete,
            PlanNodeType::SwapTables,

            PlanNodeType::Send,
            PlanNodeType::Receive,
            PlanNodeType::MergeReceive,

            PlanNodeType::Aggregate,
            PlanNodeType::HashAggregate,
            PlanNodeType::Union,
            PlanNodeType::OrderBy,
            PlanNodeType::Projection,
            PlanNodeType::Materialize,
            PlanNodeType::Limit,
            PlanNodeType::PartialAggregate,
            PlanNodeType::WindowFunction,
            PlanNodeType::CommonTable
    };

    BOOST_FOREACH(PlanNodeType pnt, nodeTypes) {
        try {
            std::unique_ptr<AbstractPlanNode> node(plannodeutil::getEmptyPlanNode(pnt));
            ASSERT_NE(PlanNodeType::Invalid, pnt);
            ASSERT_NE(NULL, node.get());
        }
        catch (const voltdb::SerializableEEException& se) {
            ASSERT_EQ(PlanNodeType::Invalid, pnt);
        }
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
