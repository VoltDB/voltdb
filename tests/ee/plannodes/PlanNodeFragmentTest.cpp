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

#include "plannodes/plannodefragment.h"

#include "harness.h"
#include "plannodes/abstractplannode.h"
#include "plannodes/deletenode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/sendnode.h"
#include "plannodes/seqscannode.h"

#include <sstream>

using namespace voltdb;
using namespace std;

class PlanNodeFragmentTest : public Test
{
public:
    PlanNodeFragmentTest()
    {
    }
};

TEST_F(PlanNodeFragmentTest, HasDeleteTrue)
{
    AbstractPlanNode* send_node = new SendPlanNode();
    send_node->setPlanNodeIdForTest(1);
    AbstractPlanNode* delete_node = new DeletePlanNode();
    delete_node->setPlanNodeIdForTest(2);
    AbstractPlanNode* seq_scan_node = new SeqScanPlanNode();
    seq_scan_node->setPlanNodeIdForTest(3);

    AbstractPlanNode* root1 = send_node;
    root1->addChild(delete_node);
    delete_node->addChild(seq_scan_node);
    PlanNodeFragment dut(root1);
    EXPECT_TRUE(dut.hasDelete());
}

TEST_F(PlanNodeFragmentTest, HasDeleteFalse)
{
    AbstractPlanNode* send_node = new SendPlanNode();
    send_node->setPlanNodeIdForTest(1);
    AbstractPlanNode* seq_scan_node = new SeqScanPlanNode();
    seq_scan_node->setPlanNodeIdForTest(2);

    AbstractPlanNode* root1 = send_node;
    root1->addChild(seq_scan_node);
    PlanNodeFragment dut(root1);
    EXPECT_FALSE(dut.hasDelete());
}

TEST_F(PlanNodeFragmentTest, HasDeleteInline)
{
    AbstractPlanNode* send_node = new SendPlanNode();
    send_node->setPlanNodeIdForTest(1);
    AbstractPlanNode* delete_node = new DeletePlanNode();
    delete_node->setPlanNodeIdForTest(2);
    AbstractPlanNode* index_scan_node = new IndexScanPlanNode();
    index_scan_node->setPlanNodeIdForTest(3);

    AbstractPlanNode* root1 = send_node;
    root1->addChild(index_scan_node);
    index_scan_node->addInlinePlanNode(delete_node);
    PlanNodeFragment dut(root1);
    EXPECT_TRUE(dut.hasDelete());
}

int main()
{
    return TestSuite::globalInstance()->runAll();
}
