/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "largeorderbyexecutor.h"
#include "execution/ExecutorVector.h"
#include "execution/ProgressMonitorProxy.h"
#include "plannodes/orderbynode.h"
#include "plannodes/limitnode.h"
#include "storage/LargeTempTable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

namespace voltdb {

bool
LargeOrderByExecutor::p_init(AbstractPlanNode* abstract_node,
                             const ExecutorVector& executorVector)
{
    OrderByPlanNode* node = dynamic_cast<OrderByPlanNode*>(abstract_node);
    vassert(node);

    // Order by nodes can be inlined into MergeReceive nodes,
    // but MP plans are not yet supported in large mode.
    vassert(!node->isInline());

    vassert(node->getInputTableCount() == 1);
    vassert(node->getChildren()[0] != NULL);

    // Our output table should look exactly like our input table
    node->setOutputTable(TableFactory::buildCopiedTempTable(node->getInputTable()->name(),
                                                            node->getInputTable(),
                                                            executorVector));
    vassert(dynamic_cast<LargeTempTable*>(node->getOutputTable()));

    // pick up an inlined limit, if one exists
    m_limitPlanNode = dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));

    return true;
}

bool
LargeOrderByExecutor::p_execute(const NValueArray &params) {
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);

    OrderByPlanNode* node = dynamic_cast<OrderByPlanNode*>(m_abstractNode);
    vassert(node);

    LargeTempTable* outputTable = dynamic_cast<LargeTempTable*>(node->getOutputTable());
    vassert(outputTable);

    LargeTempTable* inputTable = dynamic_cast<LargeTempTable*>(node->getInputTable());
    vassert(inputTable);

    int limit = -1;
    int offset = 0;
    if (m_limitPlanNode != NULL) {
        m_limitPlanNode->getLimitAndOffsetByReference(params, limit, offset);
    }

    inputTable->sort(&pmp,
                     AbstractExecutor::TupleComparer(node->getSortExpressions(), node->getSortDirections()),
                     limit,
                     offset);

    inputTable->swapContents(outputTable);

    return true;
}

LargeOrderByExecutor::~LargeOrderByExecutor() {
}

} // end namespace voltdb
