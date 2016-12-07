/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "setopreceiveexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/setopreceivenode.h"
#include "execution/VoltDBEngine.h"
#include "execution/ProgressMonitorProxy.h"
#include "executors/setoperator.h"

// #include "executors/aggregateexecutor.h"
// #include "executors/executorutil.h"
// #include "storage/table.h"
// #include "storage/tablefactory.h"
// #include "storage/tableiterator.h"
// #include "storage/tableutil.h"
// #include "plannodes/receivenode.h"
// #include "plannodes/orderbynode.h"
// #include "plannodes/limitnode.h"


namespace voltdb {

SetOpReceiveExecutor::SetOpReceiveExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node)
{ }

bool SetOpReceiveExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init SetOpReceive Executor");

    SetOpReceivePlanNode* node = dynamic_cast<SetOpReceivePlanNode*>(abstract_node);
    assert(node != NULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

    //Create a temp table to collect tuples from multiple partitions
    TupleSchema* schema = m_abstractNode->generateTupleSchema();
    std::vector<std::string> column_names(schema->columnCount());
    m_tmpInputTable.reset(TableFactory::getTempTable(m_abstractNode->databaseId(),
                                                         "tempInput",
                                                         schema,
                                                         column_names,
                                                         limits));

    m_setOperator.reset(SetOperator::getSetOperator(/*node->getSetOpType()*/SetOpTyp.UNION, node->getInputTableRefs(), node->getTempOutputTable(), false));
    return true;
}

bool SetOpReceiveExecutor::p_execute(const NValueArray &params) {
    int loadedDeps = 0;

    // iterate over dependencies and merge them into the temp table.
    do {
        loadedDeps = m_engine->loadNextDependency(m_tmpInputTable.get());
    } while (loadedDeps > 0);

    ProgressMonitorProxy pmp(m_engine, this);


    VOLT_TRACE("Result of SetOpReceive:\n '%s'", m_tmpOutputTable->debug().c_str());

    cleanupInputTempTable(m_tmpInputTable.get());

    return true;
}

}
