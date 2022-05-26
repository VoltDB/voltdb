/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#include "receiveexecutor.h"
#include "plannodes/receivenode.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"

namespace voltdb {

bool ReceiveExecutor::p_init(AbstractPlanNode* abstract_node,
                             const ExecutorVector& executorVector)
{
    VOLT_TRACE("init Receive Executor");

    vassert(dynamic_cast<ReceivePlanNode*>(abstract_node));

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);
    return true;
}

bool ReceiveExecutor::p_execute(const NValueArray &params) {
    int loadedDeps = 0;
    ReceivePlanNode* node = dynamic_cast<ReceivePlanNode*>(m_abstractNode);
    Table* output_table = dynamic_cast<Table*>(node->getOutputTable());

    // iterate dependencies stored in the frontend and union them
    // into the output_table. The engine does this work for peanuts.

    // todo: should pass the transaction's string pool through
    // as the underlying table loader would use it.
    do {
        loadedDeps =
        engine->loadNextDependency(output_table);
    } while (loadedDeps > 0);

    return true;
}

ReceiveExecutor::~ReceiveExecutor() {
}

}
