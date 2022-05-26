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

#include "tuplescanexecutor.h"
#include "plannodes/tuplescannode.h"

using namespace voltdb;

bool TupleScanExecutor::p_init(AbstractPlanNode* abstract_node,
                               const ExecutorVector& executorVector)
{
    VOLT_TRACE("init TupleScan Executor");
    TupleScanPlanNode* node = dynamic_cast<TupleScanPlanNode*>(abstract_node);
    vassert(node);

    setTempOutputTable(executorVector, node->getTargetTableName());
    return true;
}

bool TupleScanExecutor::p_execute(const NValueArray &params) {
    TupleScanPlanNode* node = static_cast<TupleScanPlanNode*>(m_abstractNode);
    vassert(node == dynamic_cast<TupleScanPlanNode*>(m_abstractNode));
    Table* output_table = node->getOutputTable();
    vassert(output_table);
    AbstractTempTable* output_temp_table = dynamic_cast<AbstractTempTable*>(output_table);
    vassert(output_temp_table);

    TableTuple temp_tuple = output_temp_table->tempTuple();
    const std::vector<int>& paramIdxs = node->getParamIdxs();
    vassert(paramIdxs.size() == output_temp_table->schema()->columnCount());
    for (int i = 0; i < paramIdxs.size(); ++i)
    {
        temp_tuple.setNValue(i, params[paramIdxs[i]]);
    }

    output_temp_table->insertTempTuple(temp_tuple);
    VOLT_TRACE("\n%s\n", output_table->debug().c_str());
    VOLT_DEBUG("Finished Tuple scanning");

    return true;
}
