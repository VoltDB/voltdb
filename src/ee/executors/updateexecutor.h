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

#pragma once

#include <vector>

#include "common/common.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"
#include "execution/VoltDBEngine.h"

namespace voltdb {

class TableIndex;

class UpdatePlanNode;
class AbstractTempTable;
class PersistentTable;

class UpdateExecutor : public AbstractExecutor {
    UpdatePlanNode* m_node = nullptr;
    std::vector<std::pair<int, int>> m_inputTargetMap{};
    int m_inputTargetMapSize = -1;
    AbstractTempTable* m_inputTable = nullptr;
    TableTuple m_inputTuple{};
    int m_partitionColumn = -1;
    static int64_t s_modifiedTuples;

    bool p_init(AbstractPlanNode*, const ExecutorVector& executorVector);
    bool p_execute(const NValueArray &params);
public:
    UpdateExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
        : AbstractExecutor(engine, abstract_node) { }
};

}

