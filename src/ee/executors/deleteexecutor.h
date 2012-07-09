/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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

#ifndef HSTOREDELETEEXECUTOR_H
#define HSTOREDELETEEXECUTOR_H

#include <boost/scoped_ptr.hpp>

#include "common/common.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "executors/abstractexecutor.h"
#include "plannodes/deletenode.h"
#include "execution/VoltDBEngine.h"

namespace voltdb {

// Aggregate Struct to keep Executor state in between iteration
namespace detail
{
    struct DeleteExecutorState;
} //namespace detail

class TableIndex;

class DeletePlanNode;
class TempTable;
class PersistentTable;

class DeleteExecutor : public AbstractExecutor
{
public:
    DeleteExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node);

    bool support_pull() const;

protected:
    bool p_init(AbstractPlanNode*,
                TempTableLimits* limits);
    bool p_execute(const NValueArray &params);

    TableTuple p_next_pull();
    void p_pre_execute_pull(const NValueArray& params);
    void p_post_execute_pull();
    void p_insert_output_table_pull(TableTuple& tuple);

    DeletePlanNode* m_node;

    /** true if all tuples are deleted, truncate is the only case we
        don't need PK to delete tuples. */
    bool m_truncate;
    TempTable* m_inputTable;
    PersistentTable* m_targetTable;
    TableTuple m_inputTuple;
    TableTuple m_targetTuple;

    /** reference to the engine/context to store the number of
        modified tuples */
    VoltDBEngine* m_engine;

private:

    boost::scoped_ptr<detail::DeleteExecutorState> m_state;
};

}

#endif
