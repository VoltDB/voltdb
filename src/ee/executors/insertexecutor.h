/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef HSTOREINSERTEXECUTOR_H
#define HSTOREINSERTEXECUTOR_H

#include "common/Pool.hpp"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/valuevector.h"
#include "executors/abstractexecutor.h"

namespace voltdb {

class InsertPlanNode;
class TempTable;

/**
 *
 */
class InsertExecutor : public AbstractExecutor
{
public:
    InsertExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
        : AbstractExecutor(engine, abstract_node),
        m_node(NULL),
        m_inputTable(NULL),
        m_partitionColumn(-1),
        m_multiPartition(false),
        m_isStreamed(false),
        m_isUpsert(false),
        m_sourceIsPartitioned(false),
        m_hasPurgeFragment(false),
        m_templateTuple(),
        m_memoryPool(),
        m_nowFields()
    {
    }

    protected:
        bool p_init(AbstractPlanNode*,
                    TempTableLimits* limits);
        bool p_execute(const NValueArray &params);

        InsertPlanNode* m_node;
        TempTable* m_inputTable;

        int m_partitionColumn;
        bool m_multiPartition;
        bool m_isStreamed;
        bool m_isUpsert;
        bool m_sourceIsPartitioned;
        bool m_hasPurgeFragment;

    private:

        /** If the table is at or over its tuple limit, this method
         * executes the purge fragment for the table.  Returns true if
         * nothing went wrong (regardless of whether the purge
         * fragment was executed) and false otherwise.
         *
         * The purge fragment might perform a truncate table,
         * in which case the persistent table object we're inserting
         * into might change.  Passing a pointer-to-pointer allows
         * the callee to update the persistent table pointer.
         */
        bool executePurgeFragmentIfNeeded(PersistentTable** table);

        /** A tuple with the target table's schema that is populated
         * with default values for each field. */
        StandAloneTupleStorage m_templateTuple;

        /** A memory pool for allocating non-inlined varchar and
         * varbinary default values */
        Pool m_memoryPool;

        /** A list of indexes of each column in the template tuple
         * that has a DEFAULT of NOW, which must be set on each
         * execution of this plan. */
        std::vector<int> m_nowFields;
};

}

#endif
