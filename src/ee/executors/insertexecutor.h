/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
#include "common/SerializableEEException.h"
#include "common/tabletuple.h"
#include "common/valuevector.h"
#include "executors/abstractexecutor.h"

namespace voltdb {

class InsertPlanNode;
class AbstractTempTable;

/**
 * This is the executor for insert nodes.
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
        m_hasStreamView(false),
        m_isUpsert(false),
        m_sourceIsPartitioned(false),
        m_hasPurgeFragment(false),
        m_templateTupleStorage(),
        m_nowFields(),
        m_targetTable(NULL),
        m_modifiedTuples(0),
        m_count_tuple(),
        m_persistentTable(NULL),
        m_upsertTuple(),
        m_templateTuple(),
        m_tempPool(NULL)
            {
            }

    /**
     * Return false iff all the work is done in init.  Inserting
     * a replicated table into an export table with no partition
     * column is done only on one site.  The rest of the sites
     * don't have any work to do.
     */
    bool p_execute_init(const TupleSchema *inputSchema,
                        AbstractTempTable *newOutputTable,
                        TableTuple &temp_tuple);

    /**
     * Insert a row into the target table and then count it.
     */
    void p_execute_tuple(TableTuple &tuple);

    /**
     * After all the rows are inserted into the target table
     * we insert one row into the output table with a count of
     * the number of rows we inserted into the target table.
     */
    void p_execute_finish();

    Table *getTargetTable() {
        return m_targetTable;
    }
 protected:
    bool p_init(AbstractPlanNode*,
                const ExecutorVector& executorVector);
    bool p_execute(const NValueArray &params);


    InsertPlanNode* m_node;
    AbstractTempTable* m_inputTable;

    int m_partitionColumn;
    bool m_multiPartition;
    bool m_isStreamed;
    bool m_hasStreamView;
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
    void executePurgeFragmentIfNeeded(PersistentTable** table);

    /**
     * Return false iff all the work is done in init.  Inserting
     * a replicated table into an export table with no partition
     * column is done only on one site.  The rest of the sites
     * don't have any work to do.
     */
    bool p_execute_init_internal(const TupleSchema *inputSchema,
                                 AbstractTempTable *newOutputTable,
                                 TableTuple &temp_tuple);
    /**
     * Insert a row into the target table and then count it.
     */
    void p_execute_tuple_internal(TableTuple &tuple);

    /** A tuple with the target table's schema that is populated
     * with default values for each field. */
    StandAloneTupleStorage m_templateTupleStorage;

    /** A memory pool for allocating non-inlined varchar and
     * varbinary default values */
    Pool m_memoryPool;

    /** A list of indexes of each column in the template tuple
     * that has a DEFAULT of NOW, which must be set on each
     * execution of this plan. */
    std::vector<int> m_nowFields;
    /*
     * These are logically local variables to p_execute.
     * But they are shared between p_execute and p_execute_init.
     */
    Table* m_targetTable;
    int64_t m_modifiedTuples;
    static int64_t s_modifiedTuples;
    TableTuple m_count_tuple;
    PersistentTable* m_persistentTable;
    TableTuple m_upsertTuple;
    TableTuple m_templateTuple;
    Pool* m_tempPool;
};

/**
 * Given an abstract plan node, extract an inline InsertExecutor
 * for its InlineInsertPlanNode if there be any.
 */
InsertExecutor *getInlineInsertExecutor(const AbstractPlanNode *node);
}

#endif
