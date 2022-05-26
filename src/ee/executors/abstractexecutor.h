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

#include "common/InterruptException.h"
#include "common/tabletuple.h"
#include "common/types.h"
#include "execution/VoltDBEngine.h"
#include "plannodes/abstractplannode.h"
#include "storage/AbstractTempTable.hpp"
#include "common/SynchronizedThreadLock.h"

#include <common/debuglog.h>
#include <vector>

namespace voltdb {

class AbstractExpression;
class ExecutorVector;
class TempTableLimits;
class VoltDBEngine;

/**
 * AbstractExecutor provides the API for initializing and invoking executors.
 */
class AbstractExecutor {
  public:
    virtual ~AbstractExecutor();

    /** Executors are initialized once when the catalog is loaded */
    bool init(VoltDBEngine*, const ExecutorVector& executorVector);

    /** Invoke a plannode's associated executor */
    bool execute(const NValueArray& params);

    /** The temp output table for this executor.  May be an instance
     *  of either TempTable or LargeTempTable.  May be null for a SEND
     *  node!  */
    const AbstractTempTable* getTempOutputTable() const {
        return m_tmpOutputTable;
    }

    /**
     * Returns the plannode that generated this executor.
     */
    inline AbstractPlanNode* getPlanNode() { return m_abstractNode; }
    inline const AbstractPlanNode* getPlanNode() const { return m_abstractNode; }

    inline void cleanupTempOutputTable() {
        if (m_tmpOutputTable) {
            VOLT_TRACE("Clearing output table...");
            m_tmpOutputTable->deleteAllTuples();
        }
    }

    virtual void cleanupMemoryPool() {
        // LEAVE as blank on purpose
    }

    inline bool outputTempTableIsEmpty() const {
        if (m_tmpOutputTable != NULL) {
            return m_tmpOutputTable->activeTupleCount() == 0;
        } else {
            return true;
        }
    }

    inline void disableReplicatedFlag() {
        m_replicatedTableOperation = false;
    }

    // Compares two tuples based on the provided sets of expressions and sort directions
    struct TupleComparer {
        TupleComparer(const std::vector<AbstractExpression*>& keys,
                  const std::vector<SortDirectionType>& dirs);

        bool operator()(TableTuple ta, TableTuple tb) const;

    private:
        const std::vector<AbstractExpression*>& m_keys;
        const std::vector<SortDirectionType>& m_dirs;
        size_t m_keyCount;
    };

    // Return a string with useful debug info
    virtual std::string debug() const;

  protected:
    AbstractExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode) :
        m_abstractNode(abstractNode), m_engine(engine) { }

    /** Concrete executor classes implement initialization in p_init() */
    virtual bool p_init(AbstractPlanNode*, const ExecutorVector& executorVector) = 0;

    /** Concrete executor classes implement execution in p_execute() */
    virtual bool p_execute(const NValueArray& params) = 0;

    /**
     * Set up a multi-column temporary output table for those executors that require one.
     * Called from p_init.
     */
    void setTempOutputTable(const ExecutorVector& executorVector, const std::string tempTableName="temp");

    /**
     * Set up a single-column temporary output table for DML executors that require one to return their counts.
     * Called from p_init.
     */
    void setDMLCountOutputTable(TempTableLimits const* limits);

    // execution engine owns the plannode allocation.
    AbstractPlanNode* m_abstractNode;
    AbstractTempTable* m_tmpOutputTable = nullptr;

    /** reference to the engine to call up to the top end */
    VoltDBEngine* m_engine;

    /** when true, indicates that we should use the SynchronizedThreadLock for any OperationNode */
    bool m_replicatedTableOperation = false;
};


inline bool AbstractExecutor::execute(const NValueArray& params) {
    AbstractPlanNode *planNode = getPlanNode();
    VOLT_TRACE("Starting execution of plannode(id=%d)...",  planNode->getPlanNodeId());

    // run the executor
    bool executorSucceeded = p_execute(params);

    // For large queries, unpin the last tuple block so that it may be
    // stored on disk if needed.  (This is a no-op for normal temp
    // tables.)
    if (m_tmpOutputTable != NULL) {
        m_tmpOutputTable->finishInserts();
    }

    // Delete data from any temporary input tables to free up memory
    size_t inputTableCount = planNode->getInputTableCount();
    for (size_t i = 0; i < inputTableCount; ++i) {
        AbstractTempTable *table = dynamic_cast<AbstractTempTable*>(planNode->getInputTable(i));
        if (table != NULL && m_tmpOutputTable != table) {
            // For simple no-op sequential scan nodes, sometimes the
            // input table and output table are the same table, hence
            // the check above.
            table->deleteAllTuples();
        }
    }

    return executorSucceeded;
}

}

