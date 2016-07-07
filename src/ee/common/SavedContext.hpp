/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef _SAVEDCONTEXT_HPP_
#define _SAVEDCONTEXT_HPP_

#include "common/executorcontext.hpp"
#include "executors/abstractexecutor.h"
#include "execution/ExecutorVector.h"

#include <vector>
#include <map>

namespace voltdb {
class ExecutorContext;
class VoltDBEngine;
class TempTable;

/*
 * Saved state from the ExecutorContext and VoltDBEngine
 * Used to keep the state of paused transactions
 */
class SavedContext {
  public:
    SavedContext() :
        m_txnId(0),
        m_executedCtr(0),
        m_allTuplesScanned(0),
        m_tuplesProcessedInBatch(0),
        m_tuplesProcessedInFragment(0),
        m_tuplesProcessedSinceReport(0),
        m_currentIndexInBatch(0),
        m_inputParamPosition(),
        m_currentTxnTimestamp(0),
        m_currentDRTimestamp(0),
        m_uniqueId(0),
        m_outFileName(),
        m_outFileCount(0),
        m_currMemoryInBytes(0),
        m_peakMemoryInBytes(0),
        m_logThreshold(0),
        m_memoryLimit(0),
        m_tmpOutputTable(NULL) { }

    ~SavedContext() { }

    void initialize(
            int64_t allTuplesScanned,
            int64_t tuplesProcessedInBatch,
            int64_t tuplesProcessedInFragment,
            int64_t tuplesProcessedSinceReport,
            int64_t currentIndexInBatch,
            const char * inputParamPosition,
            ExecutorVector * currExecutorVec,
            ExecutorContext * executorContext
            ) {
        m_txnId = executorContext->currentTxnId();
        m_allTuplesScanned = allTuplesScanned;
        m_tuplesProcessedInBatch = tuplesProcessedInBatch;
        m_tuplesProcessedInFragment = tuplesProcessedInFragment;
        m_tuplesProcessedSinceReport = tuplesProcessedSinceReport;
        m_currentIndexInBatch = currentIndexInBatch;
        m_inputParamPosition = inputParamPosition;
        m_executedCtr = executorContext->executedCtr();
        m_currentTxnTimestamp = executorContext->currentTxnTimestamp();
        m_currentDRTimestamp = executorContext->currentDRTimestamp();
        m_outFileName = executorContext->lastOutFileName();
        m_outFileCount = executorContext->getOutFileCount();
        m_currMemoryInBytes = currExecutorVec->limits().getAllocated();
        m_peakMemoryInBytes = currExecutorVec->limits().getPeakMemoryInBytes();
        m_logThreshold = currExecutorVec->limits().getLogThreshold();
        m_memoryLimit = currExecutorVec->limits().getMemoryLimit();
        m_tmpOutputTable = executorContext->getExecutors(0)[m_executedCtr]->getOutputTempTable();
        m_uniqueId = executorContext->currentUniqueId();
    }
public:
    int64_t m_txnId;
    int m_executedCtr;
    /**
     * From VoltDBEngine
     */
    int64_t m_allTuplesScanned;
    int64_t m_tuplesProcessedInBatch;
    int64_t m_tuplesProcessedInFragment;
    int64_t m_tuplesProcessedSinceReport;
    int64_t m_currentIndexInBatch;

    const char * m_inputParamPosition;

    /**
     * Temp tables
     */
    /** buffer object for result tables. set when the result table is sent out to localsite. */
    FallbackSerializeOutput m_resultOutputSerializer;

    /**
     * From ExecutorContext
     */
    int64_t m_currentTxnTimestamp;
    int64_t m_currentDRTimestamp;
    int64_t m_uniqueId;

    std::string m_outFileName;
    int m_outFileCount;

    // From ExecutorVector
    int64_t m_currMemoryInBytes;
    int64_t m_peakMemoryInBytes;
    int64_t m_logThreshold;
    int64_t m_memoryLimit;
    // Temp Tables...output table (for now input table is a persistent table managed by a COW iterator)
    TempTable * m_tmpOutputTable;

};

}

#endif
