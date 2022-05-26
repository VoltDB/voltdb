/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include "storage/CopyOnWriteContext.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/CopyOnWriteIterator.h"
#include "storage/tableiterator.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/TupleOutputStream.h"
#include "common/FatalException.hpp"
#include "common/StreamPredicateList.h"
#include "logging/LogManager.h"
#include <algorithm>
#include <common/debuglog.h>
#include <iostream>

namespace voltdb {

/**
 * Constructor.
 */
CopyOnWriteContext::CopyOnWriteContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        int32_t partitionId,
        const HiddenColumnFilter &hiddenColumnFilter,
        const std::vector<std::string> &predicateStrings,
        int64_t totalTuples) :
             TableStreamerContext(table, surgeon, partitionId, predicateStrings),
             m_pool(2097152, 320),
             m_tuple(table.schema()),
             m_finishedTableScan(false),
             m_totalTuples(totalTuples),
             m_tuplesRemaining(totalTuples),
             m_blocksCompacted(0),
             m_serializationBatches(0),
             m_inserts(0),
             m_deletes(0),
             m_updates(0),
             m_skippedDirtyRows(0),
             m_skippedInactiveRows(0),
             m_replicated(table.isReplicatedTable()),
             m_hiddenColumnFilter(hiddenColumnFilter)
{
    if (m_replicated) {
        // There is a corner case where a replicated table is streamed from a thread other than the lowest
        // site thread. The only known case is rejoin snapshot where none of the target partitions are on
        // the lowest site thread.
        ScopedReplicatedResourceLock scopedLock;
        ExecuteWithMpMemory useMpMemory;
        m_backedUpTuples.reset(TableFactory::buildCopiedTempTable("COW of " + table.name(), &table));
    }
    else {
        m_backedUpTuples.reset(TableFactory::buildCopiedTempTable("COW of " + table.name(), &table));
    }
}

/**
 * Destructor.
 */
CopyOnWriteContext::~CopyOnWriteContext()
{
    if (m_replicated) {
        ScopedReplicatedResourceLock scopedLock;
        ExecuteWithMpMemory useMpMemory;
        m_backedUpTuples.reset();
    }
    else {
        m_backedUpTuples.reset();
    }
}


/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
CopyOnWriteContext::handleActivation(TableStreamType streamType)
{
    // Only support snapshot streams.
    if (streamType != TABLE_STREAM_SNAPSHOT) {
        return ACTIVATION_UNSUPPORTED;
    }

    if (m_surgeon.hasIndex() && !m_surgeon.isIndexingComplete()) {
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN,
            "COW context activation is not allowed while elastic indexing is in progress.");
        return ACTIVATION_FAILED;
    }

    m_surgeon.activateSnapshot();

    m_iterator.reset(new CopyOnWriteIterator(&getTable(), &m_surgeon));

    return ACTIVATION_SUCCEEDED;
}

/**
* Reactivation handler.
*/
TableStreamerContext::ActivationReturnCode
CopyOnWriteContext::handleReactivation(TableStreamType streamType)
{
    // Not support multiple snapshot streams.
    if (streamType == TABLE_STREAM_SNAPSHOT) {
     return ACTIVATION_FAILED;
    }
    return ACTIVATION_UNSUPPORTED;
}

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t CopyOnWriteContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                             std::vector<int> &retPositions) {
    vassert(m_iterator.get() != NULL);

    // Don't expect to be re-called after streaming all the tuples.
    if (m_totalTuples != 0 && m_tuplesRemaining == 0) {
        throwFatalException("serializeMore() was called again after streaming completed.")
    }

    // Need to initialize the output stream list.
    if (outputStreams.empty()) {
        throwFatalException("serializeMore() expects at least one output stream.");
    }
    outputStreams.open(getTable(),
                       getMaxTupleLength(),
                       getPartitionId(),
                       getPredicates(),
                       getPredicateDeleteFlags());

    //=== Tuple processing loop

    PersistentTable &table = getTable();
    TableTuple tuple(table.schema());
    {
        ConditionalExecuteWithMpMemoryAndScopedResourceLock lockInMpMemory(m_replicated);
        // Set to true to break out of the loop after the tuples dry up
        // or the byte count threshold is hit.
        bool yield = false;
        while (!yield) {

           // Next tuple?
           bool hasMore = m_iterator->next(tuple);
           if (hasMore) {

               // -1 is used as a sentinel value to disable counting for tests.
               if (m_tuplesRemaining > 0) {
                   m_tuplesRemaining--;
               }

               /*
                * Write the tuple to all the output streams.
                * Done if any of the buffers filled up.
                * The returned copy count helps decide when to delete if m_doDelete is true.
               */
               bool deleteTuple = false;
               yield = outputStreams.writeRow(tuple, m_hiddenColumnFilter, &deleteTuple);
               /*
                * May want to delete tuple if processing the actual table.
                */
               if (!m_finishedTableScan) {
                   /*
                   * If this is the table scan, check to see if the tuple is pending
                   * delete and return the tuple if it iscop
                   */
                   if (tuple.isPendingDelete()) {
                       vassert(!tuple.isPendingDeleteOnUndoRelease());
                       CopyOnWriteIterator *iter = static_cast<CopyOnWriteIterator*>(m_iterator.get());
                       //Save the extra lookup if possible
                       m_surgeon.deleteTupleStorage(tuple, iter->m_currentBlock);
                    }

                    /*
                     * Delete a moved tuple?
                     * This is used for Elastic rebalancing, which is wrapped in a transaction.
                     * The delete for undo is generic enough to support this operation.
                     */
                    else if (deleteTuple) {
                        m_surgeon.deleteTupleForUndo(tuple.address(), true);
                    }
              }

           } else if (!m_finishedTableScan) {
              /*
               * After scanning the persistent table switch to scanning the temp
               * table with the tuples that were backed up.
               */
               m_finishedTableScan = true;
               m_skippedDirtyRows = static_cast<CopyOnWriteIterator*>(m_iterator.get())->m_skippedDirtyRows;
               m_skippedInactiveRows = static_cast<CopyOnWriteIterator*>(m_iterator.get())->m_skippedInactiveRows;
               // Note that m_iterator no longer points to (or should reference) the CopyOnWriteIterator
               m_iterator.reset(new TableIterator(m_backedUpTuples->iterator()));
           } else {
               /*
                * No more tuples in the temp table and had previously finished the
                * persistent table.
                */
               size_t allPendingCnt = m_surgeon.getSnapshotPendingBlockCount();
               size_t pendingLoadCnt = m_surgeon.getSnapshotPendingLoadBlockCount();
               if (m_tuplesRemaining > 0 || allPendingCnt > 0 || pendingLoadCnt > 0) {

                   char message[1024 * 8];
                   snprintf(message, sizeof(message),
                         "serializeMore(): tuple count > 0 after streaming:\n"
                         "Table name: %s\n"
                         "Table type: %s\n"
                         "Original tuple count: %jd\n"
                         "Active tuple count: %jd\n"
                         "Remaining tuple count: %jd\n"
                         "Pending block count: %jd\n"
                         "Pending load block count: %jd\n"
                         "Compacted block count: %jd\n"
                         "Dirty insert count: %jd\n"
                         "Dirty delete count: %jd\n"
                         "Dirty update count: %jd\n"
                         "Partition column: %d\n"
                         "Skipped dirty rows: %d\n"
                         "Skipped inactive rows: %d\n",
                         table.name().c_str(),
                         table.tableType().c_str(),
                         (intmax_t)m_totalTuples,
                         (intmax_t)table.activeTupleCount(),
                         (intmax_t)m_tuplesRemaining,
                         (intmax_t)allPendingCnt,
                         (intmax_t)pendingLoadCnt,
                         (intmax_t)m_blocksCompacted,
                         (intmax_t)m_inserts,
                         (intmax_t)m_deletes,
                         (intmax_t)m_updates,
                         table.partitionColumn(),
                         m_skippedDirtyRows,
                         m_skippedInactiveRows);
                   message[sizeof message - 1] = '\0';
                   // If m_tuplesRemaining is not 0, we somehow corrupted the iterator. To make a best effort
                   // at continuing unscathed, we will make sure all the blocks are back in the non-pending snapshot
                   // lists and hope that the next snapshot handles everything correctly. We assume that the iterator
                   // at least returned it's currentBlock to the lists.
                   if (allPendingCnt > 0) {
                       // We have orphaned or corrupted some tables. Let's make them pristine.
                       TBMapI iter = m_surgeon.getData().begin();
                       while (iter != m_surgeon.getData().end()) {
                           m_surgeon.snapshotFinishedScanningBlock(iter.data(), TBPtr());
                           iter++;
                       }
                   }
                   if (!m_surgeon.blockCountConsistent()) {
                       throwFatalException("%s", message);
                   }
                   else {
                       LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, message);
                       m_tuplesRemaining = 0;
                       outputStreams.close();
                       for (size_t i = 0; i < outputStreams.size(); i++) {
                           retPositions.push_back((int)outputStreams.at(i).position());
                       }
                       return TABLE_STREAM_SERIALIZATION_ERROR;
                   }
               } else if (m_tuplesRemaining < 0)  {
                   // -1 is used for tests when we don't bother counting. Need to force it to 0 here.
                   m_tuplesRemaining = 0;
               }
           }

           // All tuples serialized, bail
           if (m_tuplesRemaining == 0) {
               /*
                * CAUTION: m_iterator->next() is NOT side-effect free!!! It also
                * returns the block back to the table if the call causes it to go
                * over the boundary of used tuples. In case it actually returned
                * the very last tuple in the table last time it's called, the block
                * is still hanging around. So we need to call it again to return
                * the block here.
                */

               VOLT_TRACE("serializeMore(): Finish streaming"
                                  "Table name: %s\n"
                                  "Table type: %s\n"
                                  "Original tuple count: %jd\n"
                                  "Active tuple count: %jd\n"
                                  "Remaining tuple count: %jd\n"
                                  "Compacted block count: %jd\n"
                                  "Dirty insert count: %jd\n"
                                  "Dirty delete count: %jd\n"
                                  "Dirty update count: %jd\n"
                                  "Partition column: %d\n",
                                  table.name().c_str(),
                                  table.tableType().c_str(),
                                  (intmax_t)m_totalTuples,
                                  (intmax_t)table.activeTupleCount(),
                                  (intmax_t)m_tuplesRemaining,
                                  (intmax_t)m_blocksCompacted,
                                  (intmax_t)m_inserts,
                                  (intmax_t)m_deletes,
                                  (intmax_t)m_updates,
                                  table.partitionColumn());

               if (hasMore) {
                   hasMore = m_iterator->next(tuple);
                   vassert(!hasMore);
               }
               yield = true;
           }
       }
    }
    // end tuple processing while loop

    // Need to close the output streams and insert row counts.
    outputStreams.close();
    // If more was streamed copy current positions for return.
    // Can this copy be avoided?
    for (size_t i = 0; i < outputStreams.size(); i++) {
        retPositions.push_back((int)outputStreams.at(i).position());
    }

    m_serializationBatches++;

    int64_t retValue = m_tuplesRemaining;

    // Handle the sentinel value of -1 which is passed in from tests that don't
    // care about the active tuple count. Return max int as if there are always
    // tuples remaining (until the counter is forced to zero when done).
    if (m_tuplesRemaining < 0) {
        retValue = std::numeric_limits<int64_t>::max();
    }

    // Done when the table scan is finished and iteration is complete.
    return retValue;
}

int64_t CopyOnWriteContext::getRemainingCount() {
    return m_tuplesRemaining;
}

bool CopyOnWriteContext::notifyTupleDelete(TableTuple &tuple) {
    vassert(m_iterator.get() != NULL);

    if (tuple.isDirty() || m_finishedTableScan) {
        return true;
    }
    // This is a 'loose' count of the number of deletes because COWIterator could be past this
    // point in the block.
    m_deletes++;

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = static_cast<CopyOnWriteIterator*>(m_iterator.get());
    if (iter->needToDirtyTuple(tuple.address())) {
        // For replicated table
        // preserve the deleted tuples to tempTable instead of mark deletePending
        if (m_replicated) {
            m_backedUpTuples->insertTempTupleDeepCopy(tuple, &m_pool);
            return true;
        } else {
            return false;
        }
    } else {
        return true;
    }
}

void CopyOnWriteContext::markTupleDirty(TableTuple tuple, bool newTuple) {
    vassert(m_iterator.get() != NULL);

    /**
     * If this an update or a delete of a tuple that is already dirty then no further action is
     * required.
     */
    if (!newTuple && tuple.isDirty()) {
        return;
    }

    /**
     * If the table has been scanned already there is no need to continue marking tuples dirty
     * If the tuple is dirty then it has already been backed up.
     */
    if (m_finishedTableScan) {
        tuple.setDirtyFalse();
        return;
    }

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = static_cast<CopyOnWriteIterator*>(m_iterator.get());
    if (iter->needToDirtyTuple(tuple.address())) {
        tuple.setDirtyTrue();

        if (newTuple) {
            /**
             * Don't back up a newly introduced tuple, just mark it as dirty.
             */
            m_inserts++;
        }
        else {
            m_updates++;
            m_backedUpTuples->insertTempTupleDeepCopy(tuple, &m_pool);
        }
    } else {
        tuple.setDirtyFalse();
        return;
    }
}

void CopyOnWriteContext::notifyBlockWasCompactedAway(TBPtr block) {
    vassert(m_iterator.get() != NULL);
    if (m_finishedTableScan) {
        // There was a compaction while we are iterating through the m_backedUpTuples
        // TempTable. Don't do anything because the passed in block is a PersistentTable
        // block
        return;
    }
    m_blocksCompacted++;
    CopyOnWriteIterator *iter = static_cast<CopyOnWriteIterator*>(m_iterator.get());
        TBPtr nextBlock = iter->m_blockIterator.data();
                TBPtr newNextBlock = iter->m_blockIterator.data();
    iter->notifyBlockWasCompactedAway(block);
}

bool CopyOnWriteContext::notifyTupleInsert(TableTuple &tuple) {
    markTupleDirty(tuple, true);
    return true;
}

bool CopyOnWriteContext::notifyTupleUpdate(TableTuple &tuple) {
    markTupleDirty(tuple, false);
    return true;
}

/*
 * Recalculate how many tuples are remaining and compare to the countdown value.
 * This method does not work once we're in the middle of the temp table.
 * Only call it while m_finishedTableScan==false.
 */
void CopyOnWriteContext::checkRemainingTuples(const std::string &label) {
    vassert(m_iterator.get() != NULL);
    vassert(!m_finishedTableScan);
    intmax_t count1 = static_cast<CopyOnWriteIterator*>(m_iterator.get())->countRemaining();
    TableTuple tuple(getTable().schema());
    TableIterator iter = m_backedUpTuples->iterator();
    intmax_t count2 = 0;
    while (iter.next(tuple)) {
        count2++;
    }
    if (m_tuplesRemaining != count1 + count2) {
        char errMsg[1024 * 16];
        snprintf(errMsg, sizeof errMsg,
                 "CopyOnWriteContext::%s remaining tuple count mismatch: "
                 "table=%s partcol=%d count=%jd count1=%jd count2=%jd "
                 "expected=%jd compacted=%jd batch=%jd "
                 "inserts=%jd updates=%jd",
                 label.c_str(), getTable().name().c_str(), getTable().partitionColumn(),
                 count1 + count2, count1, count2, (intmax_t)m_tuplesRemaining,
                 (intmax_t)m_blocksCompacted, (intmax_t)m_serializationBatches,
                 (intmax_t)m_inserts, (intmax_t)m_updates);
        errMsg[sizeof errMsg - 1] = '\0';
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, errMsg);
    }
}

}
