/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "common/TupleOutputStream.h"
#include "common/FatalException.hpp"
#include "common/StreamPredicateList.h"
#include "logging/LogManager.h"
#include <algorithm>
#include <cassert>
#include <iostream>

namespace voltdb {

/**
 * Constructor.
 */
CopyOnWriteContext::CopyOnWriteContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        TupleSerializer &serializer,
        int32_t partitionId,
        const std::vector<std::string> &predicateStrings,
        int64_t totalTuples) :
             TableStreamerContext(table, surgeon, partitionId, serializer, predicateStrings),
             m_backedUpTuples(TableFactory::getCopiedTempTable(table.databaseId(),
                                                               "COW of " + table.name(),
                                                               &table, NULL)),
             m_pool(2097152, 320),
             m_blocks(surgeon.getData()),
             m_tuple(table.schema()),
             m_finishedTableScan(false),
             m_totalTuples(totalTuples),
             m_tuplesRemaining(totalTuples),
             m_blocksCompacted(0),
             m_serializationBatches(0),
             m_inserts(0),
             m_updates(0)
{
}

/**
 * Destructor.
 */
CopyOnWriteContext::~CopyOnWriteContext()
{}


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

    m_iterator.reset(new CopyOnWriteIterator(&getTable(), &m_surgeon, m_blocks));

    return ACTIVATION_SUCCEEDED;
}

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t CopyOnWriteContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                             std::vector<int> &retPositions) {
    assert(m_iterator != NULL);

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
            yield = outputStreams.writeRow(getSerializer(), tuple, &deleteTuple);
            /*
             * May want to delete tuple if processing the actual table.
             */
            if (!m_finishedTableScan) {
                /*
                 * If this is the table scan, check to see if the tuple is pending
                 * delete and return the tuple if it iscop
                 */
                if (tuple.isPendingDelete()) {
                    assert(!tuple.isPendingDeleteOnUndoRelease());
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
            m_iterator.reset(m_backedUpTuples.get()->makeIterator());

        } else {
            /*
             * No more tuples in the temp table and had previously finished the
             * persistent table.
             */
            if (m_tuplesRemaining > 0) {
                int32_t skippedDirtyRows = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get())->m_skippedDirtyRows;
                int32_t skippedInactiveRows = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get())->m_skippedInactiveRows;

                /*
                 * Start with fresh iterators and count how many rows are visible
                 * to hint at why the right number of rows may not have been processed
                 */
                m_iterator.reset(new CopyOnWriteIterator(&getTable(), &m_surgeon, m_blocks));
                int32_t cowCount = 0;
                while (m_iterator->next(tuple)) {
                    cowCount++;
                }

                int32_t iterationCount = 0;
                TupleIterator *iter = table.makeIterator();
                while (iter->next(tuple)) {
                    iterationCount++;
                }
                delete iter;

                char message[1024 * 16];
                snprintf(message, 1024 * 16,
                         "serializeMore(): tuple count > 0 after streaming:\n"
                         "Table name: %s\n"
                         "Table type: %s\n"
                         "Original tuple count: %jd\n"
                         "Active tuple count: %jd\n"
                         "Remaining tuple count: %jd\n"
                         "Compacted block count: %jd\n"
                         "Dirty insert count: %jd\n"
                         "Dirty update count: %jd\n"
                         "Partition column: %d\n"
                         "Skipped dirty rows: %d\n"
                         "Skipped inactive rows: %d\n"
                         "Discovered COW rows: %d\n"
                         "Discovered iteration rows: %d\n",
                         table.name().c_str(),
                         table.tableType().c_str(),
                         (intmax_t)m_totalTuples,
                         (intmax_t)table.activeTupleCount(),
                         (intmax_t)m_tuplesRemaining,
                         (intmax_t)m_blocksCompacted,
                         (intmax_t)m_inserts,
                         (intmax_t)m_updates,
                         table.partitionColumn(),
                         skippedDirtyRows,
                         skippedInactiveRows,
                         cowCount,
                         iterationCount);
                // Use a format string to prevent overzealous compiler warnings.
                throwFatalException("%s", message);
            }
            // -1 is used for tests when we don't bother counting. Need to force it to 0 here.
            if (m_tuplesRemaining < 0)  {
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
            if (hasMore) {
                hasMore = m_iterator->next(tuple);
                if (hasMore) {
                    assert(false);
                }
            }
            yield = true;
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

bool CopyOnWriteContext::notifyTupleDelete(TableTuple &tuple) {
    assert(m_iterator != NULL);

    if (tuple.isDirty() || m_finishedTableScan) {
        return true;
    }

    /**
     * Find out which block the address is contained in. Lower bound returns the first entry
     * in the index >= the address. Unless the address happens to be equal then the block
     * we are looking for is probably the previous entry. Then check if the address fits
     * in the previous entry. If it doesn't then the block is something new.
     */
    TBPtr block = PersistentTable::findBlock(tuple.address(), m_blocks, getTable().getTableAllocationSize());
    if (block.get() == NULL) {
        // tuple not in snapshot region, don't care about this tuple
        return true;
    }

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get());
    return !iter->needToDirtyTuple(block->address(), tuple.address());
}

void CopyOnWriteContext::markTupleDirty(TableTuple tuple, bool newTuple) {
    assert(m_iterator != NULL);

    if (newTuple) {
        m_inserts++;
    }
    else {
        m_updates++;
    }

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
     * Find out which block the address is contained in.
     */
    TBPtr block = PersistentTable::findBlock(tuple.address(), m_blocks, getTable().getTableAllocationSize());
    if (block.get() == NULL) {
        // tuple not in snapshot region, don't care about this tuple, no need to dirty it
        tuple.setDirtyFalse();
        return;
    }

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get());
    if (iter->needToDirtyTuple(block->address(), tuple.address())) {
        tuple.setDirtyTrue();
        /**
         * Don't back up a newly introduced tuple, just mark it as dirty.
         */
        if (!newTuple) {
            m_backedUpTuples->insertTupleNonVirtualWithDeepCopy(tuple, &m_pool);
        }
    } else {
        tuple.setDirtyFalse();
        return;
    }
}

void CopyOnWriteContext::notifyBlockWasCompactedAway(TBPtr block) {
    assert(m_iterator != NULL);
    assert(!m_finishedTableScan);
    m_blocksCompacted++;
    CopyOnWriteIterator *iter = static_cast<CopyOnWriteIterator*>(m_iterator.get());
    if (iter->m_blockIterator != m_blocks.end()) {
        TBPtr nextBlock = iter->m_blockIterator.data();
        //The next block is the one that was compacted away
        //Need to move the iterator forward to skip it
        if (nextBlock == block) {
            iter->m_blockIterator++;

            //There is another block after the one that was compacted away
            if (iter->m_blockIterator != m_blocks.end()) {
                TBPtr newNextBlock = iter->m_blockIterator.data();
                m_blocks.erase(block->address());
                iter->m_blockIterator = m_blocks.find(newNextBlock->address());
                iter->m_end = m_blocks.end();
                assert(iter->m_blockIterator != m_blocks.end());
            } else {
                //No block after the one compacted away
                //set everything to end
                m_blocks.erase(block->address());
                iter->m_blockIterator = m_blocks.end();
                iter->m_end = m_blocks.end();
            }
        } else {
            //Some random block was compacted away. Remove it and regenerate the iterator
            m_blocks.erase(block->address());
            iter->m_blockIterator = m_blocks.find(nextBlock->address());
            iter->m_end = m_blocks.end();
            assert(iter->m_blockIterator != m_blocks.end());
        }
    }
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
    assert(m_iterator != NULL);
    assert(!m_finishedTableScan);
    intmax_t count1 = static_cast<CopyOnWriteIterator*>(m_iterator.get())->countRemaining();
    TableTuple tuple(getTable().schema());
    boost::scoped_ptr<TupleIterator> iter(m_backedUpTuples.get()->makeIterator());
    intmax_t count2 = 0;
    while (iter->next(tuple)) {
        count2++;
    }
    if (m_tuplesRemaining != count1 + count2) {
        char errMsg[1024 * 16];
        snprintf(errMsg, 1024 * 16,
                 "CopyOnWriteContext::%s remaining tuple count mismatch: "
                 "table=%s partcol=%d count=%jd count1=%jd count2=%jd "
                 "expected=%jd compacted=%jd batch=%jd "
                 "inserts=%jd updates=%jd",
                 label.c_str(), getTable().name().c_str(), getTable().partitionColumn(),
                 count1 + count2, count1, count2, (intmax_t)m_tuplesRemaining,
                 (intmax_t)m_blocksCompacted, (intmax_t)m_serializationBatches,
                 (intmax_t)m_inserts, (intmax_t)m_updates);
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, errMsg);
    }
}

}
