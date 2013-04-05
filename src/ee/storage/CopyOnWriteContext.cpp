/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
#include <algorithm>
#include <cassert>
#include <iostream>
#include <boost/exception/exception.hpp>

namespace voltdb {

/*
 * Recalculate how many tuples are remaining and compare to the countdown value.
 * This method does not work once we're in the middle of the temp table.
 * Only call it while m_finishedTableScan==false.
 */
void CopyOnWriteContext::checkRemainingTuples(const std::string &label) {
    assert(!m_finishedTableScan);
    intmax_t count1 = static_cast<CopyOnWriteIterator*>(m_iterator.get())->countRemaining();
    TableTuple tuple(m_table.schema());
    boost::scoped_ptr<TupleIterator> iter(m_backedUpTuples.get()->makeIterator());
    intmax_t count2 = 0;
    while (iter->next(tuple)) {
        count2++;
    }
    if (m_tuplesRemaining != count1 + count2) {
        VOLT_ERROR("CopyOnWriteContext::%s remaining tuple count mismatch: "
                   "table=%s partcol=%d count=%jd count1=%jd count2=%jd "
                   "expected=%jd compacted=%jd batch=%jd "
                   "inserts=%jd updates=%jd",
                   label.c_str(), m_table.name().c_str(), m_table.partitionColumn(),
                   count1 + count2, count1, count2, (intmax_t)m_tuplesRemaining,
                   (intmax_t)m_blocksCompacted, (intmax_t)m_serializationBatches,
                   (intmax_t)m_inserts, (intmax_t)m_updates);
    }
}

CopyOnWriteContext::CopyOnWriteContext(
        PersistentTable &table,
        TupleSerializer &serializer,
        int32_t partitionId,
        const std::vector<std::string> &predicateStrings,
        int32_t totalPartitions,
        int64_t totalTuples) :
             m_table(table),
             m_backedUpTuples(TableFactory::getCopiedTempTable(table.databaseId(),
                                                               "COW of " + table.name(),
                                                               &table, NULL)),
             m_serializer(serializer),
             m_pool(2097152, 320),
             m_blocks(m_table.m_data),
             m_iterator(new CopyOnWriteIterator(&table, m_blocks.begin(), m_blocks.end())),
             m_maxTupleLength(serializer.getMaxSerializedTupleSize(table.schema())),
             m_tuple(table.schema()),
             m_finishedTableScan(false),
             m_partitionId(partitionId),
             m_totalPartitions(totalPartitions),
             m_totalTuples(totalTuples),
             m_tuplesRemaining(totalTuples),
             m_blocksCompacted(0),
             m_serializationBatches(0),
             m_inserts(0),
             m_updates(0)
{
    // Parse predicate strings. The factory type determines the kind of
    // predicates that get generated.
    // Throws an exception to be handled by caller on errors.
    std::ostringstream errmsg;
    if (!m_predicates.parseStrings(predicateStrings, errmsg)) {
        throwFatalException("CopyOnWriteContext() failed to parse predicate strings.");
    }
}

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or -1 on error.
 */
int64_t CopyOnWriteContext::serializeMore(TupleOutputStreamProcessor &outputStreams) {
    if (!m_finishedTableScan) {
        checkRemainingTuples("serializeMore(start)");
    }

    // Don't expect to be re-called after streaming all the tuples.
    if (m_tuplesRemaining == 0) {
        throwFatalException("serializeMore() was called again after streaming completed.")
    }

    // Need to initialize the output stream list.
    if (outputStreams.empty()) {
        throwFatalException("serializeMore() expects at least one output stream.");
    }
    outputStreams.open(m_table, m_maxTupleLength, m_partitionId, m_predicates, m_totalPartitions);

    //=== Tuple processing loop

    TableTuple tuple(m_table.schema());

    // Set to true to break out of the loop after the tuples dry up
    // or the byte count threshold is hit.
    bool yield = false;
    while (!yield) {

        // Next tuple?
        if (m_iterator->next(tuple)) {

            // -1 is used as a sentinel value to disable counting for tests.
            if (m_tuplesRemaining > 0) {
                m_tuplesRemaining--;
            }

            /*
             * Write the tuple to all the output streams.
             * Done if any of the buffers filled up.
             */
            yield = outputStreams.writeRow(m_serializer, tuple);

            /*
             * If this is the table scan, check to see if the tuple is pending
             * delete and return the tuple if it is
             */
            if (!m_finishedTableScan && tuple.isPendingDelete()) {
                assert(!tuple.isPendingDeleteOnUndoRelease());
                if (m_table.m_schema->getUninlinedObjectColumnCount() != 0)
                {
                    m_table.decreaseStringMemCount(tuple.getNonInlinedMemorySize());
                }
                tuple.setPendingDeleteFalse();
                tuple.freeObjectColumns();
                CopyOnWriteIterator *iter = static_cast<CopyOnWriteIterator*>(m_iterator.get());
                //Save the extra lookup if possible
                m_table.deleteTupleStorage(tuple, iter->m_currentBlock);
            }

        } else if (!m_finishedTableScan) {
            /*
             * After scanning the persistent table switch to scanning the temp
             * table with the tuples that were backed up.
             */
            checkRemainingTuples("serializeMore(start temp)");
            m_finishedTableScan = true;
            m_iterator.reset(m_backedUpTuples.get()->makeIterator());

        } else {
            /*
             * No more tuples in the temp table and had previously finished the
             * persistent table.
             */
            if (m_tuplesRemaining > 0) {
                throwFatalException("serializeMore(): tuple count > 0 after streaming:\n"
                                    "Table name: %s\n"
                                    "Table type: %s\n"
                                    "Original tuple count: %jd\n"
                                    "Active tuple count: %jd\n"
                                    "Remaining tuple count: %jd\n"
                                    "Compacted block count: %jd\n"
                                    "Dirty insert count: %jd\n"
                                    "Dirty update count: %jd\n"
                                    "Partition column: %d\n",
                                    m_table.name().c_str(),
                                    m_table.tableType().c_str(),
                                    (intmax_t)m_totalTuples,
                                    (intmax_t)m_table.activeTupleCount(),
                                    (intmax_t)m_tuplesRemaining,
                                    (intmax_t)m_blocksCompacted,
                                    (intmax_t)m_inserts,
                                    (intmax_t)m_updates,
                                    m_table.partitionColumn());
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
            bool hasMore = m_iterator->next(tuple);
            if (!hasMore) {
                assert(false);
            }
            yield = true;
        }
    }
    // end tuple processing while loop

    // Need to close the output streams and insert row counts.
    outputStreams.close();

    m_serializationBatches++;

    // Handle the sentinel value of -1 which is passed in from tests that don't
    // care about the active tuple count. Return max int as if there are always
    // tuples remaining (until the counter is forced to zero when done).
    if (m_tuplesRemaining < 0) {
        return std::numeric_limits<int32_t>::max();
    }

    // Done when the table scan is finished and iteration is complete.
    return m_tuplesRemaining;
}

bool CopyOnWriteContext::canSafelyFreeTuple(TableTuple tuple) {
    if (tuple.isDirty() || m_finishedTableScan) {
        return true;
    }

    /**
     * Find out which block the address is contained in. Lower bound returns the first entry
     * in the index >= the address. Unless the address happens to be equal then the block
     * we are looking for is probably the previous entry. Then check if the address fits
     * in the previous entry. If it doesn't then the block is something new.
     */
    char *address = tuple.address();
    TBMapI i = m_blocks.lower_bound(address);
    if (i == m_blocks.end() && m_blocks.empty()) {
        return true;
    }
    if (i == m_blocks.end()) {
        i--;
        if (i.key() + m_table.m_tableAllocationSize < address) {
            return true;
        }
        //OK it is in the very last block
    } else {
        if (i.key() != address) {
            i--;
            if (i.key() + m_table.m_tableAllocationSize < address) {
                return true;
            }
            //OK... this is in this particular block
        }
    }

    const char *blockStartAddress = i.key();

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get());
    return !iter->needToDirtyTuple(blockStartAddress, address);
}

void CopyOnWriteContext::markTupleDirty(TableTuple tuple, bool newTuple) {
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
    char *address = tuple.address();
    TBMapI i =
                            m_blocks.lower_bound(address);
    if (i == m_blocks.end() && m_blocks.empty()) {
        tuple.setDirtyFalse();
        return;
    }
    if (i == m_blocks.end()) {
        i--;
        if (i.key() + m_table.m_tableAllocationSize < address) {
            tuple.setDirtyFalse();
            return;
        }
        //OK it is in the very last block
    } else {
        if (i.key() != address) {
            i--;
            if (i.key() + m_table.m_tableAllocationSize < address) {
                tuple.setDirtyFalse();
                return;
            }
            //OK... this is in this particular block
        }
    }

    const char *blockStartAddress = i.key();

    /**
     * Now check where this is relative to the COWIterator.
     */
    CopyOnWriteIterator *iter = reinterpret_cast<CopyOnWriteIterator*>(m_iterator.get());
    if (iter->needToDirtyTuple(blockStartAddress, address)) {
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

CopyOnWriteContext::~CopyOnWriteContext() {}

}
