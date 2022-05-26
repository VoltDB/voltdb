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

#include <chrono>
#include <cstdlib>

#include "common/LargeTempTableBlockId.hpp"
#include "common/LargeTempTableBlockCache.h"
#include "execution/ProgressMonitorProxy.h"
#include "storage/LargeTempTable.h"
#include "storage/LargeTempTableBlock.h"
#include "storage/tablefactory.h"

namespace voltdb {

LargeTempTable::LargeTempTable() : AbstractTempTable(LargeTempTableBlock::BLOCK_SIZE_IN_BYTES) { }

void LargeTempTable::getEmptyBlock() {
    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    // Mark the current block we're writing to as unpinned so it can
    // be stored if needed to make space for the next block.
    if (m_blockForWriting != NULL) {
        m_blockForWriting->unpin();
    }

    // Try to get an empty block (this will invoke I/O via topend, and
    // could throw for any number of reasons)
    LargeTempTableBlock* newBlock = lttBlockCache.getEmptyBlock(m_schema);

    m_blockForWriting = newBlock;
    m_blockIds.push_back(m_blockForWriting->id());
}

bool LargeTempTable::insertTuple(TableTuple& source) {

    if (m_blockForWriting == NULL) {
        if (! m_blockIds.empty()) {
            throwSerializableEEException("Attempt to insert after finishInserts() called");
        }

        getEmptyBlock();
    }

    bool success = m_blockForWriting->insertTuple(source);
    if (! success) {
        if (m_blockForWriting->activeTupleCount() == 0) {
            throwSerializableEEException("Failed to insert tuple into empty LTT block");
        }

        // Try again, maybe there will be enough space with an empty block.
        getEmptyBlock();
        success = m_blockForWriting->insertTuple(source);
        if (! success) {
            throwSerializableEEException("Failed to insert tuple into empty LTT block");
        }
    }

    ++m_tupleCount;

    return true;
}

void LargeTempTable::finishInserts() {
    if (m_blockForWriting) {
        vassert(m_blockIds.size() > 0 && m_blockIds.back() == m_blockForWriting->id());
        if (m_blockForWriting->isPinned()) {
            // In general, if m_blockForWriting is not null, then the
            // block it points to will be pinned.  The only case where
            // this is not true is when we throw an exception
            // attempting to fetch a new empty block.
            m_blockForWriting->unpin();
        }
        m_blockForWriting = NULL;
    }
}

TableIterator LargeTempTable::iterator() {
    if (m_blockForWriting != NULL) {
        throwSerializableEEException("Attempt to iterate over large temp table before finishInserts() is called");
    }

    return TableIterator(this, m_blockIds.begin(), false);
}

TableIterator LargeTempTable::iteratorDeletingAsWeGo() {
    if (m_blockForWriting != NULL) {
        throwSerializableEEException("Attempt to iterate over large temp table before finishInserts() is called");
    }

    return TableIterator(this, m_blockIds.begin(), true);
}


void LargeTempTable::deleteAllTuples() {
    finishInserts();

    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    for(auto blockId : m_blockIds) {
        lttBlockCache.releaseBlock(blockId);
    }

    m_blockIds.clear();
    m_tupleCount = 0;
}

std::vector<LargeTempTableBlockId>::iterator LargeTempTable::releaseBlock(
        std::vector<LargeTempTableBlockId>::iterator it) {
    if (it == m_blockIds.end()) {
        // block may have already been deleted
        return it;
    }

    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    m_tupleCount -= lttBlockCache.getBlockTupleCount(*it);
    lttBlockCache.releaseBlock(*it);

    return m_blockIds.erase(it);
}

void LargeTempTable::swapContents(AbstractTempTable* otherTable) {
    vassert(dynamic_cast<LargeTempTable*>(otherTable));
    LargeTempTable* otherLargeTable = static_cast<LargeTempTable*>(otherTable);

    if (m_blockForWriting || otherLargeTable->m_blockForWriting) {
        throwSerializableEEException("Please only swap large temp tables after finishInserts has been called");
    }

    m_blockIds.swap(otherLargeTable->m_blockIds);
    std::swap(m_tupleCount, otherLargeTable->m_tupleCount);
}

LargeTempTable::~LargeTempTable() {
    deleteAllTuples();
}

void LargeTempTable::nextFreeTuple(TableTuple*) {
    throwSerializableEEException("nextFreeTuple not implemented");
}

std::string LargeTempTable::debug(const std::string& spacer) const {
    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    std::ostringstream oss;
    oss << Table::debug(spacer);
    std::string infoSpacer = spacer + "  |";
    oss << infoSpacer << "\tLTT BLOCK IDS (" << m_blockIds.size() << " blocks):\n";
    if (m_blockIds.size() > 0) {
        for(auto id : m_blockIds) {
            oss << infoSpacer;
            LargeTempTableBlock *block = lttBlockCache.getBlockForDebug(id);
            if (block != NULL) {
                oss << "   " << block->debug();
            } else {
                oss << "   block " << id << " is not in LTT block cache?!";
            }

            oss << "\n";
        }
    } else {
        oss << infoSpacer << "  <no blocks>\n";
    }

    return oss.str();
}


// Sorting-related classes and methods below

namespace {

/**
 * This class sorts a single large temp table block.
 *
 * Depending on the table's schema it may choose different ways of
 * sorting.
 *
 * If there all columns have inlined data, then it can be faster to
 * sort out-of-place, by sorting instances of TableTuples (16-byte
 * objects that are a pointer to tuple storage and a pointer to tuple
 * schema), and then copying the tuples to a new block in the sorted
 * order.
 *
 * If there non-inlined columns then there is an advantage to sorting
 * in place because all the non-inlined values can be left where they
 * are.  In this case we do an in-place quicksort, and swap the
 * position of tuples by copying tuple storage.
 *
 * If there is a limit (pass -1 to ctor for no limit) then only the
 * first <limit + offset> tuples will be sorted.  The block may or may
 * not contain the tuples that follow when the sort method returns.
 */
class BlockSorter {
public:
    typedef LargeTempTableBlock::iterator iterator;
    typedef iterator::difference_type difference_type;

    BlockSorter(LargeTempTableBlockCache& lttBlockCache, ProgressMonitorProxy* pmp,
            const TupleSchema* schema, const AbstractExecutor::TupleComparer& lessThan,
                int limit, int offset) : m_lttBlockCache(lttBlockCache), m_pmp(pmp), m_schema(schema)
        , m_tempStorage(schema), m_tempTuple(m_tempStorage.tuple())
        , m_lessThan(lessThan), m_limit(limit == -1 ? -1 : (limit + offset)) { }

    void sort(LargeTempTableBlock* block) {
        int limit = m_limit;
        if (limit > block->activeTupleCount()) {
            limit = -1;
        }

        // if there are non-inlined columns, then an in-place
        // sort is usually faster, because we don't have to
        // move any non-inlined values.

        if (m_schema->getUninlinedObjectColumnCount() > 0) {
            // Do an in-place quicksort
            quicksort(block->begin(), block->end(), limit);
        } else {
            // There's no non-inlined data in this block, so
            // do a faster out-of-place sort.
            std::vector<TableTuple> ttVector;
            for (auto& tuple : *block) {
                if (m_pmp != NULL) {
                    m_pmp->countdownProgress();
                }

                ttVector.push_back(tuple.toTableTuple(m_schema));
            }

            // Sort the vector of TableTuples.
            if (limit == -1 || limit > ttVector.size()) {
                std::sort(ttVector.begin(), ttVector.end(), m_lessThan);
            } else {
                std::partial_sort(ttVector.begin(), ttVector.begin() + limit, ttVector.end(), m_lessThan);
            }

            LargeTempTableBlock *outputBlock = m_lttBlockCache.getEmptyBlock(m_schema);

            // Copy each tuple in the input block to the output block
            int tupleCount = 0;
            for (TableTuple& tuple : ttVector) {
                if (m_pmp != NULL) {
                    m_pmp->countdownProgress();
                }

                bool success = outputBlock->insertTuple(tuple);
                if (! success) {
                    throwSerializableEEException("Failed to insert into LTT block during out-of-place sort");
                }

                ++tupleCount;
                if (tupleCount == m_limit) {
                    break;
                }
            }

            // Swap the blocks so that the caller sees the input block as sorted.
            block->swap(outputBlock);

            outputBlock->unpin();
            m_lttBlockCache.releaseBlock(outputBlock->id());
        }
    }

private:
    LargeTempTableBlockCache& m_lttBlockCache;
    ProgressMonitorProxy* m_pmp;
    const TupleSchema* m_schema;
    StandAloneTupleStorage m_tempStorage;
    TableTuple m_tempTuple;
    const AbstractExecutor::TupleComparer& m_lessThan;
    const int m_limit;

    // It turns out the be difficult to use std::sort on objects whose
    // size is unknown at compile time, so here is an implementation
    // of quicksort that is similar to those used in the system libraries.
    void quicksort(LargeTempTableBlock::iterator beginIt, LargeTempTableBlock::iterator endIt, int limit) {
        while (true) {
            difference_type numElems = endIt - beginIt;
            switch (numElems) {
                case 0:
                case 1:
                    return;
                    // For small numbers of records, use insertion sort, using
                    // a template parameter so the number of records is known
                    // at compile time.
                case 2:
                    insertionSort<2>(beginIt);
                    return;
                case 3:
                    insertionSort<3>(beginIt);
                    return;
                case 4:
                    insertionSort<4>(beginIt);
                    return;
                default:
                    break;
            }

            // choose a pivot randomly to avoid worst-case behavior
            iterator pivot = beginIt + (rand() % numElems);
            swap(*pivot, endIt[-1]);
            pivot = endIt - 1;

            iterator iter = beginIt - 1; // index of last less-than-pivot element
            for (difference_type j = 0; j < numElems - 1; ++j) {
                if (m_pmp != NULL) {
                    m_pmp->countdownProgress();
                }


                iterator it = beginIt + j;
                if (m_lessThan(it->toTableTuple(m_schema), pivot->toTableTuple(m_schema))) {
                    ++iter;
                    swap(*it, *iter);
                }
            }

            // move the pivot to the correct place
            ++iter; // index of first greater-than-or-equal-to-pivot element
            if (m_lessThan(pivot->toTableTuple(m_schema), iter->toTableTuple(m_schema))) {
                swap(*pivot, *iter);
            }

            pivot = iter; // pivot is now in correct ordinal position

            difference_type numElemsLeft = pivot - beginIt;
            difference_type numElemsRight = endIt - (pivot + 1);

            if (limit != -1 && numElemsLeft + 1 >= limit) {
                // the part we care about is entirely within the left
                // partition---don't bother sorting the right
                // partition.
                endIt = pivot;
                // limit stays the same
            } else if (numElemsLeft > numElemsRight)  {
                // Make recursive call for smaller partition,
                // and use tail recursion elimination for larger one.
                int rightLimit = (limit == -1) ? -1 : (limit - (numElemsLeft + 1));
                quicksort(pivot + 1, endIt, rightLimit);
                endIt = pivot;
                // limit stays the same
            } else {
                quicksort(beginIt, pivot, limit);
                beginIt = pivot + 1;
                if (limit != -1) {
                    limit -= (numElemsLeft + 1);
                }
            }
        }
    }

    // A simple insertion sort, which is efficient if N is small.
    // N is a compile time parameter here to encourage loop unrolling.
    template<int N>
    void insertionSort(LargeTempTableBlock::iterator beginIt) {
        vassert(N > 1);
        for (difference_type i = 0; i < N; ++i) {
            int j = i;
            while (j > 0 && m_lessThan(beginIt[j].toTableTuple(m_schema),
                                      beginIt[j - 1].toTableTuple(m_schema))) {
                swap(beginIt[j - 1], beginIt[j]);
                --j;
            }
        }
    }

    void swap(LargeTempTableBlock::Tuple& t0, LargeTempTableBlock::Tuple& t1) {
        if (&t0 != &t1) {
            int tupleLength = m_tempTuple.tupleLength();
            char* tempBuffer = m_tempTuple.address();
            char* buf0 = reinterpret_cast<char*>(&t0);
            char* buf1 = reinterpret_cast<char*>(&t1);
            ::memcpy(tempBuffer, buf0, tupleLength);
            ::memcpy(buf0, buf1, tupleLength);
            ::memcpy(buf1, tempBuffer, tupleLength);
        }
    }
};

/**
 * This class bundles a LargeTempTable with a delete-as-you-go
 * TableIterator.  It's used to merge tables that have already been
 * sorted into a new, large table.
 */
class SortRun {
    LargeTempTable* m_table;
    TableIterator m_iterator;
    TableTuple m_curTuple;
public:
    SortRun(LargeTempTable* table) : m_table(table), m_iterator(m_table->iteratorDeletingAsWeGo()),
    m_curTuple(m_table->schema()) {
        m_table->incrementRefcount();
    }

    ~SortRun() {
        if (m_table) {
            // If the table was in the process of being scanned, we
            // want to unpin the block that was mid-scan before the
            // table is destroyed.  Calling the iterator's reset
            // method does this.
            m_iterator.reset();

            // When reference count goes to zero in the next line,
            // table is destroyed.
            m_table->decrementRefcount();
        }
    }

    void init() {
        // The iterator may be in the process of
        m_iterator.reset();
        m_iterator.next(m_curTuple); // pins first block in LTT block cache
    }

    bool insertTuple(TableTuple& tuple) {
        return m_table->insertTuple(tuple);
    }

    void finishInserts() {
        m_table->finishInserts();
    }

    TableTuple& currentTuple() {
        return m_curTuple;
    }

    const TableTuple& currentTuple() const {
        return m_curTuple;
    }

    std::string debug() const {
        std::ostringstream oss;
        oss << "sort run with blocks: ";
        for(auto id : m_table->getBlockIds()) {
            oss << id << " ";
        }

        return oss.str();
    }

    bool advance() {
        return m_iterator.next(m_curTuple);
    }

    LargeTempTable* peekTable() {
        return m_table;
    }
};

using SortRunPtr = std::shared_ptr<SortRun>;

/**
 * Compares two sort runs, based on the value of their current tuple.
 */
struct SortRunComparer {
private:
    const AbstractExecutor::TupleComparer& m_tupleComparer;
public:
    SortRunComparer(const AbstractExecutor::TupleComparer& tupleComparer)
        : m_tupleComparer(tupleComparer) { }

    bool operator()(const SortRunPtr& run0, const SortRunPtr& run1) {
        const TableTuple& tuple0 = run0->currentTuple();
        const TableTuple& tuple1 = run1->currentTuple();

        // transpose arguments to get greater-than instead of
        // less-than, since std::priority_queue is really a max heap.
        return m_tupleComparer(tuple1, tuple0);
    }
};

}

void LargeTempTable::sort(ProgressMonitorProxy* pmp,
        const AbstractExecutor::TupleComparer& comparer, int limit, int offset) {

    if (activeTupleCount() == 0) {
        return;
    } else if (limit == 0 || offset >= activeTupleCount()) {
        deleteAllTuples();
        return;
    }

    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    // Let's merge as much as we can, reserving one slot in the block
    // cache for the output of the merge.
    const int MERGE_FACTOR = lttBlockCache.maxCacheSizeInBlocks() - 1;

    // Sort each block and create a bunch of 1-block sort runs to be merged below
    std::queue<SortRunPtr> sortRunQueue;
    BlockSorter sorter{lttBlockCache, pmp, m_schema, comparer, limit, offset};
    auto it = getBlockIds().begin();
    while (it != getBlockIds().end()) {
        auto blockId = *it;
        it = disownBlock(it);
        LargeTempTableBlock* block = lttBlockCache.fetchBlock(blockId);
        sorter.sort(block);
        lttBlockCache.invalidateStoredCopy(block);
        block->unpin();
        LargeTempTable* table = TableFactory::buildCopiedLargeTempTable("largesort", this);
        table->inheritBlock(blockId);
        sortRunQueue.emplace(SortRunPtr{new SortRun(table)});
    }

    do {
        typedef std::priority_queue<SortRunPtr, std::vector<SortRunPtr>, SortRunComparer> SortRunPriorityQueue;
        SortRunPriorityQueue mergeHeap{SortRunComparer{comparer}};

        for (int i = 0; i < MERGE_FACTOR; ++i) {
            if (sortRunQueue.empty()) {
                break;
            }

            SortRunPtr run = sortRunQueue.front();
            sortRunQueue.pop();
            run->init();
            mergeHeap.push(run);
        }

        int limitThisPass;
        int offsetThisPass;
        if (sortRunQueue.size() != 0) {
            limitThisPass = (limit == -1 ? -1 : limit + offset);
            offsetThisPass = 0;
        } else {
            limitThisPass = limit;
            offsetThisPass = offset;
        }

        SortRunPtr outputSortRun(new SortRun(TableFactory::buildCopiedLargeTempTable("largesort", this)));
        int outputTupleCount = 0;
        while (mergeHeap.size() > 0) {
            if (pmp != NULL) {
                pmp->countdownProgress();
            }

            if (limitThisPass != -1 && outputTupleCount == limitThisPass) {
                break;
            }

            SortRunPtr run = mergeHeap.top();
            mergeHeap.pop();

            if (offsetThisPass > 0) {
                // Advance past the current tuple without putting it
                // into output sort run.
                if (run->advance()) {
                    mergeHeap.push(run);
                }

                --offsetThisPass;
            } else {
                outputSortRun->insertTuple(run->currentTuple());
                ++outputTupleCount;
                if (run->advance()) {
                    mergeHeap.push(run);
                }
            }
        }

        outputSortRun->finishInserts();
        sortRunQueue.push(outputSortRun);
    } while (sortRunQueue.size() > 1);

    swapContents(sortRunQueue.front()->peekTable());
}


} // namespace voltdb
