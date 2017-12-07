/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "common/LargeTempTableBlockCache.h"
#include "storage/LargeTempTable.h"
#include "storage/LargeTempTableBlock.h"
#include "storage/tablefactory.h"

namespace voltdb {

LargeTempTable::LargeTempTable()
    : AbstractTempTable(LargeTempTableBlock::BLOCK_SIZE_IN_BYTES)
    , m_blockIds()
    , m_iter(this, m_blockIds.begin())
    , m_blockForWriting(NULL)
{
}

void LargeTempTable::getEmptyBlock() {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    // Mark the current block we're writing to as unpinned so it can
    // be stored if needed to make space for the next block.
    if (m_blockForWriting != NULL) {
        m_blockForWriting->unpin();
    }

    // Try to get an empty block (this will invoke I/O via topend, and
    // could throw for any number of reasons)
    LargeTempTableBlock* newBlock = lttBlockCache->getEmptyBlock(m_schema);

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
        assert (m_blockIds.size() > 0 && m_blockIds.back() == m_blockForWriting->id());
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

    m_iter.reset(m_blockIds.begin());
    return m_iter;
}


void LargeTempTable::deleteAllTempTuples() {
    finishInserts();

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    BOOST_FOREACH(int64_t blockId, m_blockIds) {
        lttBlockCache->releaseBlock(blockId);
    }

    m_blockIds.clear();
    m_tupleCount = 0;
}

std::vector<int64_t>::iterator LargeTempTable::releaseBlock(std::vector<int64_t>::iterator it) {
    if (it == m_blockIds.end()) {
        // block may have already been deleted
        return it;
    }

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    m_tupleCount -= lttBlockCache->getBlockTupleCount(*it);
    lttBlockCache->releaseBlock(*it);

    return m_blockIds.erase(it);
}

void LargeTempTable::swapContents(AbstractTempTable* otherTable) {
    assert (dynamic_cast<LargeTempTable*>(otherTable));
    LargeTempTable* otherLargeTable = static_cast<LargeTempTable*>(otherTable);

    m_blockIds.swap(otherLargeTable->m_blockIds);
    std::swap(m_tupleCount, otherLargeTable->m_tupleCount);
    if (m_blockForWriting || otherLargeTable->m_blockForWriting) {
        throwSerializableEEException("Please only swap large temp tables after finishInserts has been called");
    }
}

LargeTempTable::~LargeTempTable() {
    deleteAllTempTuples();
}

void LargeTempTable::nextFreeTuple(TableTuple*) {
    throwSerializableEEException("nextFreeTuple not implemented");
}

std::string LargeTempTable::debug(const std::string& spacer) const {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    std::ostringstream oss;
    oss << Table::debug(spacer);
    std::string infoSpacer = spacer + "  |";
    oss << infoSpacer << "\tLTT BLOCK IDS (" << m_blockIds.size() << " blocks):\n";
    if (m_blockIds.size() > 0) {
        BOOST_FOREACH(auto id, m_blockIds) {
            oss << infoSpacer;
            LargeTempTableBlock *block = lttBlockCache->getBlockForDebug(id);
            if (block != NULL) {
                oss << "   " << block->debug();
            }
            else {
                oss << "   block " << id << " is not in LTT block cache?!";
            }

            oss << "\n";
        }
    }
    else {
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
 * If there are no non-inlined columns, then it can be faster to sort
 * out-of-place, by sorting instances of TableTuples (16-byte objects
 * that are a pointer to tuple storage and a pointer to tuple
 * schema), and then copying the tuples to a new block in the sorted
 * order.
 *
 * If there non-inlined columns then there is an advantage to sorting
 * in place because all the non-inlined values can be left where they
 * are.  In this case we do an in-place quicksort, and swap the
 * position of tuples by copying tuple storage.
 */
class BlockSorter {
public:
    typedef LargeTempTableBlock::iterator iterator;
    typedef iterator::difference_type difference_type;

    BlockSorter(LargeTempTableBlockCache* lttBlockCache,
                const TupleSchema* schema,
                const AbstractExecutor::TupleComparer& compare)
        : m_lttBlockCache(lttBlockCache)
        , m_schema(schema)
        , m_tempStorage(schema)
        , m_tempTuple(m_tempStorage.tuple())
        , m_compare(compare)
    {
    }

    void sort(LargeTempTableBlock* block) {
        // if there are non-inlined columns, then an in-place
        // sort is usually faster, because we don't have to
        // move any non-inlined values.
        if (m_schema->getUninlinedObjectColumnCount() > 0) {
            // Do an in-place quicksort
            quicksort(block->begin(), block->end());
        }
        else {
            // There's no non-inlined data in this block, so
            // do a faster out-of-place sort.
            std::vector<TableTuple> ttVector;
            BOOST_FOREACH (auto& tuple, *block) {
                ttVector.push_back(tuple.toTableTuple(m_schema));
            }

            // Sort the vector of TableTuples.
            std::sort(ttVector.begin(), ttVector.end(), m_compare);

            LargeTempTableBlock *outputBlock = m_lttBlockCache->getEmptyBlock(m_schema);

            // Copy all the non-inlined data at once
            outputBlock->copyNonInlinedData(*block);

            // Copy each tuple in the input block to the output block
            BOOST_FOREACH (TableTuple& tuple, ttVector) {
                bool success = outputBlock->insertTupleRelocateNonInlinedFields(tuple, block->address());
                if (! success) {
                    throwSerializableEEException("Failed to insert into LTT block during out-of-place sort");
                }
            }

            // Swap the blocks so that the caller sees the input block as sorted.
            block->swap(outputBlock);

            outputBlock->unpin();
            m_lttBlockCache->releaseBlock(outputBlock->id());
        }
    }

private:

    // It turns out the be difficult to use std::sort on objects whose
    // size is unknown at compile time, so here is an implementation
    // of quicksort that is similar to those used in the system libraries.
    void quicksort(LargeTempTableBlock::iterator beginIt,
              LargeTempTableBlock::iterator endIt) {
        while (true) {
            difference_type numElems = endIt - beginIt;
            switch (numElems) {
            case 0:
            case 1:
                return;
            // For small numbers of records, use insertion sort, using
            // a template parameter so the number of records is known
            // at compile time.
            case 2: insertionSort<2>(beginIt); return;
            case 3: insertionSort<3>(beginIt); return;
            case 4: insertionSort<4>(beginIt); return;
            default:
                break;
            }

            // choose a pivot randomly to avoid worst-case behavior
            iterator pivot = beginIt + (rand() % numElems);
            swap(*pivot, endIt[-1]);
            pivot = endIt - 1;

            difference_type i = -1; // index of last less-than-pivot element
            for (difference_type j = 0; j < numElems - 1; ++j) {
                iterator it = beginIt + j;
                if (m_compare(it->toTableTuple(m_schema), pivot->toTableTuple(m_schema))) {
                    ++i;
                    swap(*it, beginIt[i]);
                }
            }

            // move the pivot to the correct place
            ++i; // index of first greater-than-or-equal-to-pivot element
            if (m_compare(pivot->toTableTuple(m_schema), beginIt[i].toTableTuple(m_schema))) {
                swap(*pivot, beginIt[i]);
            }

            pivot = beginIt + i; // pivot is now in correct ordinal position

            // Make recursive call for smaller partition,
            // and use tail recursion elimination for larger one.
            if (pivot - beginIt > endIt - (pivot + 1))  {
                quicksort(pivot + 1, endIt);
                endIt = pivot;
            }
            else {
                quicksort(beginIt, pivot);
                beginIt = pivot + 1;
            }
        }
    }

    // A simple insertion sort, which is efficient if N is small.
    // N is a compile time parameter here to encourage loop unrolling.
    template<int N>
    void insertionSort(LargeTempTableBlock::iterator beginIt) {
        assert (N > 1);

        for (difference_type i = 0; i < N; ++i) {
            int j = i;
            while (j > 0 && m_compare(beginIt[j].toTableTuple(m_schema),
                                      beginIt[j - 1].toTableTuple(m_schema))) {
                swap(beginIt[j - 1], beginIt[j]);
                --j;
            }
        }
    }

    void swap(LargeTempTableBlock::Tuple& t0,
              LargeTempTableBlock::Tuple& t1) {
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

    LargeTempTableBlockCache* m_lttBlockCache;
    const TupleSchema* m_schema;
    StandAloneTupleStorage m_tempStorage;
    TableTuple m_tempTuple;
    const AbstractExecutor::TupleComparer& m_compare;
};

/**
 * This class bundles a LargeTempTable with a delete-as-you-go
 * TableIterator.  It's used to merge tables that have already been
 * sorted into a new, large table.
 */
class SortRun {
public:
    SortRun(LargeTempTable* table)
        : m_table(table)
        , m_iterator(m_table->iteratorDeletingAsWeGo())
        , m_curTuple(m_table->schema())
    {
        m_table->incrementRefcount();
    }

    ~SortRun() {
        if (m_table) {
            m_iterator = m_table->iteratorDeletingAsWeGo(); // unpins block if scan in progress
            m_table->decrementRefcount();
        }
    }

    void init() {
        m_iterator = m_table->iteratorDeletingAsWeGo();
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
        BOOST_FOREACH(int64_t id, m_table->getBlockIds()) {
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

private:
    LargeTempTable* m_table;
    TableIterator m_iterator;
    TableTuple m_curTuple;
};

typedef std::shared_ptr<SortRun> SortRunPtr;

/**
 * Compares two sort runs, based on the value of their current tuple.
 */
struct SortRunComparer {
    SortRunComparer(const AbstractExecutor::TupleComparer& tupleComparer)
        : m_tupleComparer(tupleComparer)
    {
    }

    bool operator()(const SortRunPtr& run0, const SortRunPtr& run1) {
        const TableTuple& tuple0 = run0->currentTuple();
        const TableTuple& tuple1 = run1->currentTuple();

        // transpose arguments to get greater-than instead of
        // less-than, since std::priority_queue is really a max heap.
        return m_tupleComparer(tuple1, tuple0);
    }

private:
    const AbstractExecutor::TupleComparer& m_tupleComparer;
};

}

void LargeTempTable::sort(const AbstractExecutor::TupleComparer& comparer, int limit, int offset) {

    if (limit != -1 || offset != 0) {
        throwSerializableEEException("Limit and offset not yet supported on large temp tables");
    }

    // TODO: caller should pass in a ProgressMonitorProxy (or define
    // one locally here) to ensure we can cancel the query if it's
    // taking too long.

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    // Sort each block and create a bunch of 1-block sort runs to be merged below
    std::queue<SortRunPtr> sortRunQueue;
    BlockSorter sorter{lttBlockCache, m_schema, comparer};
    auto it = getBlockIds().begin();
    while (it != getBlockIds().end()) {
        int64_t blockId = *it;
        it = disownBlock(it);
        LargeTempTableBlock* block = lttBlockCache->fetchBlock(blockId);
        sorter.sort(block);
        lttBlockCache->invalidateStoredCopy(block);
        block->unpin();
        LargeTempTable* table = TableFactory::buildCopiedLargeTempTable("largesort", this);
        table->inheritBlock(blockId);
        SortRunPtr sortRun{new SortRun(table)};
        sortRunQueue.push(sortRun);
    }

    // Let's merge as much as we can, reserving one slot in the block
    // cache for the output of the merge.
    const int MERGE_FACTOR = lttBlockCache->maxCacheSizeInBlocks() - 1;
    while (sortRunQueue.size() != 1) {
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

        SortRunPtr outputSortRun(new SortRun(TableFactory::buildCopiedLargeTempTable("largesort", this)));
        while (mergeHeap.size() > 0) {
            SortRunPtr run = mergeHeap.top();
            mergeHeap.pop();

            outputSortRun->insertTuple(run->currentTuple());
            if (run->advance()) {
                mergeHeap.push(run);
            }
        }

        outputSortRun->finishInserts();
        sortRunQueue.push(outputSortRun);
    }

    swapContents(sortRunQueue.front()->peekTable());
}


} // namespace voltdb
