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

#ifndef VOLTDB_LARGETEMPTABLE_H
#define VOLTDB_LARGETEMPTABLE_H

#include "common/LargeTempTableBlockCache.h"
#include "executors/abstractexecutor.h"
#include "storage/AbstractTempTable.hpp"
#include "storage/tableiterator.h"

namespace voltdb {

class LargeTempTableBlock;

/**
 * A large temp table class that uses LargeTempTableCache to request
 * tuple blocks, allowing some blocks to be stored on disk.
 *
 * A few assumptions about large temp tables (some of which may not
 * continue to hold moving forward):
 *
 * - Tables are inserted into (and not scanned) until inserts are
 *   complete, and thereafter read-only
 *
 * - Tables can be scanned by only one iterator at a time (as a result
 *   "pinned" is a boolean attribute, not a reference count)
 *
 * - Client code is responsible for unpinning blocks when they are no
 *   longer needed.  When inserting tuples, call finishInserts() to
 *   unpin.  Iterators will automatically unpin blocks after they are
 *   completely scanned, and when their destructors are fired,
 *   RAII-style.
 *
 * This makes it easier to track which blocks are currently in use,
 * and which may be stored to disk.
 */
class LargeTempTable : public AbstractTempTable {

    friend class TableFactory;

public:

    /** return the iterator for this table */
    TableIterator iterator();

    /** return an iterator that will automatically delete blocks after
        they are scanned. */
    TableIterator iteratorDeletingAsWeGo() {
        m_iter.reset(m_blockIds.begin());
        m_iter.setTempTableDeleteAsGo(true);
        return m_iter;
    }

    /** Delete all the tuples in this table */
    void deleteAllTuples(bool freeAllocatedStrings, bool fallible) {
        return deleteAllTempTuples();
    }

    /** Delete all the tuples in this table */
    virtual void deleteAllTempTuples();

    /** insert a tuple into this table */
    bool insertTuple(TableTuple& tuple);

    /** insert a tuple into this table */
    virtual void insertTempTuple(TableTuple &source) {
        insertTuple(source);
    }

    /** To unpin the last written block when all inserts are
        complete. */
    virtual void finishInserts();

    /**
     * Sort this table using the given compare function.  Also apply
     * the given limit and offset.
     */
    void sort(const AbstractExecutor::TupleComparer& comparer, int limit, int offset);

    /** Releases the specified block.  Called by delete-as-you-go
        iterators.  Returns an iterator pointing to the next block
        id. */
    virtual std::vector<int64_t>::iterator releaseBlock(std::vector<int64_t>::iterator it);

    /** Return the number of large temp table blocks used by this
        table */
    size_t allocatedBlockCount() const {
        return m_blockIds.size();
    }

    /** The type of this table, useful for debugging */
    std::string tableType() const {
        return "LargeTempTable";
    }

    /** This method seems to be used by some plan nodes, but the
        particulars are unclear. */
    std::vector<uint64_t> getBlockAddresses() const {
        throwSerializableEEException("Invalid call to getBlockAddresses() on LargeTempTable");
    }

    /** Return a table stats object for this table (unimplemented) */
    voltdb::TableStats* getTableStats() {
        throwSerializableEEException("Invalid call to getTableStats() on LargeTempTable");
    }

    /** return a tuple object pointing to the address where the next
        tuple should be inserted. */
    void nextFreeTuple(TableTuple* tuple);

    /** Return the temp table limits object for this table. (Currently none) */
    virtual const TempTableLimits* getTempTableLimits() const {
        return NULL;
    }

    /** Prints useful info about this table */
    virtual std::string debug(const std::string& spacer) const;

    /** Deletes all the tuples in this temp table (and their blocks) */
    virtual ~LargeTempTable();

    /**
     * Swap the contents of this table with another.
     */
    virtual void swapContents(AbstractTempTable* otherTable);

    std::vector<int64_t>& getBlockIds() {
        return m_blockIds;
    }

    const std::vector<int64_t>& getBlockIds() const {
        return m_blockIds;
    }

    std::vector<int64_t>::iterator disownBlock(std::vector<int64_t>::iterator pos) {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        m_tupleCount -= lttBlockCache->getBlockTupleCount(*pos);
        return m_blockIds.erase(pos);
    }

    void inheritBlock(int64_t blockId) {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        m_tupleCount += lttBlockCache->getBlockTupleCount(blockId);
        m_blockIds.push_back(blockId);

    }

protected:

    LargeTempTable();

private:

    void getEmptyBlock();

    std::vector<int64_t> m_blockIds;

    TableIterator m_iter;

    LargeTempTableBlock* m_blockForWriting;
};

}

#endif // VOLTDB_LARGETEMPTABLE_H
