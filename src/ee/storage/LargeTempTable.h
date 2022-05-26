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

#pragma once

#include "common/LargeTempTableBlockCache.h"
#include "common/LargeTempTableBlockId.hpp"
#include "executors/abstractexecutor.h"
#include "storage/AbstractTempTable.hpp"
#include "storage/tableiterator.h"

namespace voltdb {

class LargeTempTableBlock;
class ProgressMonitorProxy;

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

    std::vector<LargeTempTableBlockId> m_blockIds {};

    LargeTempTableBlock* m_blockForWriting = nullptr;

    friend class TableFactory;
    void getEmptyBlock();
public:

    /** return the iterator for this table */
    TableIterator iterator() override;

    /** return an iterator that will automatically delete blocks after
        they are scanned. */
    TableIterator iteratorDeletingAsWeGo() override;

    /** Delete all the tuples in this table */
    void deleteAllTuples() override;

    /** insert a tuple into this table */
    bool insertTuple(TableTuple& tuple) override;

    /** insert a tuple into this table */
    void insertTempTuple(TableTuple &source) override {
        insertTuple(source);
    }

    /** To unpin the last written block when all inserts are
        complete. */
    void finishInserts() override;

    /**
     * Sort this table using the given compare function.  Also apply
     * the given limit and offset.
     */
    void sort(ProgressMonitorProxy *pmp, const AbstractExecutor::TupleComparer& comparer, int limit, int offset);

    /** Releases the specified block.  Called by delete-as-you-go
        iterators.  Returns an iterator pointing to the next block
        id. */
    std::vector<LargeTempTableBlockId>::iterator releaseBlock(std::vector<LargeTempTableBlockId>::iterator it) override;

    /** Return the number of large temp table blocks used by this
        table */
    size_t allocatedBlockCount() const override {
        return m_blockIds.size();
    }

    /** The type of this table, useful for debugging */
    std::string tableType() const override {
        return "LargeTempTable";
    }

    /** This method seems to be used by some plan nodes, but the
        particulars are unclear. */
    std::vector<uint64_t> getBlockAddresses() const override {
        throwSerializableEEException("Invalid call to getBlockAddresses() on LargeTempTable");
    }

    /** Return a table stats object for this table (unimplemented) */
    voltdb::TableStats* getTableStats() override {
        throwSerializableEEException("Invalid call to getTableStats() on LargeTempTable");
    }

    /** return a tuple object pointing to the address where the next
        tuple should be inserted. */
    void nextFreeTuple(TableTuple* tuple) override;

    /** Return the temp table limits object for this table. (Currently none) */
    const TempTableLimits* getTempTableLimits() const override {
        return NULL;
    }

    /** Prints useful info about this table */
    std::string debug(const std::string& spacer) const override;

    /** Deletes all the tuples in this temp table (and their blocks) */
    virtual ~LargeTempTable();

    /**
     * Swap the contents of this table with another.
     */
    void swapContents(AbstractTempTable* otherTable) override;

    std::vector<LargeTempTableBlockId>& getBlockIds() {
        return m_blockIds;
    }

    const std::vector<LargeTempTableBlockId>& getBlockIds() const {
        return m_blockIds;
    }

    std::vector<LargeTempTableBlockId>::iterator disownBlock(std::vector<LargeTempTableBlockId>::iterator pos) {
        m_tupleCount -= ExecutorContext::getExecutorContext()->lttBlockCache().getBlockTupleCount(*pos);
        return m_blockIds.erase(pos);
    }

    void inheritBlock(LargeTempTableBlockId blockId) {
        m_tupleCount += ExecutorContext::getExecutorContext()->lttBlockCache().getBlockTupleCount(blockId);
        m_blockIds.push_back(blockId);

    }

protected:

    LargeTempTable();
};

}

