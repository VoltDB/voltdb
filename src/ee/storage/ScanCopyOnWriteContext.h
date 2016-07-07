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
#ifndef SCANCOPYONWRITECONTEXT_H_
#define SCANCOPYONWRITECONTEXT_H_

#include <string>
#include <vector>
#include <utility>
#include "storage/TupleBlock.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"

#include <boost/scoped_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace voltdb {
class TupleIterator;
class TempTable;
class PersistentTableSurgeon;
class PersistentTable;

class ScanCopyOnWriteContext {
public:

    /**
     * Construct a copy on write context for the specified table that will
     * serialize tuples using the provided serializer.
     */
    ScanCopyOnWriteContext(PersistentTable &table,
                       PersistentTableSurgeon &surgeon,
                       int64_t totalTuples);

    virtual ~ScanCopyOnWriteContext();

    /**
     * Mark a tuple as dirty and make a copy if necessary. The new tuple param indicates
     * that this is a new tuple being introduced into the table (nextFreeTuple was called).
     * In that situation the tuple doesn't need to be copied, but and may need to be marked dirty
     * (if it will be scanned later by COWIterator), and it must be marked clean if it is not going to
     * be scanned by the COWIterator
     */
    void markTupleDirty(TableTuple tuple, bool newTuple);

    /**
     * Activation handler.
     */
    void handleActivation();

    /**
     * Do surgery
     */
    bool advanceIterator(TableTuple &tuple);

    bool cleanupTuple(TableTuple &tuple, bool deleteTuple);

    bool cleanup();

    void completePassIfDone(bool hasMore);

    /**
     * Optional block compaction handler.
     */
    void notifyBlockWasCompactedAway(TBPtr block);

    /**
     * Optional tuple insert handler.
     */
    bool notifyTupleInsert(TableTuple &tuple);

    /**
     * Optional tuple update handler.
     */
    bool notifyTupleUpdate(TableTuple &tuple);

    /**
     * Optional tuple delete handler.
     */
    bool notifyTupleDelete(TableTuple &tuple);

    bool isTableScanFinished() {
        return m_finishedTableScan;
    }

    int64_t getTuplesRemaining() {
        return m_tuplesRemaining;
    }

private:

    /**
     * Temp table for copies of tuples that were dirtied.
     */
    boost::scoped_ptr<TempTable> m_backedUpTuples;

    /**
     * Table we are maintaining a COW context for, and its surgeon
     */
    PersistentTable &m_table;
    PersistentTableSurgeon &m_surgeon;

    /**
     * Memory pool for string allocations
     */
    Pool m_pool;

    /**
     * Iterator over the table via a CopyOnWriteIterator or an iterator over
     *  temp table used to stored backed up tuples
     */
    boost::scoped_ptr<TupleIterator> m_iterator;

    TableTuple m_tuple;

    bool m_finishedTableScan;

    int64_t m_totalTuples;
    int64_t m_tuplesRemaining;
    int64_t m_blocksCompacted;
    int64_t m_serializationBatches;
    int64_t m_inserts;
    int64_t m_deletes;
    int64_t m_updates;

    void checkRemainingTuples(const std::string &label);

};

}

#endif /* SCANCOPYONWRITECONTEXT_H_ */
