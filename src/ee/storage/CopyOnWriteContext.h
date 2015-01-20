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
#ifndef COPYONWRITECONTEXT_H_
#define COPYONWRITECONTEXT_H_

#include <string>
#include <vector>
#include <utility>
#include "common/TupleSerializer.h"
#include "common/TupleOutputStreamProcessor.h"
#include "storage/persistenttable.h"
#include "storage/TableStreamer.h"
#include "storage/TableStreamerContext.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include <boost/scoped_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace voltdb {
class TupleIterator;
class TempTable;
class ParsedPredicate;
class TupleOutputStreamProcessor;
class PersistentTableSurgeon;

class CopyOnWriteContext : public TableStreamerContext {

    friend bool TableStreamer::activateStream(PersistentTableSurgeon&, TupleSerializer&,
                                              TableStreamType, const std::vector<std::string>&);

public:

    /**
     * Mark a tuple as dirty and make a copy if necessary. The new tuple param indicates
     * that this is a new tuple being introduced into the table (nextFreeTuple was called).
     * In that situation the tuple doesn't need to be copied, but and may need to be marked dirty
     * (if it will be scanned later by COWIterator), and it must be marked clean if it is not going to
     * be scanned by the COWIterator
     */
    void markTupleDirty(TableTuple tuple, bool newTuple);

    virtual ~CopyOnWriteContext();

    /**
     * Activation handler.
     */
    virtual ActivationReturnCode handleActivation(TableStreamType streamType);

    /**
     * Mandatory TableStreamContext override.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions);

    /**
     * Optional block compaction handler.
     */
    virtual void notifyBlockWasCompactedAway(TBPtr block);

    /**
     * Optional tuple insert handler.
     */
    virtual bool notifyTupleInsert(TableTuple &tuple);

    /**
     * Optional tuple update handler.
     */
    virtual bool notifyTupleUpdate(TableTuple &tuple);

    /**
     * Optional tuple delete handler.
     */
    virtual bool notifyTupleDelete(TableTuple &tuple);

private:

    /**
     * Construct a copy on write context for the specified table that will
     * serialize tuples using the provided serializer.
     * Private so that only TableStreamer::activateStream() can call.
     */
    CopyOnWriteContext(PersistentTable &table,
                       PersistentTableSurgeon &surgeon,
                       TupleSerializer &serializer,
                       int32_t partitionId,
                       const std::vector<std::string> &predicateStrings,
                       int64_t totalTuples);

    /**
     * Temp table for copies of tuples that were dirtied.
     */
    boost::scoped_ptr<TempTable> m_backedUpTuples;

    /**
     * Memory pool for string allocations
     */
    Pool m_pool;

    /**
     * Copied and sorted tuple blocks that can be binary searched in order to find out. The pair
     * contains the block address as well as the original index of the block.
     */
    TBMap m_blocks;

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
    int64_t m_updates;

    void checkRemainingTuples(const std::string &label);

};

}

#endif /* COPYONWRITECONTEXT_H_ */
