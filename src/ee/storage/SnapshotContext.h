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
#ifndef SNAPSHOTCONTEXT_H_
#define SNAPSHOTCONTEXT_H_

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
class ScanCopyOnWriteContext;

class SnapshotContext : public TableStreamerContext {

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

    virtual ~SnapshotContext();

    /**
     * Activation handler.
     */
    virtual ActivationReturnCode handleActivation(TableStreamType streamType);

    virtual ActivationReturnCode handleReactivation(TableStreamType streamType) {
        return ACTIVATION_UNSUPPORTED;
    }

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
    SnapshotContext(PersistentTable &table,
                       PersistentTableSurgeon &surgeon,
                       TupleSerializer &serializer,
                       int32_t partitionId,
                       const std::vector<std::string> &predicateStrings,
                       int64_t totalTuples);

    /**
     * Copy on write context
     */
    ScanCopyOnWriteContext * m_copyOnWriteContext;

    TableTuple m_tuple;

    int64_t m_totalTuples;
    int64_t m_serializationBatches;

    TableStreamType m_streamType;

    void checkRemainingTuples(const std::string &label);

};

}

#endif /* SNAPSHOTCONTEXT_H_ */
