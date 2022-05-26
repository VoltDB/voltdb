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

#ifndef TABLE_STREAMER_CONTEXT_H
#define TABLE_STREAMER_CONTEXT_H

#include <vector>
#include <string>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "common/StreamPredicateList.h"
#include "common/FatalException.hpp"
#include "storage/TupleBlock.h"

namespace voltdb {
class TupleOutputStreamProcessor;
class TableTuple;
class PersistentTable;
class PersistentTableSurgeon;

/**
 * Abstract class that provides the interface for all table streamer contexts.
 */
class TableStreamerContext {

public:

    /**
     * handleActivation()/handleReactivation() return codes.
     */
    enum ActivationReturnCode {
        // (Re)Activation is not supported for this stream type by this context.
        ACTIVATION_UNSUPPORTED = -1,
        // (Re)Activation is supported for this stream type and succeeded.
        ACTIVATION_SUCCEEDED = 0,
        // (Re)Activation is supported for this stream type, but the attempt failed.
        ACTIVATION_FAILED = 1,
    };

    virtual ~TableStreamerContext() {}

    /**
     * Optional activation handler.
     *  Called after creating the context to see if activation is allowed.
     *  Return ACTIVATION_SUCCEEDED if (re)activation is allowed and succeeded.
     *  Return ACTIVATION_FAILED if (re)activation is allowed but failed.
     *  Return ACTIVATION_UNSUPPORTED if (re)activation is not supported for the stream type.
     */
    virtual ActivationReturnCode handleActivation(TableStreamType streamType) {
        return ACTIVATION_SUCCEEDED;
    }

    /**
     * Optional reactivation handler.
     *  Called see if reactivation is allowed.
     *  Return ACTIVATION_SUCCEEDED if reactivation is allowed and succeeded.
     *  Return ACTIVATION_FAILED if reactivation is allowed but failed.
     *  Return ACTIVATION_UNSUPPORTED if reactivation is not supported for the stream type.
     */
    virtual ActivationReturnCode handleReactivation(TableStreamType streamType) {
        return ACTIVATION_UNSUPPORTED;
    }

    /**
     * Mandatory streamMore() handler.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions) = 0;

    /**
     * Optional deactivation handler.
     *  Called when the stream is shutting down.
     *  Return true to keep it around and listening to updates. (default=false)
     */
    virtual bool handleDeactivation(TableStreamType streamType) {return false;}

    /**
     * Optional tuple insert handler.
     */
    virtual bool notifyTupleInsert(TableTuple &tuple) {return false;}

    /**
     * Optional tuple update handler.
     */
    virtual bool notifyTupleUpdate(TableTuple &tuple) {return false;}

    /**
     * Optional tuple delete handler.
     * returns true meaning that the tuple can be freed
     */
    virtual bool notifyTupleDelete(TableTuple &tuple) {return true;}

    /**
     * Optional block compaction handler.
     */
    virtual void notifyBlockWasCompactedAway(TBPtr block) {}

    /**
     * Optional tuple compaction handler.
     */
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {}

    /**
     * Table accessor.
     */
    PersistentTable &getTable()
    {
        return m_table;
    }

    /**
     * Predicates accessor.
     */
    StreamPredicateList &getPredicates()
    {
        return m_predicates;
    }

    /**
     * Tuple length accessor.
     */
    size_t getMaxTupleLength() const
    {
        return m_maxTupleLength;
    }

    /**
     * Partition ID accessor.
     */
    int32_t getPartitionId() const
    {
        return m_partitionId;
    }

    /**
     * Parse and save predicates.
     */
    virtual void updatePredicates(const std::vector<std::string> &predicateStrings);

    virtual TableStreamerContext* cloneForTruncatedTable(PersistentTableSurgeon &surgeon)
    {
        // Derived classes that are not related to ongoing elastic rebalance
        // do not need to be applied to the post-truncated copy of the table.
        return NULL;
    }
    virtual int64_t getRemainingCount() { return TABLE_STREAM_SERIALIZATION_ERROR;}
protected:

    /**
     * Constructor with predicates.
     */
    TableStreamerContext(PersistentTable &table,
                         PersistentTableSurgeon &surgeon,
                         int32_t partitionId,
                         const std::vector<std::string> &predicateStrings);

    /**
     * Constructor without predicates.
     */
    TableStreamerContext(PersistentTable &table,
                         PersistentTableSurgeon &surgeon,
                         int32_t partitionId);

    /**
     * Predicate delete flags accessor.
     */
    std::vector<bool> &getPredicateDeleteFlags()
    {
        return m_predicateDeleteFlags;
    }

    PersistentTableSurgeon &m_surgeon;

    /**
     * Parsed hash range predicates.
     */
    StreamPredicateList m_predicates;

private:

    /**
     * Table being streamed.
     */
    PersistentTable &m_table;

    /**
     * Per-predicate delete if true flags.
     */
    std::vector<bool> m_predicateDeleteFlags;

    /**
     * Maximum serialized length of a tuple
     */
    const size_t m_maxTupleLength;

    /**
     * Partition ID
     */
    const int32_t m_partitionId;
};

typedef boost::shared_ptr<TableStreamerContext> TableStreamerContextPtr;

} // namespace voltdb

#endif // TABLE_STREAMER_CONTEXT_H
