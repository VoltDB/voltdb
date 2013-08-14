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

#ifndef TABLE_STREAMER_CONTEXT_H
#define TABLE_STREAMER_CONTEXT_H

#include <vector>
#include <string>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "common/StreamPredicateList.h"
#include "common/FatalException.hpp"
#include "storage/TupleBlock.h"

namespace voltdb
{

class TupleOutputStreamProcessor;
class TableTuple;
class TupleSerializer;
class PersistentTable;
class PersistentTableSurgeon;

/**
 * Abstract class that provides the interface for all table streamer contexts.
 */
class TableStreamerContext {

public:

    virtual ~TableStreamerContext() {}

    /**
     * Mandatory streamMore() handler.
     */
    virtual int64_t handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                     std::vector<int> &retPositions) = 0;

    /**
     * Optional activation handler.
     *  Called after creating the context to see if activation is allowed.
     *  Return true if activation is allowed. (default=true)
     */
    virtual bool handleActivation(TableStreamType streamType) {
        return true;
    }

    /**
     * Optional reactivation handler.
     *  Called during activation when the stream already exists.
     *  Return true if reactivation is allowed. (default=false)
     */
    virtual bool handleReactivation(TableStreamType streamType) {
        VOLT_ERROR("Not allowed to reactivate stream type %d", static_cast<int>(streamType));
        return false;
    }

    /**
     * Optional deactivation handler.
     *  Called when the stream is shutting down.
     *  Return true to keep it around and listening to updates. (default=false)
     */
    virtual bool handleDeactivation() {return false;}

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
     */
    virtual bool notifyTupleDelete(TableTuple &tuple) {return false;}

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
     * Optional tuple freeing check handler.
     */
    virtual bool canSafelyFreeTuple(TableTuple tuple) {return true;}

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
    int getMaxTupleLength() const
    {
        return m_maxTupleLength;
    }

    /**
     * Tuple serializer accessor.
     */
    TupleSerializer &getSerializer()
    {
        return m_serializer;
    }

    /**
     * Partition ID accessor.
     */
    int32_t getPartitionId() const
    {
        return m_partitionId;
    }

protected:

    /**
     * Constructor with predicates.
     */
    TableStreamerContext(PersistentTable &table,
                         PersistentTableSurgeon &surgeon,
                         int32_t partitionId,
                         TupleSerializer &serializer,
                         const std::vector<std::string> &predicateStrings);

    /**
     * Constructor without predicates.
     */
    TableStreamerContext(PersistentTable &table,
                         PersistentTableSurgeon &surgeon,
                         int32_t partitionId,
                         TupleSerializer &serializer);

    /**
     * Predicate delete flags accessor.
     */
    std::vector<bool> &getPredicateDeleteFlags()
    {
        return m_predicateDeleteFlags;
    }

    PersistentTableSurgeon &m_surgeon;

private:

    /**
     * Table being streamed.
     */
    PersistentTable &m_table;

    /**
     * Parsed hash range predicates.
     */
    StreamPredicateList m_predicates;

    /**
     * Per-predicate delete if true flags.
     */
    std::vector<bool> m_predicateDeleteFlags;

    /**
     * Maximum serialized length of a tuple
     */
    const int m_maxTupleLength;

    /**
     * Serializer for tuples
     */
    TupleSerializer &m_serializer;

    /**
     * Partition ID
     */
    const int32_t m_partitionId;
};

typedef boost::shared_ptr<TableStreamerContext> TableStreamerContextPtr;

} // namespace voltdb

#endif // TABLE_STREAMER_CONTEXT_H
