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

#ifndef TABLE_STREAMER_H
#define TABLE_STREAMER_H

#include <string>
#include <vector>
#include <list>
#include <boost/scoped_ptr.hpp>
#include "common/ids.h"
#include "common/types.h"
#include "common/TupleSerializer.h"
#include "storage/TableStreamerInterface.h"
#include "storage/TupleBlock.h"
#include "storage/ElasticScanner.h"
#include "storage/TableStreamerContext.h"

namespace voltdb
{

class CopyOnWriteContext;
class RecoveryContext;
class ElasticContext;
class ReferenceSerializeInput;
class PersistentTable;
class TupleOutputStreamProcessor;

class TableStreamer : public TableStreamerInterface
{
public:

    /**
     * Constructor with data from serialized message.
     */
    TableStreamer(TupleSerializer &tupleSerializer,
                  TableStreamType streamType,
                  int32_t partitionId,
                  ReferenceSerializeInput &serializeIn);

    /**
     * Destructor.
     */
    virtual ~TableStreamer();

    /**
     * Return true if the stream has already been activated.
     */
    virtual bool isAlreadyActive() const {
        return m_context != NULL;
    }

    /**
     * Activate streaming.
     */
    virtual bool activateStream(PersistentTable &table, CatalogId tableId);

    /**
     * Continue streaming.
     */
    virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                               std::vector<int> &retPositions);

    /**
     * Tuple insert hook.
     * Return true if it was handled by the COW context.
     */
    virtual bool notifyTupleInsert(TableTuple &tuple) {
        bool handled = false;
        if (m_context != NULL) {
            handled = m_context->notifyTupleInsert(tuple);
        }
        return handled;
    }

    /**
     * Tuple update hook.
     * Return true if it was handled by the COW context.
     */
    virtual bool notifyTupleUpdate(TableTuple &tuple) {
        bool handled = false;
        if (m_context != NULL) {
            handled = m_context->notifyTupleUpdate(tuple);
        }
        return handled;
    }

    /**
     * Tuple delete hook.
     * Return true if it was handled by the COW context.
     */
    virtual bool notifyTupleDelete(TableTuple &tuple) {
        bool handled = false;
        if (m_context != NULL) {
            handled = m_context->notifyTupleDelete(tuple);
        }
        return handled;
    }

    /**
     * Block compaction hook.
     */
    virtual void notifyBlockWasCompactedAway(TBPtr block) {
        if (m_context != NULL) {
            m_context->notifyBlockWasCompactedAway(block);
        }
    }

    /**
     * Called for each tuple moved.
     */
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        if (m_context != NULL) {
            m_context->notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
        }
    }

    /**
     * Return the stream type, snapshot, recovery, etc..
     * TODO: Refactor so the caller doesn't need to know the stream type, just the context.
     */
    virtual TableStreamType getStreamType() const {
        return m_streamType;
    }

    /**
     * Return the current active stream type or TABLE_STREAM_NONE if nothing is active.
     * TODO: Refactor so the caller doesn't need to know the stream type, just the context.
     */
    virtual TableStreamType getActiveStreamType() const {
        return m_context.get() != NULL ? m_streamType : TABLE_STREAM_NONE;
    }

    /**
     * Return true if a tuple can be freed safely.
     */
    virtual bool canSafelyFreeTuple(TableTuple &tuple) const;

private:

    /// Tuple serializer.
    TupleSerializer &m_tupleSerializer;

    /// The type of scan.
    const TableStreamType m_streamType;

    /// Current partition ID.
    int32_t m_partitionId;

    /// Predicate strings.
    std::vector<std::string> m_predicateStrings;

    /// Context to keep track of snapshot scans.
    boost::scoped_ptr<TableStreamerContext> m_context;
};

} // namespace voltdb

#endif // TABLE_STREAMER_H
