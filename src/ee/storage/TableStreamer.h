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
#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>
#include "common/ids.h"
#include "common/types.h"
#include "common/TupleSerializer.h"
#include "storage/TableStreamerInterface.h"
#include "storage/TupleBlock.h"
#include "storage/ElasticScanner.h"
#include "storage/TableStreamerContext.h"

class CopyOnWriteTest;

namespace voltdb
{

class ReferenceSerializeInput;
class PersistentTable;
class PersistentTableSurgeon;
class TupleOutputStreamProcessor;

class TableStreamer : public TableStreamerInterface
{
    friend class ::CopyOnWriteTest;

public:

    /**
     * Constructor with data from serialized message.
     */
    TableStreamer(int32_t partitionId, PersistentTable &table, CatalogId tableId);

    /**
     * Destructor.
     */
    virtual ~TableStreamer();

    /**
     * Activate a stream.
     * Return true if the stream was activated (by the call or previously).
     */
    virtual bool activateStream(PersistentTableSurgeon &surgeon,
                                TupleSerializer &serializer,
                                TableStreamType streamType,
                                std::vector<std::string> &predicateStrings);

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
        // If any stream handles the notification, it's "handled".
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr != NULL) {
                handled |= streamPtr->m_context->notifyTupleInsert(tuple);
            }
        }
        return handled;
    }

    /**
     * Tuple update hook.
     * Return true if it was handled by the COW context.
     */
    virtual bool notifyTupleUpdate(TableTuple &tuple) {
        bool handled = false;
        // If any context handles the notification, it's "handled".
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr != NULL) {
                handled |= streamPtr->m_context->notifyTupleUpdate(tuple);
            }
        }
        return handled;
    }

    /**
     * Tuple delete hook.
     * Return true if it was handled by the COW context.
     */
    virtual bool notifyTupleDelete(TableTuple &tuple) {
        bool handled = false;
        // If any context handles the notification, it's "handled".
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr != NULL) {
                handled |= streamPtr->m_context->notifyTupleDelete(tuple);
            }
        }
        return handled;
    }

    /**
     * Block compaction hook.
     */
    virtual void notifyBlockWasCompactedAway(TBPtr block) {
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr != NULL) {
                streamPtr->m_context->notifyBlockWasCompactedAway(block);
            }
        }
    }

    /**
     * Called for each tuple moved.
     */
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr != NULL) {
                streamPtr->m_context->notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
            }
        }
    }

    /**
     * Return the partition ID.
     */
    virtual int32_t getPartitionID() const {
        return m_partitionId;
    }

    /**
     * Return true if a tuple can be freed safely.
     */
    virtual bool canSafelyFreeTuple(TableTuple &tuple) const;

    /**
     * Return stream type for active stream or TABLE_STREAM_NONE if nothing is active.
     */
    virtual TableStreamType getActiveStreamType() const {
        if (m_activeStreamIndex == -1) {
            return TABLE_STREAM_NONE;
        }
        assert(m_activeStreamIndex >= 0 && m_activeStreamIndex < m_streams.size());
        if (m_activeStreamIndex < 0 || m_activeStreamIndex >= m_streams.size()) {
            return TABLE_STREAM_NONE;
        }
        StreamPtr streamPtr = m_streams.at(m_activeStreamIndex);
        assert(streamPtr != NULL);
        if (streamPtr == NULL) {
            return TABLE_STREAM_NONE;
        }
        return streamPtr->m_streamType;
    }

    /**
     * Return true if managing a stream of the specified type.
     */
    virtual bool hasStreamType(TableStreamType streamType) const {
        BOOST_FOREACH(const StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr != NULL && streamPtr->m_streamType == streamType) {
                return true;
            }
        }
        return false;
    }

private:

    class Stream
    {
        friend class TableStreamer;
        friend class ::CopyOnWriteTest;

    public:

        Stream(TableStreamType streamType,
               boost::shared_ptr<TableStreamerContext> context);

        /// The type of scan.
        const TableStreamType m_streamType;

        /// The stream context.
        boost::shared_ptr<TableStreamerContext> m_context;
    };

    /// Purge all completed streams.
    void purgeStreams();

    /// Current partition ID.
    int32_t m_partitionId;

    /// The table that we're streaming.
    PersistentTable &m_table;

    /// The ID of the table that we're streaming.
    CatalogId m_tableId;

    /**
     * Snapshot streams. There can be more than one stream, but only one is
     * active and streamable. The other(s) can provide results after
     * scanning completes, like the elastic index. All streams are notified
     * of inserts, updates, deletes, and compactions.
     */
    typedef boost::shared_ptr<Stream> StreamPtr;
    typedef std::vector<StreamPtr> StreamList;
    StreamList m_streams;

    /// The current active stream index. Not valid when m_streams is empty.
    int m_activeStreamIndex;
};

} // namespace voltdb

#endif // TABLE_STREAMER_H
