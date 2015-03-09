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

#ifndef TABLE_STREAMER_H
#define TABLE_STREAMER_H

#include <string>
#include <vector>
#include <list>
#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>
#include "common/declarations.h"
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
                                const std::vector<std::string> &predicateStrings);

    /**
     * Continue streaming.
     */
    virtual int64_t streamMore(TupleOutputStreamProcessor &outputStreams,
                               TableStreamType streamType,
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
            handled = streamPtr->m_context->notifyTupleInsert(tuple) || handled;
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
            handled = streamPtr->m_context->notifyTupleUpdate(tuple) || handled;
        }
        return handled;
    }

    /**
     * Tuple delete hook.
     * Return true if it was handled by the COW context.
     */
    virtual bool notifyTupleDelete(TableTuple &tuple) {
        bool freeable = true;
        // Any active stream can reject freeing the tuple.
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            freeable = streamPtr->m_context->notifyTupleDelete(tuple) && freeable;
        }
        return freeable;
    }

    /**
     * Block compaction hook.
     */
    virtual void notifyBlockWasCompactedAway(TBPtr block) {
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            streamPtr->m_context->notifyBlockWasCompactedAway(block);
        }
    }

    /**
     * Called for each tuple moved.
     */
    virtual void notifyTupleMovement(TBPtr sourceBlock, TBPtr targetBlock,
                                     TableTuple &sourceTuple, TableTuple &targetTuple) {
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            streamPtr->m_context->notifyTupleMovement(sourceBlock, targetBlock, sourceTuple, targetTuple);
        }
    }

    /**
     * Return the partition ID.
     */
    virtual int32_t getPartitionID() const {
        return m_partitionId;
    }

    /**
     * Return context or null for specified type.
     */
    virtual TableStreamerContextPtr findStreamContext(TableStreamType streamType) {
        boost::shared_ptr<TableStreamerContext> context;
        BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
            assert(streamPtr != NULL);
            if (streamPtr->m_streamType == streamType) {
                context = streamPtr->m_context;
                break;
            }
        }
        return context;
    }

    virtual TableStreamerInterface* cloneForTruncatedTable(PersistentTableSurgeon &surgeon);

private:

    class Stream
    {
    public:

        Stream(TableStreamType streamType,
               boost::shared_ptr<TableStreamerContext> context);

        /// The type of scan.
        const TableStreamType m_streamType;

        /// The stream context.
        boost::shared_ptr<TableStreamerContext> m_context;
    };

    /// Current partition ID.
    int32_t m_partitionId;

    /// The table that we're streaming.
    PersistentTable &m_table;

    /// The ID of the table that we're streaming.
    CatalogId m_tableId;

    /**
     * Snapshot streams.
     * All streams are notified of inserts, updates, deletes, and compactions.
     */
    typedef boost::shared_ptr<Stream> StreamPtr;
    typedef std::vector<StreamPtr> StreamList;
    StreamList m_streams;
};

} // namespace voltdb

#endif // TABLE_STREAMER_H
