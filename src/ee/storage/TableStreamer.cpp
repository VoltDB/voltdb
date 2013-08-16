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

#include <map>
#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include "common/serializeio.h"
#include "storage/persistenttable.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/ElasticContext.h"
#include "storage/ElasticIndexReadContext.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"

namespace voltdb
{

typedef std::pair<CatalogId, Table*> TIDPair;

TableStreamer::Stream::Stream(TableStreamType streamType,
                              boost::shared_ptr<TableStreamerContext> context) :
    m_streamType(streamType),
    m_context(context)
{}

/**
 * Constructor.
 */
TableStreamer::TableStreamer(int32_t partitionId, PersistentTable &table, CatalogId tableId) :
    m_partitionId(partitionId),
    m_table(table),
    m_tableId(tableId)
{}

TableStreamer::~TableStreamer()
{}

/**
 * activateStream() knows how to create streams based on type.
 * Context classes determine whether or not reactivation is allowed.
 */
bool TableStreamer::activateStream(PersistentTableSurgeon &surgeon,
                                   TupleSerializer &serializer,
                                   TableStreamType streamType,
                                   std::vector<std::string> &predicateStrings)
{
    bool failed = false;

    // Rebuild the stream list with just the streams that deserve to persist.
    StreamList savedStreams(m_streams);
    m_streams.clear();
    bool found = false;
    BOOST_FOREACH(StreamPtr &streamPtr, savedStreams) {
        assert(streamPtr != NULL);
        found = streamPtr->m_streamType == streamType;
        if (found) {
            if (streamPtr->m_context->handleActivation(streamType, true)) {
                m_streams.push_back(streamPtr);
            }
            else {
                failed = true;
            }
            break;
        }
        else {
            m_streams.push_back(streamPtr);
        }
    }

    // Create an appropriate streaming context based on the stream type.
    if (!found && !failed) {
        try {
            boost::shared_ptr<TableStreamerContext> context;
            switch (streamType) {
                case TABLE_STREAM_SNAPSHOT:
                    // Constructor can throw exception when it parses the predicates.
                    context.reset(
                        new CopyOnWriteContext(m_table, surgeon, serializer, m_partitionId,
                                               predicateStrings, m_table.activeTupleCount()));
                    break;

                case TABLE_STREAM_RECOVERY:
                    context.reset(new RecoveryContext(m_table, surgeon, m_partitionId,
                                                      serializer, m_tableId));
                    break;

                case TABLE_STREAM_ELASTIC_INDEX:
                    context.reset(new ElasticContext(m_table, surgeon, m_partitionId,
                                                     serializer, predicateStrings));
                    break;

                case TABLE_STREAM_ELASTIC_INDEX_READ:
                    context.reset(new ElasticIndexReadContext(m_table, surgeon, m_partitionId,
                                                              serializer, predicateStrings));
                    break;

                default:
                    assert(false);
            }
            if (context->handleActivation(streamType, false)) {
                // Activation was accepted by the new context. Attach it to a stream.
                m_streams.push_back(StreamPtr(new Stream(streamType, context)));
            }
            else {
                // Activation was rejected by the new context.
                // Let the context disappear when it goes out of scope.
                failed = true;
            }
        }
        catch(SerializableEEException &e) {
            // The stream will not be added.
            failed = true;
        }
    }

    return !failed;
}

int64_t TableStreamer::streamMore(TupleOutputStreamProcessor &outputStreams,
                                  TableStreamType streamType,
                                  std::vector<int> &retPositions)
{
    int64_t remaining = -1;

    // Rebuild the stream list as dictated by context semantics.
    StreamList savedStreams(m_streams);
    m_streams.clear();
    for (StreamList::iterator iter = savedStreams.begin(); iter != savedStreams.end(); ++iter) {
        StreamPtr streamPtr = *iter;
        assert(streamPtr->m_context != NULL);
        if (streamPtr->m_streamType == streamType) {
            // Assert that we didn't find the stream type twice.
            assert(remaining == -1);
            remaining = streamPtr->m_context->handleStreamMore(outputStreams, retPositions);
            if (remaining <= 0) {
                // Drop the stream if it doesn't need to hang around (e.g. elastic).
                if (streamPtr->m_context->handleDeactivation()) {
                    m_streams.push_back(streamPtr);
                }
            }
            else {
                // Keep the stream because tuples remain.
                m_streams.push_back(streamPtr);
            }
        }
        else {
            // Keep other existing streams.
            m_streams.push_back(streamPtr);
        }
    }

    return remaining;
}

/**
 * Return true if a tuple can be freed safely.
 */
bool TableStreamer::canSafelyFreeTuple(TableTuple &tuple) const
{
    bool freeable = true;

    // Any active stream can reject freeing the tuple.
    BOOST_FOREACH(StreamPtr streamPtr, m_streams) {
        freeable = streamPtr->m_context->canSafelyFreeTuple(tuple);
        if (!freeable) {
            break;
        }
    }

    return freeable;
}

} // namespace voltdb
