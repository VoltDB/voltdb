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
    m_tableId(tableId),
    m_activeStreamIndex(-1)
{}

TableStreamer::~TableStreamer()
{}

/**
 * Purge all inactive streams except for elastic.
 * The elastic index needs to be available after scans complete.
 */
void TableStreamer::purgeStreams()
{
    // Clear and rebuild the stream list (m_streams) with only the streams that
    // can be accessed for data later (only elastic for now).
    StreamList savedStreams(m_streams);
    m_streams.clear();
    BOOST_FOREACH(StreamPtr &streamPtr, savedStreams) {
        assert(streamPtr != NULL);
        if (streamPtr->m_streamType == TABLE_STREAM_ELASTIC_INDEX) {
            m_streams.push_back(streamPtr);
            break;
        }
    }
    // Nothing is active after purging.
    m_activeStreamIndex = -1;
}

/**
 * activateStream() encapsulates all the stream type-specific knowledge.
 * It knows how to create streams, with or without predicates, and validate
 * that multiple stream types can coexist.
 * The only permitted context combinations are an elastic context paired with
 * any other context type. Of course any individual context is fine too.
 */
bool TableStreamer::activateStream(PersistentTableSurgeon &surgeon,
                                   TupleSerializer &serializer,
                                   TableStreamType streamType,
                                   std::vector<std::string> &predicateStrings)
{
    // It's an error if any stream is active. Let Java figure out how to recover,
    // e.g. by finishing the serializeMore() call sequence.
    TableStreamType activeStreamType = getActiveStreamType();
    if (activeStreamType != TABLE_STREAM_NONE) {
        VOLT_ERROR("An existing stream is still active (type %d).",
                   static_cast<int>(activeStreamType));
        return false;
    }

    // It's an error (handled below) if the stream type is already present.
    // Everything should have been purged except for any elastic stream if
    // streamMore() had been called repeatedly until it returned 0.
    if (hasStreamType(streamType)) {
        if (streamType == TABLE_STREAM_ELASTIC_INDEX) {
            // If starting a new elastic index stream get rid of the old one.
            m_streams.clear();
        }
        else {
            VOLT_ERROR("TableStreamer already has stream type %d.", static_cast<int>(streamType));
            return false;
        }
    }

    // Purge unneeded streams, e.g. to handle streamMore() not being completely drained.
    purgeStreams();

    // Create an appropriate streaming context based on the stream type.
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

            default:
                assert(false);
        }
        m_activeStreamIndex = static_cast<int>(m_streams.size());
        m_streams.push_back(StreamPtr(new Stream(streamType, context)));
    }
    catch(SerializableEEException &e) {
        // The stream will not be added.
    }

    return (m_activeStreamIndex != -1);
}

int64_t TableStreamer::streamMore(TupleOutputStreamProcessor &outputStreams,
                                  std::vector<int> &retPositions)
{
    int64_t remaining = -2;
    if (m_activeStreamIndex == -1) {
        // No active stream.
        remaining = -1;
    }
    else {
        // Let the active stream handle it.
        assert(m_activeStreamIndex >= 0 && m_activeStreamIndex < m_streams.size());
        if (m_activeStreamIndex >= 0 && m_activeStreamIndex < m_streams.size()) {
            StreamPtr streamPtr = m_streams.at(m_activeStreamIndex);
            assert(streamPtr != NULL);
            remaining = streamPtr->m_context->handleStreamMore(outputStreams, retPositions);
        }
    }
    if (remaining <= 0) {
        // No longer streaming - purge all but elastic streams (to keep the index around).
        purgeStreams();
    }

    return remaining;
}

/**
 * Return true if a tuple can be freed safely.
 */
bool TableStreamer::canSafelyFreeTuple(TableTuple &tuple) const
{
    bool freeable = true;
    if (m_activeStreamIndex != -1) {
        assert(m_activeStreamIndex >= 0 && m_activeStreamIndex < m_streams.size());
        const StreamPtr streamPtr = m_streams.at(m_activeStreamIndex);
        assert(streamPtr != NULL);
        freeable = streamPtr->m_context->canSafelyFreeTuple(tuple);
    }
    return freeable;
}

} // namespace voltdb
