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

void TableStreamer::purgeStreams()
{
    // Purge everything but an elastic stream.
    Stream *elasticStream = NULL;
    size_t i = 0;
    for (boost::ptr_vector<Stream>::iterator iter = m_streams.begin();
         iter != m_streams.end(); ++iter) {
        if (iter->m_streamType == TABLE_STREAM_ELASTIC_INDEX) {
            // Rebuild the vector to only contain the elastic stream.
            elasticStream = m_streams.release(iter).release();
            break;
        }
        ++i;
    }

    m_streams.clear();
    if (elasticStream != NULL) {
        m_streams.push_back(elasticStream);
    }
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
    // Reactivate an already-present stream? Also look for an elastic stream.
    m_activeStreamIndex = -1;
    size_t i = 0;
    for (boost::ptr_vector<Stream>::const_iterator iter = m_streams.begin();
         iter != m_streams.end(); ++iter) {
        if (m_activeStreamIndex == -1 && iter->m_streamType == streamType) {
            m_activeStreamIndex = static_cast<int>(i);
        }
        ++i;
    }

    /*
     * Conditions for purging non-elastic streams (OR):
     *  - An elastic stream is active (because non-elastic streams don't serve
     *    any purpose after streaming completes).
     *  - No stream is active.
     * purgeStreams() never gets rid of an elastic stream in case the index is
     * needed. It also adjusts m_activeStreamIndex if an active elastic stream
     * moves.
     */
    if (   (   m_activeStreamIndex != -1
            && m_streams.at(m_activeStreamIndex).m_streamType == TABLE_STREAM_ELASTIC_INDEX)
        || m_activeStreamIndex == -1 ) {
        purgeStreams();
    }

    // Activate a new stream?
    if (m_activeStreamIndex == -1) {
        /*
         * For now the semantics are that there can be two streams. One is the
         * active stream used by streamMore(). The second, if present, provides
         * access to completed elastic scan results, i.e. the elastic index.
         * m_activeStreamIndex indexes the active stream for streamMore(),
         * allowing the active stream to be in either position.
         */
        if (streamType == TABLE_STREAM_ELASTIC_INDEX) {
            // There should be at most one stream if we didn't find an existing elastic stream.
            assert(m_streams.size() <= 1);
            if (m_streams.size() > 1) {
                // cya
                m_streams.clear();
            }
        }
        // At this point m_streams is either empty or with a single elastic stream.
        assert(   m_streams.empty()
               || (m_streams.size() == 1 && m_streams.at(0).m_streamType == TABLE_STREAM_ELASTIC_INDEX));

        // Create an appropriate streaming context based on the stream type.
        try {
            boost::shared_ptr<TableStreamerContext> context;
            switch (streamType) {
                case TABLE_STREAM_SNAPSHOT: {
                    // Constructor can throw exception when it parses the predicates.
                    context.reset(
                        new CopyOnWriteContext(m_table, surgeon, serializer, m_partitionId,
                                               predicateStrings, m_table.activeTupleCount()));
                    break;
                }

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
            m_streams.push_back(new Stream(streamType, context));
        }
        catch(SerializableEEException &e) {
            // The stream will not be added.
        }
    }
    return (m_activeStreamIndex >= 0);
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
            Stream &stream = m_streams.at(m_activeStreamIndex);
            remaining = stream.m_context->handleStreamMore(outputStreams, retPositions);
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
        const Stream &stream = m_streams.at(m_activeStreamIndex);
        freeable = stream.m_context->canSafelyFreeTuple(tuple);
    }
    return freeable;
}

} // namespace voltdb
