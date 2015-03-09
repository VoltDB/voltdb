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
#include "logging/LogManager.h"

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

TableStreamerInterface* TableStreamer::cloneForTruncatedTable(PersistentTableSurgeon &surgeon)
{
    TableStreamer* the_clone = new TableStreamer(m_partitionId, surgeon.getTable(), m_tableId);
    surgeon.initTableStreamer(the_clone);
    BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
        assert(streamPtr != NULL);
        boost::shared_ptr<TableStreamerContext> cloned_context;
        cloned_context.reset(streamPtr->m_context->cloneForTruncatedTable(surgeon));
        if (cloned_context != NULL) {
            the_clone->m_streams.push_back(StreamPtr(new Stream(streamPtr->m_streamType,
                                                                cloned_context)));
        }
    }
    return the_clone;
}

/**
 * activateStream() knows how to create streams based on type.
 * Context classes determine whether or not reactivation is allowed.
 */
bool TableStreamer::activateStream(PersistentTableSurgeon &surgeon,
                                   TupleSerializer &serializer,
                                   TableStreamType streamType,
                                   const std::vector<std::string> &predicateStrings)
{
    bool failed = false;
    bool found = false;
    BOOST_FOREACH(StreamPtr &streamPtr, m_streams) {
        assert(streamPtr != NULL);
        switch (streamPtr->m_context->handleReactivation(streamType)) {
            case TableStreamerContext::ACTIVATION_SUCCEEDED:
                streamPtr->m_context->updatePredicates(predicateStrings);
                found = true;
                break;
            case TableStreamerContext::ACTIVATION_FAILED:
                failed = true;
                break;
            case TableStreamerContext::ACTIVATION_UNSUPPORTED:
                break;
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

                case TABLE_STREAM_ELASTIC_INDEX_CLEAR:
                    VOLT_DEBUG("Clear elastic index before materializing it.");
                    // not an error
                    break;

                default:
                    assert(false);
            }
            if (context) {
                TableStreamerContext::ActivationReturnCode retcode = context->handleActivation(streamType);
                switch (retcode) {
                    case TableStreamerContext::ACTIVATION_SUCCEEDED:
                        // Activation was accepted by the new context. Attach it to a stream.
                        m_streams.push_back(StreamPtr(new Stream(streamType, context)));
                        break;
                    case TableStreamerContext::ACTIVATION_FAILED:
                        // Activation was rejected by the new context.
                        // Let the context disappear when it goes out of scope.
                        failed = true;
                        break;
                    default:
                        throwFatalException("Unexpected activation return code from new context handleActivation(): %d",
                                            static_cast<int>(retcode))
                        break;
                }
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
    int64_t remaining = TABLE_STREAM_SERIALIZATION_ERROR;

    if (m_streams.empty()) {
        char errMsg[1024];
        snprintf(errMsg, 1024, "Table streamer has no streams to serialize more for table %s.",
                 m_table.name().c_str());
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, errMsg);
    }

    // Rebuild the stream list as dictated by context semantics.
    StreamList savedStreams(m_streams);
    m_streams.clear();
    for (StreamList::iterator iter = savedStreams.begin(); iter != savedStreams.end(); ++iter) {
        StreamPtr streamPtr = *iter;
        assert(streamPtr->m_context != NULL);
        if (streamPtr->m_streamType == streamType) {
            // Assert that we didn't find the stream type twice.
            assert(remaining == TABLE_STREAM_SERIALIZATION_ERROR);
            remaining = streamPtr->m_context->handleStreamMore(outputStreams, retPositions);
            if (remaining <= 0) {
                // Drop the stream if it doesn't need to hang around (e.g. elastic).
                if (streamPtr->m_context->handleDeactivation(streamType)) {
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

} // namespace voltdb
