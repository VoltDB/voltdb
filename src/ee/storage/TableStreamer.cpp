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
#include "common/serializeio.h"
#include "storage/persistenttable.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/ElasticContext.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"

namespace voltdb
{

typedef std::pair<CatalogId, Table*> TIDPair;

/**
 * Constructor with data from serialized message.
 */
TableStreamer::TableStreamer(TupleSerializer &tupleSerializer,
                   TableStreamType streamType,
                   int32_t partitionId,
                   ReferenceSerializeInput &serializeIn) :
    m_tupleSerializer(tupleSerializer),
    m_streamType(streamType),
    m_partitionId(partitionId)
{
    // Grab the predicates and delete flag for snapshots or elastic contexts.
    if (streamType == TABLE_STREAM_SNAPSHOT || streamType == TABLE_STREAM_ELASTIC) {
        int npreds = serializeIn.readInt();
        if (npreds > 0) {
            m_predicateStrings.reserve(npreds);
            for (int ipred = 0; ipred < npreds; ipred++) {
                std::string spred = serializeIn.readTextString();
                m_predicateStrings.push_back(spred);
            }
        }
    }
}

TableStreamer::~TableStreamer()
{}

bool TableStreamer::activateStream(PersistentTable &table, CatalogId tableId)
{
    if (m_context == NULL) {
        // This is the only place that can create a streaming context based on
        // the stream type. Other places shouldn't need to know about the
        // context sub-types.
        try {
            switch (m_streamType) {
                case TABLE_STREAM_SNAPSHOT: {
                    // Constructor can throw exception when it parses the predicates.
                    CopyOnWriteContext *newContext =
                        new CopyOnWriteContext(table, m_tupleSerializer, m_partitionId,
                                               m_predicateStrings, table.activeTupleCount());
                    m_context.reset(newContext);
                    break;
                }

                case TABLE_STREAM_RECOVERY:
                    m_context.reset(new RecoveryContext(table, tableId));
                    break;

                case TABLE_STREAM_ELASTIC:
                    m_context.reset(new ElasticContext(table, m_predicateStrings));
                    break;

                default:
                    assert(false);
            }
        }
        catch(SerializableEEException &e) {
            // m_context will be NULL if we get an exception.
        }
    }

    return (m_context != NULL);
}

int64_t TableStreamer::streamMore(TupleOutputStreamProcessor &outputStreams,
                                  std::vector<int> &retPositions)
{
    int64_t remaining = -2;
    if (m_context == NULL) {
        remaining = -1;
    }
    else {
        remaining = m_context->handleStreamMore(outputStreams, retPositions);
    }
    if (remaining <= 0) {
        m_context.reset(NULL);
    }

    return remaining;
}

/**
 * Return true if a tuple can be freed safely.
 */
bool TableStreamer::canSafelyFreeTuple(TableTuple &tuple) const
{
    bool freeable = true;
    if (m_context != NULL) {
        freeable = m_context->canSafelyFreeTuple(tuple);
    }
    return freeable;
}

} // namespace voltdb
