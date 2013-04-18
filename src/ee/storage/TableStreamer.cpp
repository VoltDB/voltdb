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
#include "TableStreamer.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"

namespace voltdb
{

typedef std::pair<CatalogId, Table*> TIDPair;

/**
 * Use a factory method for construction.
 */
TableStreamer::TableStreamer(TableStreamType streamType, int32_t partitionId, bool doDelete) :
    m_streamType(streamType),
    m_partitionId(partitionId),
    m_doDelete(doDelete)
{}

TableStreamer::~TableStreamer()
{}

boost::shared_ptr<TableStreamer> TableStreamer::fromMessage(
    TableStreamType streamType,
    int32_t partitionId,
    ReferenceSerializeInput &serializeIn)
{
    // Defaults to a NULL pointer for unknown types.
    boost::shared_ptr<TableStreamer> tableStream;

    switch (streamType) {

        case TABLE_STREAM_SNAPSHOT: {
            bool doDelete = (serializeIn.readByte() != 0);
            tableStream.reset(new TableStreamer(streamType, partitionId, doDelete));
            int npreds = serializeIn.readInt();
            if (npreds > 0) {
                tableStream->m_predicateStrings.reserve(npreds);
                for (int ipred = 0; ipred < npreds; ipred++) {
                    std::string spred = serializeIn.readTextString();
                    tableStream->m_predicateStrings.push_back(spred);
                }
            }
            break;
        }

        case TABLE_STREAM_RECOVERY:
            tableStream.reset(new TableStreamer(streamType, partitionId, false));
            break;
    }

    return tableStream;
}

boost::shared_ptr<TableStreamer> TableStreamer::fromData(
     TableStreamType streamType,
     int32_t partitionId,
     bool doDelete,
     std::vector<std::string> &predicateStrings)
{
    // Defaults to a NULL pointer for unknown types.
    boost::shared_ptr<TableStreamer> tableStream;

    switch (streamType) {

        case TABLE_STREAM_SNAPSHOT: {
            tableStream.reset(new TableStreamer(streamType, partitionId, doDelete));
            if (!predicateStrings.empty()) {
                tableStream->m_predicateStrings.reserve(predicateStrings.size());
                for (std::vector<std::string>::const_iterator ipred = predicateStrings.begin();
                     ipred != predicateStrings.end(); ++ipred) {
                    tableStream->m_predicateStrings.push_back(*ipred);
                }
            }
            break;
        }

        case TABLE_STREAM_RECOVERY:
            tableStream.reset(new TableStreamer(streamType, partitionId, false));
            break;
    }

    return tableStream;
}

boost::shared_ptr<TableStreamer> TableStreamer::fromData(TableStreamType streamType,
                                                         int32_t partitionId,
                                                         bool doDelete)
{
    std::vector<std::string> predicateStrings;
    return fromData(streamType, partitionId, doDelete, predicateStrings);
}

bool TableStreamer::isAlreadyActive() const
{
    switch (m_streamType) {
        case TABLE_STREAM_SNAPSHOT:
            return (m_COWContext.get() != NULL);
        case TABLE_STREAM_RECOVERY:
            return (m_recoveryContext.get() != NULL);
    }
    return false;
}

bool TableStreamer::activateStream(PersistentTable &table)
{
    CatalogId tableId = table.databaseId();

    switch (m_streamType) {
        case TABLE_STREAM_SNAPSHOT: {
            if (m_COWContext.get() != NULL) {
                // true => COW already active
                return true;
            }
            try {
                // Constructor can throw exception when it parses the predicates.
                CopyOnWriteContext *newCOW =
                    new CopyOnWriteContext(table, m_tupleSerializer, m_partitionId,
                                           m_predicateStrings, table.activeTupleCount(),
                                           m_doDelete);
                m_COWContext.reset(newCOW);
            }
            catch(SerializableEEException &e) {
                VOLT_ERROR("SerializableEEException: %s", e.message().c_str());
                return false;
            }
            break;
        }

        case TABLE_STREAM_RECOVERY:
            if (m_recoveryContext.get() != NULL) {
                // Recovery context already active.
                return true;
            }
            m_recoveryContext.reset(new RecoveryContext(&table, tableId));
            break;

        default:
            assert(false);
    }

    return true;
}

int64_t TableStreamer::streamMore(TupleOutputStreamProcessor &outputStreams,
                                  std::vector<int> &retPositions)
{
    int64_t remaining = -2;
    switch (m_streamType) {
        case TABLE_STREAM_SNAPSHOT: {
            if (m_COWContext.get() == NULL) {
                throwFatalException("TableStreamer::continueStreaming: Expect non-null COW context.");
            }
            remaining = m_COWContext->serializeMore(outputStreams);
            // If more was streamed copy current positions for return.
            // Can this copy be avoided?
            for (size_t i = 0; i < outputStreams.size(); i++) {
                retPositions.push_back((int)outputStreams.at(i).position());
            }
            if (remaining <= 0) {
                m_COWContext.reset(NULL);
            }
            break;
        }

        case TABLE_STREAM_RECOVERY: {
            if (m_recoveryContext.get() == NULL) {
                throwFatalException("TableStreamer::continueStreaming: Expect non-null recovery context.");
            }
            if (outputStreams.size() != 1) {
                throwFatalException("TableStreamer::continueStreaming: Expect 1 output stream "
                                    "for recovery, received %ld", outputStreams.size());
            }
            /*
             * Table ids don't change during recovery because
             * catalog changes are not allowed.
             */
            bool hasMore = m_recoveryContext->nextMessage(&outputStreams[0]);
            // Non-zero if some tuples remain, we're just not sure how many.
            remaining = (hasMore ? 1 : 0);
            for (size_t i = 0; i < outputStreams.size(); i++) {
                retPositions.push_back((int)outputStreams.at(i).position());
            }
            if (!hasMore) {
                m_recoveryContext.reset(NULL);
            }
            break;
        }

        default:
            // Failure
            remaining = -2;
    }

    return remaining;
}

/**
 * Block compaction hook.
 */
void TableStreamer::notifyBlockWasCompactedAway(TBPtr block) {
    if (m_COWContext.get() != NULL) {
        m_COWContext->notifyBlockWasCompactedAway(block);
    }
}

/**
 * Tuple insert hook.
 * Return true if it was handled by the COW context.
 */
bool TableStreamer::notifyTupleInsert(TableTuple &tuple) {
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(tuple, true);
        return true;
    }
    else {
        return false;
    }
}

/**
 * Tuple update hook.
 * Return true if it was handled by the COW context.
 */
bool TableStreamer::notifyTupleUpdate(TableTuple &tuple) {
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(tuple, false);
        return true;
    }
    else {
        return false;
    }
}

/**
 * Return true if a tuple can be freed safely.
 */
bool TableStreamer::canSafelyFreeTuple(TableTuple &tuple) const {
    return (m_COWContext == NULL || m_COWContext->canSafelyFreeTuple(tuple));
}

} // namespace voltdb
