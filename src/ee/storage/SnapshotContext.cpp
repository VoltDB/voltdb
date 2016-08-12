/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "SnapshotContext.h"

#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "common/TupleOutputStream.h"
#include "common/FatalException.hpp"
#include "common/StreamPredicateList.h"
#include "logging/LogManager.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include "storage/ScanCopyOnWriteContext.h"

namespace voltdb {

/**
 * Constructor.
 */
SnapshotContext::SnapshotContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        TupleSerializer &serializer,
        int32_t partitionId,
        const std::vector<std::string> &predicateStrings,
        int64_t totalTuples) :
             TableStreamerContext(table, surgeon, partitionId, serializer, predicateStrings),
             m_copyOnWriteContext(new ScanCopyOnWriteContext(table, surgeon, partitionId, totalTuples)),
             m_tuple(table.schema()),
             m_totalTuples(totalTuples),
             m_serializationBatches(0)
{
}

/**
 * Destructor.
 */
SnapshotContext::~SnapshotContext()
{}


/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
SnapshotContext::handleActivation(TableStreamType streamType)
{
    // Only support snapshot streams.
    if (streamType != TABLE_STREAM_SNAPSHOT) {
        return ACTIVATION_UNSUPPORTED;
    }

    if (m_surgeon.hasIndex() && !m_surgeon.isIndexingComplete()) {
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN,
            "COW context activation is not allowed while elastic indexing is in progress.");
        return ACTIVATION_FAILED;
    }

    m_copyOnWriteContext->handleActivation(streamType);

    return ACTIVATION_SUCCEEDED;
}


/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t SnapshotContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                             std::vector<int> &retPositions) {

    // Don't expect to be re-called after streaming all the tuples.
    if (m_totalTuples != 0 && m_copyOnWriteContext->getTuplesRemaining() == 0) {
        throwFatalException("serializeMore() was called again after streaming completed.")
    }

    // Need to initialize the output stream list.
    if (outputStreams.empty()) {
        throwFatalException("serializeMore() expects at least one output stream.");
    }
    outputStreams.open(getTable(),
                       getMaxTupleLength(),
                       getPartitionId(),
                       getPredicates(),
                       getPredicateDeleteFlags());

    //=== Tuple processing loop

    PersistentTable &table = getTable();
    TableTuple tuple(table.schema());

    // Set to true to break out of the loop after the tuples dry up
    // or the byte count threshold is hit.
    bool yield = false;
    while (!yield) {

        // Next tuple?
        bool hasMore = m_copyOnWriteContext->advanceIterator(tuple);
        if (hasMore) {

            /*
             * Write the tuple to all the output streams.
             * Done if any of the buffers filled up.
             * The returned copy count helps decide when to delete if m_doDelete is true.
             */
            bool deleteTuple = false;
            yield = outputStreams.writeRow(getSerializer(), tuple, &deleteTuple);
            /*
             * May want to delete tuple if processing the actual table.
             */
            if (!m_copyOnWriteContext->isTableScanFinished()) {
                /*
                 * If this is the table scan, check to see if the tuple is pending
                 * delete and return the tuple if it iscop
                 */
                m_copyOnWriteContext->cleanupTuple(tuple, deleteTuple);
            }

        }
        else if (m_copyOnWriteContext->isTableScanFinished()) {
            /*
             * No more tuples in the temp table and had previously finished the
             * persistent table.
             */
            if (!m_copyOnWriteContext->cleanup()) {
                outputStreams.close();
                for (size_t i = 0; i < outputStreams.size(); i++) {
                    retPositions.push_back((int)outputStreams.at(i).position());
                }
                return TABLE_STREAM_SERIALIZATION_ERROR;
            }

        }

        // All tuples serialized, bail
        m_copyOnWriteContext->completePassIfDone(hasMore);
        if (m_copyOnWriteContext->getTuplesRemaining() == 0) {
            yield = true;
        }
    }
    // end tuple processing while loop

    // Need to close the output streams and insert row counts.
    outputStreams.close();
    // If more was streamed copy current positions for return.
    // Can this copy be avoided?
    for (size_t i = 0; i < outputStreams.size(); i++) {
        retPositions.push_back((int)outputStreams.at(i).position());
    }

    m_serializationBatches++;

    int64_t retValue = m_copyOnWriteContext->getTuplesRemaining();

    // Handle the sentinel value of -1 which is passed in from tests that don't
    // care about the active tuple count. Return max int as if there are always
    // tuples remaining (until the counter is forced to zero when done).
    if (m_copyOnWriteContext->getTuplesRemaining() < 0) {
        retValue = std::numeric_limits<int64_t>::max();
    }

    // Done when the table scan is finished and iteration is complete.
    return retValue;
}

bool SnapshotContext::notifyTupleDelete(TableTuple &tuple) {
    return m_copyOnWriteContext->notifyTupleDelete(tuple);
}

void SnapshotContext::notifyBlockWasCompactedAway(TBPtr block) {
    m_copyOnWriteContext->notifyBlockWasCompactedAway(block);
}

bool SnapshotContext::notifyTupleInsert(TableTuple &tuple) {
    return m_copyOnWriteContext->notifyTupleInsert(tuple);
}

bool SnapshotContext::notifyTupleUpdate(TableTuple &tuple) {
    return m_copyOnWriteContext->notifyTupleUpdate(tuple);
}

}
