/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
#include "storage/CopyOnWriteContext.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/TupleOutputStream.h"
#include "common/FatalException.hpp"
#include "common/StreamPredicateList.h"
#include "logging/LogManager.h"
#include <algorithm>
#include <common/debuglog.h>
#include <iostream>

namespace voltdb {

/**
 * Constructor.
 */
CopyOnWriteContext::CopyOnWriteContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        int32_t partitionId,
        const HiddenColumnFilter &hiddenColumnFilter,
        const std::vector<std::string> &predicateStrings,
        int64_t totalTuples) :
             TableStreamerContext(table, surgeon, partitionId, predicateStrings),
             m_pool(2097152, 320),
             m_totalTuples(totalTuples),
             m_tuplesRemaining(totalTuples),
             m_serializationBatches(0),
             m_replicated(table.isReplicatedTable()),
             m_hiddenColumnFilter(hiddenColumnFilter)
{
    if (m_replicated) {
        // There is a corner case where a replicated table is streamed from a thread other than the lowest
        // site thread. The only known case is rejoin snapshot where none of the target partitions are on
        // the lowest site thread.
        ScopedReplicatedResourceLock scopedLock;
        ExecuteWithMpMemory useMpMemory;
    }
}

/**
 * Destructor.
 */
CopyOnWriteContext::~CopyOnWriteContext()
{
    if (m_replicated) {
        ScopedReplicatedResourceLock scopedLock;
        ExecuteWithMpMemory useMpMemory;
    }
}


/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
CopyOnWriteContext::handleActivation(TableStreamType streamType)
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

    m_surgeon.activateSnapshot(TABLE_STREAM_SNAPSHOT);
    return ACTIVATION_SUCCEEDED;
}

/**
* Reactivation handler.
*/
TableStreamerContext::ActivationReturnCode
CopyOnWriteContext::handleReactivation(TableStreamType streamType)
{
    // Not support multiple snapshot streams.
    if (streamType == TABLE_STREAM_SNAPSHOT) {
        return ACTIVATION_FAILED;
    }
    return ACTIVATION_UNSUPPORTED;
}

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t CopyOnWriteContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                             std::vector<int> &retPositions) {

    // Don't expect to be re-called after streaming all the tuples.
    if (m_totalTuples != 0 && m_tuplesRemaining == 0) {
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

    PersistentTable &table = getTable();
    TableTuple tuple(table.schema());

    // Set to true to break out of the loop after the tuples dry up
    // or the byte count threshold is hit.
    bool yield = false;
    while (!yield) {
        bool hasMore = table.nextSnapshotTuple(tuple, TABLE_STREAM_SNAPSHOT);
        if (!hasMore) {
            yield = true;
        } else {
            if (!tuple.isNullTuple()) {
                m_tuplesRemaining--;
                if (m_tuplesRemaining < 0) {
                   // -1 is used for tests when we don't bother counting. Need to force it to 0 here.
                   m_tuplesRemaining = 0;
                }
                bool deleteTuple = false;
                yield = outputStreams.writeRow(tuple, m_hiddenColumnFilter, &deleteTuple);
            }
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

    int64_t retValue = m_tuplesRemaining;

    // Handle the sentinel value of -1 which is passed in from tests that don't
    // care about the active tuple count. Return max int as if there are always
    // tuples remaining (until the counter is forced to zero when done).
    if (m_tuplesRemaining < 0) {
        retValue = std::numeric_limits<int64_t>::max();
    }

    // Done when the table scan is finished and iteration is complete.
    return retValue;
}

}
