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

#include "storage/ElasticContext.h"
#include "storage/ElasticIndex.h"
#include "storage/persistenttable.h"
#include "common/TupleOutputStreamProcessor.h"
#include "common/FixUnusedAssertHack.h"

namespace voltdb {

ElasticContext::ElasticContext(PersistentTable &table,
                               PersistentTableSurgeon &surgeon,
                               int32_t partitionId,
                               TupleSerializer &serializer,
                               const std::vector<std::string> &predicateStrings,
                               size_t nTuplesPerCall) :
    TableStreamerContext(table, surgeon, partitionId, serializer, predicateStrings),
    m_nTuplesPerCall(nTuplesPerCall),
    m_indexActive(false)
{
    if (predicateStrings.size() != 1) {
        throwFatalException("ElasticContext::ElasticContext() expects a single predicate.");
    }
}

ElasticContext::~ElasticContext()
{}

/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
ElasticContext::handleActivation(TableStreamType streamType)
{
    // Can't activate an indexing stream during a snapshot.
    if (m_surgeon.hasStreamType(TABLE_STREAM_SNAPSHOT)) {
        VOLT_ERROR("Elastic context activation is not allowed while a snapshot is in progress.");
        return ACTIVATION_FAILED;
    }

    // Create the index?
    if (streamType == TABLE_STREAM_ELASTIC_INDEX) {
        // Don't allow activation if there's an existing index.
        if (m_surgeon.hasIndex()) {
            VOLT_ERROR("Elastic context activation is not allowed while an index is "
                       "present that has not been completely consumed.");
            return ACTIVATION_FAILED;
        }
        m_surgeon.createIndex();
        m_scanner.reset(new ElasticScanner(getTable(), m_surgeon.getData()));
        m_indexActive = true;
        return ACTIVATION_SUCCEEDED;
    }

    // Clear the index?
    if (streamType == TABLE_STREAM_ELASTIC_INDEX_CLEAR) {
        if (!m_surgeon.isIndexEmpty()) {
            VOLT_ERROR("Elastic index clear is not allowed while an index is "
                       "present that has not been completely consumed.");
            return ACTIVATION_FAILED;
        }
        m_surgeon.dropIndex();
        m_scanner.reset();
        m_indexActive = false;
        return ACTIVATION_SUCCEEDED;
    }

    // It wasn't one of the supported stream types.
    return ACTIVATION_UNSUPPORTED;
}

/**
 * Reactivation handler.
 */
TableStreamerContext::ActivationReturnCode
ElasticContext::handleReactivation(TableStreamType streamType)
{
    return handleActivation(streamType);
}

/**
 * Deactivation handler.
 */
bool ElasticContext::handleDeactivation(TableStreamType streamType)
{
    // Keep this context around to maintain the index.
    return true;
}

/*
 * Serialize to output stream.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t ElasticContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                         std::vector<int> &retPositions)
{
    if (!m_surgeon.hasIndex()) {
        VOLT_ERROR("Elastic streaming was invoked without proper activation.");
        return TABLE_STREAM_SERIALIZATION_ERROR;
    }
    if (m_surgeon.isIndexingComplete()) {
        VOLT_ERROR("Elastic streaming was called after indexing had already completed.");
        return TABLE_STREAM_SERIALIZATION_ERROR;
    }

    // Populate index with current tuples.
    // Table changes are tracked through notifications.
    size_t i = 0;
    TableTuple tuple(getTable().schema());
    while (m_scanner->next(tuple)) {
        if (getPredicates()[0].eval(&tuple).isTrue()) {
            m_surgeon.indexAdd(tuple);
        }
        // Take a breather after every chunk of m_nTuplesPerCall tuples.
        if (++i == m_nTuplesPerCall) {
            break;
        }
    }

    // Done with indexing?
    bool indexingComplete = m_scanner->isScanComplete();
    if (indexingComplete) {
        m_surgeon.setIndexingComplete();
    }
    return indexingComplete ? 0 : 1;
}

/**
 * Tuple insert handler lets us add late arriving tuples to the index.
 */
bool ElasticContext::notifyTupleInsert(TableTuple &tuple)
{
    if (m_indexActive) {
        StreamPredicateList &predicates = getPredicates();
        assert(predicates.size() > 0);
        if (predicates[0].eval(&tuple).isTrue()) {
            m_surgeon.indexAdd(tuple);
        }
    }
    return true;
}

/**
 * Tuple update handler is not currently needed.
 */
bool ElasticContext::notifyTupleUpdate(TableTuple &tuple)
{
    return true;
}

/**
 * Tuple delete handler lets us erase tuples from the index.
 */
bool ElasticContext::notifyTupleDelete(TableTuple &tuple)
{
    if (m_indexActive) {
        if (m_surgeon.indexHas(tuple)) {
            bool removed = m_surgeon.indexRemove(tuple);
            assert(removed);
        }
    }
    return true;
}

/**
 * Tuple compaction handler lets us reindex when a tuple's address changes.
 */
void ElasticContext::notifyTupleMovement(TBPtr sourceBlock,
                                         TBPtr targetBlock,
                                         TableTuple &sourceTuple,
                                         TableTuple &targetTuple)
{
    if (m_indexActive) {
        StreamPredicateList &predicates = getPredicates();
        assert(predicates.size() > 0);
        if (m_surgeon.indexHas(sourceTuple)) {
            bool removed = m_surgeon.indexRemove(sourceTuple);
            assert(removed);
        }
        if (getPredicates()[0].eval(&targetTuple).isTrue()) {
            bool added = m_surgeon.indexAdd(targetTuple);
            assert(added);
        }
    }
}

} // namespace voltdb
