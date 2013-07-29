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
    m_scanner(table, surgeon.getData()),
    m_nTuplesPerCall(nTuplesPerCall)
{
    surgeon.clearIndex();
    if (predicateStrings.size() != 1) {
        throwFatalException("ElasticContext::ElasticContext() expects a single predicate.");
    }
}

ElasticContext::~ElasticContext()
{}

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or -1 on error.
 */
int64_t ElasticContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                         std::vector<int> &retPositions)
{
    if (m_surgeon.isIndexed()) {
        throwFatalException("ElasticContext::handleStreamMore() was called more than once.");
    }

    // Populate index with current tuples.
    // Table changes are tracked through notifications.
    size_t i = 0;
    TableTuple tuple(getTable().schema());
    while (m_scanner.next(tuple)) {
        if (getPredicates()[0].eval(&tuple).isTrue()) {
            m_surgeon.indexAdd(tuple);
        }
        // Take a breather after every chunk of m_n
        if (++i == m_nTuplesPerCall) {
            break;
        }
    }

    // We're done with indexing.
    bool isIndexed = m_scanner.isScanComplete();
    m_surgeon.setIsIndexed(isIndexed);
    return isIndexed ? 0 : 1;
}

/**
 * Tuple insert handler lets us add late arriving tuples to the index.
 */
bool ElasticContext::notifyTupleInsert(TableTuple &tuple)
{
    if (getPredicates()[0].eval(&tuple).isTrue()) {
        m_surgeon.indexAdd(tuple);
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
    if (m_surgeon.indexHas(tuple)) {
        bool removed = m_surgeon.indexRemove(tuple);
        assert(removed);
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
    if (m_surgeon.indexHas(sourceTuple)) {
        bool removed = m_surgeon.indexRemove(sourceTuple);
        assert(removed);
    }
    if (getPredicates()[0].eval(&targetTuple).isTrue()) {
        bool added = m_surgeon.indexAdd(targetTuple);
        assert(added);
    }
}

} // namespace voltdb
