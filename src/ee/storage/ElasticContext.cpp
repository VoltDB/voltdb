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

namespace voltdb {

ElasticContext::ElasticContext(PersistentTable &table,
                               const std::vector<std::string> &predicateStrings) :
    TableStreamerContext(table, predicateStrings),
    m_scanner(table)
{
    if (predicateStrings.size() != 1) {
        throwFatalException("ElasticContext() expects a single predicate");
    }
}

ElasticContext::~ElasticContext()
{
}

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or -1 on error.
 */
int64_t ElasticContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                         std::vector<int> &retPositions)
{
    PersistentTable &table = getTable();
    TableTuple tuple(table.schema());
    while (m_scanner.next(tuple)) {
        if (getPredicates()[0].eval(&tuple).isTrue()) {
            m_index.add(table, tuple);
        }
    }
    return 0;
}

/**
 * Tuple insert handler lets us add late arriving tuples to the index.
 */
bool ElasticContext::notifyTupleInsert(TableTuple &tuple)
{
    PersistentTable &table = getTable();
    if (getPredicates()[0].eval(&tuple).isTrue()) {
        m_index.add(table, tuple);
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
    PersistentTable &table = getTable();
    if (m_index.has(table, tuple)) {
#ifdef DEBUG
        bool removed =
#endif
        m_index.remove(table, tuple);
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
    PersistentTable &table = getTable();
    if (m_index.has(table, sourceTuple)) {
#ifdef DEBUG
        bool removed =
#endif
        m_index.remove(getTable(), sourceTuple);
        assert(removed);
    }
    if (getPredicates()[0].eval(&targetTuple).isTrue()) {
#ifdef DEBUG
        bool added =
#endif
        m_index.add(getTable(), targetTuple);
        assert(added);
    }
}

} // namespace voltdb
