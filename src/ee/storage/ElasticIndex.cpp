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

#include "ElasticIndex.h"
#include "persistenttable.h"

namespace voltdb
{

/**
 * Generate hash value for key.
 */
ElasticHash ElasticIndex::generateHash(const PersistentTable &table, const TableTuple &tuple)
{
    int64_t hashValues[2];
    tuple.getNValue(table.partitionColumn()).murmurHash3(hashValues);
    // Only the least significant 8 bytes is used.
    return hashValues[0];
}

ElasticIndexTupleRangeIterator::ElasticIndexTupleRangeIterator(
        ElasticIndex &index,
        const TupleSchema &schema,
        const ElasticIndexHashRange &range) :
    m_index(index),
    m_schema(schema),
    m_range(range)
{
    reset();
}

void ElasticIndexTupleRangeIterator::reset()
{
    m_iter = m_index.createLowerBoundIterator(m_range.getLowerBound());
    if (m_range.wrapsAround()) {
        m_end = m_index.end();
        m_lastIteration = false;
    }
    else {
        m_end = m_index.createUpperBoundIterator(m_range.getUpperBound());
        m_lastIteration = true;
    }
}

bool ElasticIndexTupleRangeIterator::wrap()
{
    if (m_lastIteration) {
        return false;
    }
    // Wrap back to the beginning.
    m_iter = m_index.createIterator();
    m_end  = m_index.createUpperBoundIterator(m_range.getUpperBound());
    m_lastIteration = true;
    if (m_iter == m_end) {
        return false;
    }
    return true;
}

bool ElasticIndexTupleRangeIterator::next(TableTuple &tuple)
{
    if (m_iter == m_end) {
        if (!wrap()) {
            return false;
        }
    }
    tuple = TableTuple(m_iter++->getTupleAddress(), &m_schema);
    return true;
}

} // namespace voltdb
