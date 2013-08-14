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

#include <boost/lexical_cast.hpp>
#include "common/MiscUtil.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "storage/ElasticIndexReadContext.h"
#include "storage/persistenttable.h"

namespace voltdb
{

/**
 * Constructor.
 * Parse and hold onto an "XXX:YYY" range predicate locally.
 */
ElasticIndexReadContext::ElasticIndexReadContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        int32_t partitionId,
        TupleSerializer &serializer,
        const std::vector<std::string> &predicateStrings) :
    TableStreamerContext(table, surgeon, partitionId, serializer),
    m_range(parseHashRange(predicateStrings)),   // throws on error
    m_wrappedAround(false)
{
    // Make sure the index is available and complete.
    if (!m_surgeon.hasIndex()) {
        throwFatalException("Attempted to create ElasticIndexReadContext with no index available.");
    }
    if (!m_surgeon.isIndexingComplete()) {
        throwFatalException("Attempted to create ElasticIndexReadContext before indexing was completed.");
    }
    // Initialize the iterator to start from the range lower bounds or higher.
    m_iter = m_surgeon.indexIterator(m_range.m_from);
    if (m_iter == m_surgeon.indexEnd()) {
        if (m_range.wrapsAround()) {
            m_iter = m_surgeon.indexIterator(std::numeric_limits<int64_t>::min());
            m_wrappedAround = true;
        }
    }
}

/**
 * Destructor.
 */
ElasticIndexReadContext::~ElasticIndexReadContext()
{}

/**
 * Activation handler.
 */
bool ElasticIndexReadContext::handleActivation(TableStreamType streamType)
{
    if (!m_surgeon.hasIndex() || !m_surgeon.isIndexingComplete()) {
        VOLT_ERROR("Elastic index consumption is not allowed until index generation completes.");
        return false;
    }
    return true;
}

/*
 * Serialize to output stream. Receive a list of streams, but expect only 1.
 * Return 1 if tuples remain, 0 if done, or -1 on error.
 */
int64_t ElasticIndexReadContext::handleStreamMore(
        TupleOutputStreamProcessor &outputStreams,
        std::vector<int> &retPositions)
{
    int64_t remaining = 1;

    // End of iteration or wrap around?
    if (m_iter == m_surgeon.indexEnd()) {
        remaining = 0;
    }

    // Need to initialize the output stream list.
    if (outputStreams.size() != 1) {
        throwFatalException("serializeMore() expects exactly one output stream.");
    }

    if (remaining != 0) {
        outputStreams.open(getTable(),
                           getMaxTupleLength(),
                           getPartitionId(),
                           getPredicates(),
                           getPredicateDeleteFlags());

        const TupleSchema *schema = getTable().schema();
        assert(schema != NULL);

        // Set to true to break out of the loop after the tuples dry up
        // or the byte count threshold is hit.
        bool yield = false;
        while (!yield) {
            // Write the tuple.
            // The delete flag should not be set.
            TableTuple tuple(m_iter->getTupleAddress(), schema);
            bool deleteTuple = false;
            yield = outputStreams.writeRow(getSerializer(), tuple, deleteTuple);
            assert(deleteTuple == false);
            if (++m_iter == m_surgeon.indexEnd()) {
                if (m_range.wrapsAround() && !m_wrappedAround) {
                    m_iter = m_surgeon.indexIterator(std::numeric_limits<int64_t>::min());
                    if (m_iter == m_surgeon.indexEnd()) {
                        yield = true;
                        remaining = 0;
                    }
                    else {
                        m_wrappedAround = true;
                    }
                }
                else {
                    yield = true;
                    remaining = 0;
                }
            }
            else {
                // End of range (if already wrapped around or didn't need to)?
                if ((!m_range.wrapsAround() || m_wrappedAround) && m_iter->getHash() >= m_range.m_to) {
                    yield = true;
                    remaining = 0;
                }
            }
        }

        // Need to close the output streams and insert row counts.
        outputStreams.close();
    }

    // If more was streamed copy current position for return (exactly one stream).
    retPositions.push_back((int)outputStreams.at(0).position());

    // Drop the index after completely consuming it.
    if (remaining <= 0) {
        m_surgeon.dropIndex();
    }

    return remaining;
}

/**
 * Parse and validate the hash range.
 */
ElasticIndexReadContext::HashRange ElasticIndexReadContext::parseHashRange(
        const std::vector<std::string> &predicateStrings)
{
    if (predicateStrings.size() != 1) {
        throwFatalException("Too many ElasticIndexReadContext predicates (>1): %ld",
                            predicateStrings.size())
    }
    std::vector<std::string> rangeStrings = MiscUtil::splitToTwoString(predicateStrings.at(0), ':');
    if (rangeStrings.size() == 2) {
        try {
            return ElasticIndexReadContext::HashRange(boost::lexical_cast<int64_t>(rangeStrings[0]),
                                                      boost::lexical_cast<int64_t>(rangeStrings[1]));
        }
        catch(boost::bad_lexical_cast) {
            // Throws below.
        }
    }
    throwFatalException("Unable to parse ElasticIndexReadContext predicate \"%s\".",
                        predicateStrings.at(0).c_str())
}

} // namespace voltdb
