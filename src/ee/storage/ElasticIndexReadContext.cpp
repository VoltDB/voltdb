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
    m_range(), // Parsed below
    m_wrapsAround(false),
    m_wrappedAround(false)
{
    if (predicateStrings.size() != 1) {
        throwFatalException("Too many ElasticIndexReadContext predicates (>1): %ld",
                            predicateStrings.size())
    }
    if (!m_range.parse(predicateStrings.at(0))) {
        throwFatalException("Unable to parse ElasticIndexReadContext predicate \"%s\".",
                            predicateStrings.at(0).c_str())
    }
    m_wrapsAround = (m_range.m_from >= m_range.m_to);
    // Initialize the iterator to start from the range lower bounds or higher.
    m_iter = m_surgeon.indexIterator(m_range.m_from);
    if (m_iter == m_surgeon.indexEnd()) {
        if (m_wrapsAround) {
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

/*
 * Serialize to multiple output streams.
 * Return remaining tuple count, 0 if done, or -1 on error.
 * Not keeping track of the actual tuple count. Returns only maxint, 0, or -1.
 */
int64_t ElasticIndexReadContext::handleStreamMore(
        TupleOutputStreamProcessor &outputStreams,
        std::vector<int> &retPositions)
{
    int64_t remaining = std::numeric_limits<int64_t>::max();

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
                if (m_wrapsAround && !m_wrappedAround) {
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
                if ((!m_wrapsAround || m_wrappedAround) && m_iter->getHash() >= m_range.m_to) {
                    yield = true;
                    remaining = 0;
                }
            }
        }

        // Need to close the output streams and insert row counts.
        outputStreams.close();
    }

    // If more was streamed copy current positions for return.
    // Can this copy be avoided?
    for (size_t i = 0; i < outputStreams.size(); i++) {
        retPositions.push_back((int)outputStreams.at(i).position());
    }

    return remaining;
}

/**
 * Parse a hash range string and update data members.
 */
bool ElasticIndexReadContext::HashRange::parse(const std::string &predicateString)
{
    bool success = false;
    std::vector<std::string> rangeStrings = MiscUtil::splitToTwoString(predicateString, ':');
    if (rangeStrings.size() == 2) {
        try {
            m_from = boost::lexical_cast<int64_t>(rangeStrings[0]);
            m_to = boost::lexical_cast<int64_t>(rangeStrings[1]);
            success = true;
        }
        catch(boost::bad_lexical_cast) {
            // success = false
        }
    }
    return success;
}

} // namespace voltdb
