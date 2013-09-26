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
    m_predicateStrings(predicateStrings),
    m_materialized(false)
{}

/**
 * Destructor.
 */
ElasticIndexReadContext::~ElasticIndexReadContext()
{}

/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
ElasticIndexReadContext::handleActivation(TableStreamType streamType)
{
    if (streamType != TABLE_STREAM_ELASTIC_INDEX_READ) {
        return ACTIVATION_UNSUPPORTED;
    }

    if (!m_surgeon.hasIndex()) {
        VOLT_ERROR("There is no index to materialize.");
        return ACTIVATION_FAILED;
    }

    if (!m_surgeon.isIndexingComplete()) {
        VOLT_ERROR("Index generation has not completed.");
        return ACTIVATION_FAILED;
    }

    ElasticIndexHashRange range;
    if (!parseHashRange(m_predicateStrings, range)) {
        return ACTIVATION_FAILED;
    }

    m_iter = m_surgeon.getIndexTupleRangeIterator(range);
    return ACTIVATION_SUCCEEDED;
}

/**
 * Deactivation handler.
 */
bool ElasticIndexReadContext::handleDeactivation(TableStreamType streamType)
{
    if (streamType == TABLE_STREAM_ELASTIC_INDEX_READ) {
        // This context is no longer needed.
        return false;
    }

    // Fall through for other unsupported stream types.
    throwFatalException("Unexpected stream type %d in handleDeactivation().", static_cast<int>(streamType))
    return false;
}

/*
 * Serialize to output stream. Receive a list of streams, but expect only 1.
 * Return 1 if tuples remain, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t ElasticIndexReadContext::handleStreamMore(
        TupleOutputStreamProcessor &outputStreams,
        std::vector<int> &retPositions)
{
    // Default to success.
    int64_t remaining = 1;

    // Check that activation happened.
    if (m_iter == NULL) {
        VOLT_ERROR("Attempted to begin serialization without activating the context.");
        remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    }

    // Need to initialize the output stream list.
    else if (outputStreams.size() != 1) {
        VOLT_ERROR("serializeMore() expects exactly one output stream.");
        remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    }

    else {
        // Anything left?
        TableTuple tuple;
        if (!m_iter->next(tuple)) {
            remaining = 0;
        }

        // More tuples are available - continue streaming and iterating.
        if (remaining != 0) {
            outputStreams.open(getTable(),
                               getMaxTupleLength(),
                               getPartitionId(),
                               getPredicates(),
                               getPredicateDeleteFlags());

            // Set to true to break out of the loop after the tuples dry up
            // or the byte count threshold is hit.
            bool yield = false;
            while (!yield) {
                // Write the tuple.
                yield = outputStreams.writeRow(getSerializer(), tuple);
                if (!yield) {
                    if (!m_iter->next(tuple)) {
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

        // After the index is completely consumed delete index entries and referenced tuples.
        if (remaining <= 0) {
            m_materialized = true;
            deleteStreamedTuples();
        }
    }

    return remaining;
}

/**
 * Parse and validate the hash range.
 */
bool ElasticIndexReadContext::parseHashRange(
        const std::vector<std::string> &predicateStrings,
        ElasticIndexHashRange &rangeOut)
{
    bool success = false;
    if (predicateStrings.size() != 1) {
        VOLT_ERROR("Too many ElasticIndexReadContext predicates (>1): %ld",
                   predicateStrings.size());
    }
    else {
        std::vector<std::string> rangeStrings = MiscUtil::splitToTwoString(predicateStrings.at(0), ':');
        if (rangeStrings.size() == 2) {
            try {
                rangeOut = ElasticIndexHashRange(boost::lexical_cast<int64_t>(rangeStrings[0]),
                                                 boost::lexical_cast<int64_t>(rangeStrings[1]));
                success = true;
            }
            catch(boost::bad_lexical_cast) {
                VOLT_ERROR("Unable to parse ElasticIndexReadContext predicate \"%s\".",
                           predicateStrings.at(0).c_str());
            }
        }
    }
    return success;
}

/**
 * Clean up after consuming indexed tuples.
 */
void ElasticIndexReadContext::deleteStreamedTuples()
{
    // Delete the indexed tuples that were streamed.
    // Undo token release will cause the index to delete the corresponding items
    // via notifications.
    m_iter->reset();
    TableTuple tuple;
    while (m_iter->next(tuple)) {
        m_surgeon.deleteTuple(tuple);
    }
}

} // namespace voltdb
