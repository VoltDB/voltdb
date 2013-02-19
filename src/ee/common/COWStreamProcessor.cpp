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

#include "COWStream.h"
#include "COWStreamProcessor.h"
#include "StreamPredicate.h"
#include "TupleSerializer.h"
#include "tabletuple.h"
#include <limits>

namespace voltdb {

/** Default constructor. */
COWStreamProcessor::COWStreamProcessor()
    : boost::ptr_vector<COWStream>()
{
    clearState();
}

/** Constructor with initial size. */
COWStreamProcessor::COWStreamProcessor(std::size_t nBuffers)
    : boost::ptr_vector<COWStream>(nBuffers)
{
    clearState();
}

/** Constructor for a single stream. Convenient for backward compatibility in tests. */
COWStreamProcessor::COWStreamProcessor(void *data, std::size_t length)
    : boost::ptr_vector<COWStream>(1)
{
    clearState();
    add(data, length);
}

/** Private method used by constructors, etc. to clear state. */
void COWStreamProcessor::clearState()
{
    m_maxTupleLength = 0;
    m_predicates = NULL;
    m_table = NULL;
}

/** Convenience method to create and add a new COWStream. */
COWStream &COWStreamProcessor::add(void *data, std::size_t length)
{
    std::auto_ptr<COWStream> out(new COWStream(data, length));
    push_back(out);
    return *out;
}

/** Start serializing. */
void COWStreamProcessor::open(PersistentTable &table,
                              std::size_t maxTupleLength,
                              int32_t partitionId,
                              int32_t totalPartitions)
{
    m_table = &table;
    m_maxTupleLength = maxTupleLength;
    m_totalPartitions = totalPartitions;
    for (COWStreamProcessor::iterator iter = begin(); iter != end(); ++iter) {
        iter->startRows(partitionId);
    }
}

/** Stop serializing. */
void COWStreamProcessor::close()
{
    for (COWStreamProcessor::iterator iter = begin(); iter != end(); ++iter) {
        iter->endRows();
    }
    clearState();
}

/**
 * Write a tuple to the output streams.
 * Expects buffer space was already checked.
 * Maintains the total byte counter provided by the caller.
 * Returns true when one of the output buffers fills.
 */
bool COWStreamProcessor::writeRow(TupleSerializer &serializer,
                                  TableTuple &tuple,
                                  std::size_t &totalBytesSerialized)
{
    if (m_table == NULL) {
        throwFatalException("COWStreamProcessor::writeRow() was called before initialize().");
    }

    // Predicates, if supplied, are one per output stream (previously asserted).
    StreamPredicateList::const_iterator iterPredicate;
    if (m_predicates != NULL) {
        iterPredicate = m_predicates->begin();
    }
    bool aBufferIsFull = false;
    for (COWStreamProcessor::iterator iter = begin(); iter != end(); ++iter) {
        // Get approval from corresponding output stream predicate, if provided.
        bool accepted = true;
        if (m_predicates != NULL) {
            accepted = iterPredicate->accept(*m_table, tuple, m_totalPartitions);
            // Keep walking through predicates in lock-step with the streams.
            ++iterPredicate;
        }
        if (accepted) {
            if (!iter->canFit(m_maxTupleLength)) {
                throwFatalException("COWStreamProcessor::writeRow() failed because buffer has no space.");
            }
            totalBytesSerialized += iter->writeRow(serializer, tuple);
            // Is this buffer capable of handling another tuple after this one is done?
            if (!aBufferIsFull && !iter->canFit(m_maxTupleLength)) {
                aBufferIsFull = true;
            }
        }
    }
    return aBufferIsFull;
}

} // namespace voltdb
