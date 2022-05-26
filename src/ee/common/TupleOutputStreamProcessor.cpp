/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "TupleOutputStream.h"
#include "TupleOutputStreamProcessor.h"
#include "tabletuple.h"

namespace voltdb {

/** Default constructor. */
TupleOutputStreamProcessor::TupleOutputStreamProcessor()
    : boost::ptr_vector<TupleOutputStream>()
{
    clearState();
}

/** Constructor with initial size. */
TupleOutputStreamProcessor::TupleOutputStreamProcessor(std::size_t nBuffers)
    : boost::ptr_vector<TupleOutputStream>(nBuffers)
{
    clearState();
}

/** Constructor for a single stream. Convenient for backward compatibility in tests. */
TupleOutputStreamProcessor::TupleOutputStreamProcessor(void *data, std::size_t length)
    : boost::ptr_vector<TupleOutputStream>(1)
{
    clearState();
    add(data, length);
}

/** Private method used by constructors, etc. to clear state. */
void TupleOutputStreamProcessor::clearState()
{
    m_maxTupleLength = 0;
    m_predicates = NULL;
    m_table = NULL;
}

/** Convenience method to create and add a new TupleOutputStream. */
TupleOutputStream &TupleOutputStreamProcessor::add(void *data, std::size_t length)
{
    std::auto_ptr<TupleOutputStream> out(new TupleOutputStream(data, length));
    push_back(out);
    return *out;
}

/** Start serializing. */
void TupleOutputStreamProcessor::open(PersistentTable &table,
                                      std::size_t maxTupleLength,
                                      int32_t partitionId,
                                      StreamPredicateList &predicates,
                                      std::vector<bool> &predicateDeletes)
{
    m_table = &table;
    m_maxTupleLength = maxTupleLength;
    // It must be either one predicate per output stream or none at all.
    bool havePredicates = !predicates.empty();
    if (havePredicates && predicates.size() != size()) {
        throwFatalException("serializeMore() expects either no predicates or one per output stream.");
    }
    m_predicates = &predicates;
    m_predicateDeletes = &predicateDeletes;
    for (TupleOutputStreamProcessor::iterator iter = begin(); iter != end(); ++iter) {
        iter->startRows(partitionId);
    }
}

/** Stop serializing. */
void TupleOutputStreamProcessor::close()
{
    for (TupleOutputStreamProcessor::iterator iter = begin(); iter != end(); ++iter) {
        iter->endRows();
    }
    clearState();
}

/**
 * Write a tuple to the output streams.
 * Expects buffer space was already checked.
 * Returns true when the caller should yield to allow other work to proceed.
 */
bool TupleOutputStreamProcessor::writeRow(TableTuple &tuple, const HiddenColumnFilter &hiddenColumnFilter, bool *deleteRow)
{
    if (m_table == NULL) {
        throwFatalException("TupleOutputStreamProcessor::writeRow() was called before open().");
    }

    // Predicates, if supplied, are one per output stream (previously asserted).
    StreamPredicateList::iterator ipredicate;
    std::vector<bool>::iterator iDeleteFlag;
    vassert(m_predicates != NULL);

    if (!m_predicates->empty()) {
        ipredicate = m_predicates->begin();
        iDeleteFlag = m_predicateDeletes->begin();
    }

    bool yield = false;
    for (TupleOutputStreamProcessor::iterator iter = begin(); iter != end(); ++iter) {
        // Get approval from corresponding output stream predicate, if provided.
        bool accepted = true;
        if (!m_predicates->empty()) {
            if (!boost::is_null(ipredicate)) {
                accepted = ipredicate->eval(&tuple).isTrue();
            }
            // Keep walking through predicates in lock-step with the streams.
            // As with first() we expect a predicate to be available for each and every stream.
            // It was already checked, so just assert here.
            vassert(ipredicate != m_predicates->end());
            if (accepted && deleteRow != NULL) {
                (*deleteRow) = (*deleteRow) || *iDeleteFlag;
            }
            ++ipredicate;
            ++iDeleteFlag;
        }

        if (accepted) {
            if (!iter->canFit(m_maxTupleLength)) {
                throwFatalException(
                    "TupleOutputStreamProcessor::writeRow() failed because buffer has no space.");
            }
            iter->writeRow(tuple, hiddenColumnFilter);

            // Check if we'll need to yield after handling this row.
            if (!yield) {
                // Yield when the buffer is not capable of handling another tuple
                // or when the total bytes serialized threshold is exceeded.
                yield = (   !iter->canFit(m_maxTupleLength)
                         || iter->getTotalBytesSerialized() > m_bytesSerializedThreshold);
            }
        }
    }
    return yield;
}

} // namespace voltdb
