/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

/** Constructor with initial size. */
TupleOutputStreamProcessor::TupleOutputStreamProcessor(std::size_t nBuffers) : super(nBuffers) {
}

/** Constructor for a single stream. Convenient for backward compatibility in tests. */
TupleOutputStreamProcessor::TupleOutputStreamProcessor(void *data, std::size_t length) : super(1) {
    add(data, length);
}

/** Convenience method to create and add a new TupleOutputStream. */
TupleOutputStream &TupleOutputStreamProcessor::add(void *data, std::size_t length) {
    std::auto_ptr<TupleOutputStream> out(new TupleOutputStream(data, length));
    push_back(out);
    return *out;
}

/** Start serializing. */
void TupleOutputStreamProcessor::open(std::size_t maxTupleLength,
                                      int32_t partitionId,
                                      StreamPredicateList const& predicates,
                                      std::vector<bool> const& predicateDeletes) {
    m_opened = true;
    m_maxTupleLength = maxTupleLength;
    // It must be either one predicate per output stream or none at all.
    bool havePredicates = !predicates.empty();
    if (havePredicates && predicates.size() != size()) {
        throwFatalException("serializeMore() expects either no predicates or one per output stream.");
    }
    m_predicates = &predicates;
    m_predicateDeletes = &predicateDeletes;
    for_each(begin(), end(), [partitionId](TupleOutputStream& strm) { strm.startRows(partitionId); });
}

/** Stop serializing. */
void TupleOutputStreamProcessor::close() {
    for_each(begin(), end(), [](TupleOutputStream& strm) { strm.endRows(); });
    m_maxTupleLength = 0;
    m_predicates = nullptr;
    m_predicateDeletes = nullptr;
    m_opened = false;
}

/**
 * Write a tuple to the output streams.
 * Expects buffer space was already checked.
 * Returns true when the caller should yield to allow other work to proceed.
 */
bool TupleOutputStreamProcessor::writeRow(TableTuple &tuple, const HiddenColumnFilter &hiddenColumnFilter, bool *deleteRow) {
    if (! m_opened) {
        throwFatalException("TupleOutputStreamProcessor::writeRow() was called before open().");
    }

    // Predicates, if supplied, are one per output stream (previously asserted).
    vassert(m_predicates != nullptr);

    auto ipredicate = m_predicates->cbegin();
    auto iDeleteFlag = m_predicateDeletes->cbegin();

    bool yield = false;
    for (auto& iter : *this) {
        // Get approval from corresponding output stream predicate, if provided.
        bool accepted = true;
        if (ipredicate != m_predicates->cend()) {
            if (! boost::is_null(ipredicate)) {
                accepted = ipredicate->eval(&tuple).isTrue();
            }
            // Keep walking through predicates in lock-step with the streams.
            // As with first() we expect a predicate to be available for each and every stream.
            // It was already checked, so just assert here.
            vassert(ipredicate != m_predicates->cend());
            if (accepted && deleteRow != NULL) {
                *deleteRow = *deleteRow || *iDeleteFlag;
            }
            ++ipredicate;
            ++iDeleteFlag;
        }

        if (accepted) {
            if (! iter.canFit(m_maxTupleLength)) {
                throwFatalException(
                    "TupleOutputStreamProcessor::writeRow() failed because buffer has no space.");
            }
            iter.writeRow(tuple, hiddenColumnFilter);

            // Check if we'll need to yield after handling this row.
            if (!yield) {
                // Yield when the buffer is not capable of handling another tuple
                // or when the total bytes serialized threshold is exceeded.
                yield = ! iter.canFit(m_maxTupleLength) ||
                    iter.getTotalBytesSerialized() > BYTES_SERIALIZED_THRESHOLD;
            }
        }
    }
    return yield;
}

} // namespace voltdb
