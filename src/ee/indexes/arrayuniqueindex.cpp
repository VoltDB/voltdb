/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "indexes/arrayuniqueindex.h"
#include "common/tabletuple.h"
#include "common/debuglog.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"

#include <sstream>
#include <map>
#include <cstdlib>

namespace voltdb {


ArrayUniqueIndex::ArrayUniqueIndex(const TableIndexScheme &scheme) : TableIndex(scheme) {
    assert(colCount_ == 1);
    assert((column_types_[0] == VALUE_TYPE_TINYINT) || (column_types_[0] == VALUE_TYPE_SMALLINT) ||
        (column_types_[0] == VALUE_TYPE_INTEGER) || (column_types_[0] == VALUE_TYPE_BIGINT));
    entries_ = new void*[ARRAY_INDEX_INITIAL_SIZE];
    ::memset(entries_, 0, sizeof(void*) * ARRAY_INDEX_INITIAL_SIZE);
    allocated_entries_ = ARRAY_INDEX_INITIAL_SIZE;
}

ArrayUniqueIndex::~ArrayUniqueIndex() {
    delete[] entries_;
}

bool ArrayUniqueIndex::addEntry(const TableTuple *tuple) {
    const int32_t key = ValuePeeker::peekAsInteger(tuple->getNValue(column_indices_[0]));
    //VOLT_TRACE ("Adding entry %ld from column index %d", key, column_indices_[0]);
    assert((key < ARRAY_INDEX_INITIAL_SIZE) && (key >= 0));

    // uniqueness check
    if (entries_[key] != NULL)
        return false;
    entries_[key] = static_cast<void*>(const_cast<TableTuple*>(tuple)->address());
    ++m_inserts;
    return true;
}

bool ArrayUniqueIndex::deleteEntry(const TableTuple *tuple) {
    const int32_t key = ValuePeeker::peekAsInteger(tuple->getNValue(column_indices_[0]));
    assert((key < ARRAY_INDEX_INITIAL_SIZE) && (key >= 0));

    //VOLT_DEBUG("Deleting entry %lld", key);
    entries_[key] = NULL;
    ++m_deletes;
    return true; //deleted
}

bool ArrayUniqueIndex::replaceEntry(const TableTuple *oldTupleValue, const TableTuple* newTupleValue) {
    // this can probably be optimized
    int32_t old_key = ValuePeeker::peekAsInteger(oldTupleValue->getNValue(column_indices_[0]));
    int32_t new_key = ValuePeeker::peekAsInteger(newTupleValue->getNValue(column_indices_[0]));
    assert((old_key < ARRAY_INDEX_INITIAL_SIZE) && (old_key >= 0));
    assert((new_key < ARRAY_INDEX_INITIAL_SIZE) && (new_key >= 0));
    if (old_key == new_key) return true; // no update is needed for this index

    entries_[new_key] = const_cast<TableTuple*>(newTupleValue)->address();
    entries_[old_key] = NULL;
    ++m_updates;
    return true;
}

/**
 * Update in place an index entry with a new tuple address
 */
bool ArrayUniqueIndex::replaceEntryNoKeyChange(const TableTuple *oldTupleValue,
                          const TableTuple *newTupleValue) {
    assert(oldTupleValue->address() != newTupleValue->address());
    int32_t old_key = ValuePeeker::peekAsInteger(oldTupleValue->getNValue(column_indices_[0]));
    entries_[old_key] = newTupleValue->address();
    m_updates++;
    return true;
}

bool ArrayUniqueIndex::exists(const TableTuple* values) {
    int32_t key = ValuePeeker::peekAsInteger(values->getNValue(column_indices_[0]));
    //VOLT_DEBUG("Exists?: %lld", key);
    assert(key < ARRAY_INDEX_INITIAL_SIZE);
    assert(key >= 0);
    if (key >= allocated_entries_) return false;
    VOLT_TRACE("Checking entry b: %d", (int)key);
    ++m_lookups;
    return entries_[key] != NULL;
}

bool ArrayUniqueIndex::moveToKey(const TableTuple *searchKey) {
    match_i_ = ValuePeeker::peekAsInteger(searchKey->getNValue(0));
    if (match_i_ < 0) return false;
    assert(match_i_ < ARRAY_INDEX_INITIAL_SIZE);
    ++m_lookups;
    return entries_[match_i_];
}

bool ArrayUniqueIndex::moveToTuple(const TableTuple *searchTuple) {
    match_i_ = ValuePeeker::peekAsInteger(searchTuple->getNValue(0));
    if (match_i_ < 0) return false;
    assert(match_i_ < ARRAY_INDEX_INITIAL_SIZE);
    ++m_lookups;
    return entries_[match_i_];
}

TableTuple ArrayUniqueIndex::nextValueAtKey() {
    if (match_i_ == -1) return TableTuple();
    if (!(entries_[match_i_])) return TableTuple();
    TableTuple retval(m_tupleSchema);
    retval.move(entries_[match_i_]);
    match_i_ = -1;
    return retval;
}

bool ArrayUniqueIndex::advanceToNextKey() {
    assert((match_i_ < ARRAY_INDEX_INITIAL_SIZE) && (match_i_ >= 0));
    return entries_[++match_i_];
}

bool ArrayUniqueIndex::checkForIndexChange(const TableTuple *lhs, const TableTuple *rhs) {
    return lhs->getNValue(column_indices_[0]).op_notEquals(rhs->getNValue(column_indices_[0])).isTrue();
}

}
