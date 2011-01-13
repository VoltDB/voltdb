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

#ifndef ARRAYUNIQUEINDEX_H
#define ARRAYUNIQUEINDEX_H

#include "common/ids.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"

#include <vector>
#include <string>
#include <map>

namespace voltdb {

class Table;
class TableTuple;

const int ARRAY_INDEX_INITIAL_SIZE = 131072; // (2^17)

/**
 * Unique Index specialized for 1 integer.
 * This is implemented as a giant array, which gives optimal performance as far
 * as the entry value is assured to be sequential and limited to a small number.
 * @see TableIndex
 */
class ArrayUniqueIndex : public TableIndex {
    friend class TableIndexFactory;

    public:
        ~ArrayUniqueIndex();
        bool addEntry(const TableTuple *tuples);
        bool deleteEntry(const TableTuple *tuple);
        bool replaceEntry(const TableTuple *oldTupleValue, const TableTuple* newTupleValue);
        bool replaceEntryNoKeyChange(const TableTuple *oldTupleValue,
                                  const TableTuple *newTupleValue);
        bool exists(const TableTuple* values);
        bool moveToKey(const TableTuple *searchKey);
        bool moveToTuple(const TableTuple *searchTuple);
        TableTuple nextValueAtKey();
        bool advanceToNextKey();

        bool checkForIndexChange(const TableTuple *lhs, const TableTuple *rhs);

        size_t getSize() const { return 0; }
        int64_t getMemoryEstimate() const
        {
            return ARRAY_INDEX_INITIAL_SIZE * sizeof(void*);
        }
        std::string getTypeName() const { return "ArrayIntUniqueIndex"; };

    protected:
        ArrayUniqueIndex(const TableIndexScheme &scheme);

        void **entries_;
        int32_t allocated_entries_;
        int32_t match_i_;
};

}

#endif // ARRAYUNIQUEINDEX_H
