/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#ifndef VOLTDB_LARGETEMPTABLE_H
#define VOLTDB_LARGETEMPTABLE_H

#include "storage/table.h"
#include "storage/tableiterator.h"

namespace voltdb {

class LargeTempTable : public Table {

    friend class TableFactory;

public:

    TableIterator& iterator() {
        return m_iter;
    }

    TableIterator& iteratorDeletingAsWeGo() {
        return m_iter;
    }

    void deleteAllTuples(bool freeAllocatedStrings, bool fallible) {
        return;
    }

    bool insertTuple(TableTuple& tuple) {
        return true;
    }

    size_t allocatedBlockCount() const {
        return 0;
    }

    std::string tableType() const {
        return "LargeTempTable";
    }

    std::vector<uint64_t> getBlockAddresses() const {
        return std::vector<uint64_t>();
    }

    voltdb::TableStats* getTableStats() {
        return NULL;
    }

    void nextFreeTuple(TableTuple* tuple) {

    }

protected:

    LargeTempTable()
        : Table(BLOCKSIZE)
        , m_iter(this)
    {
    }

private:

    static const int BLOCKSIZE = 131072;

    TableIterator m_iter;
};

}

#endif // VOLTDB_LARGETEMPTABLE_H
