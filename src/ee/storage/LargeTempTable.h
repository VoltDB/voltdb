/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#include "common/LargeTempTableBlockCache.h"
#include "storage/table.h"
#include "storage/tableiterator.h"

namespace voltdb {

class LargeTempTableIterator;
class LargeTempTableBlock;

/** A large temp table class that uses LargeTempTableCache to request
    tuple blocks, allowing some blocks to be stored on disk.  */
class LargeTempTable : public Table {

    friend class TableFactory;

public:

    TableIterator& iterator() {
        return m_iter;
    }

    LargeTempTableIterator largeIterator();

    TableIterator& iteratorDeletingAsWeGo() {
        return m_iter;
    }

    void deleteAllTuples(bool freeAllocatedStrings, bool fallible) {
        return;
    }

    bool insertTuple(TableTuple& tuple);

    /** To unpin the last written block when all inserts are
        complete. */
    void finishInserts();

    size_t allocatedBlockCount() const {
        return m_blockIds.size();
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

    int64_t numTuples() const {
        return m_numTuples;
    }

    virtual ~LargeTempTable();

protected:

    LargeTempTable();

private:

    bool m_insertsFinished;

    TableIterator m_iter;

    LargeTempTableBlock* m_blockForWriting;
    std::vector<int64_t> m_blockIds;

    int64_t m_numTuples; // redundant with base class?? xxx
};

}

#endif // VOLTDB_LARGETEMPTABLE_H
