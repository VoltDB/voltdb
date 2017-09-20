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
#include "storage/AbstractTempTable.hpp"
#include "storage/tableiterator.h"

namespace voltdb {

class LargeTempTableIterator;
class LargeTempTableBlock;

/** A large temp table class that uses LargeTempTableCache to request
    tuple blocks, allowing some blocks to be stored on disk.  */
class LargeTempTable : public AbstractTempTable {

    friend class TableFactory;

public:

    TableIterator iterator() {
        m_iter.reset(m_blockIds.begin());
        return m_iter;
    }

    TableIterator iteratorDeletingAsWeGo() {
        m_iter.reset(m_blockIds.begin());
        m_iter.setTempTableDeleteAsGo(true);
        return m_iter;
    }

    void deleteAllTuples(bool freeAllocatedStrings, bool fallible) {
        return deleteAllTempTuples();
    }

    bool insertTuple(TableTuple& tuple);

    virtual void insertTempTuple(TableTuple &source) {
        insertTuple(source);
    }

    /** To unpin the last written block when all inserts are
        complete. */
    virtual void finishInserts();

    virtual void deleteAllTempTuples();

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

    void nextFreeTuple(TableTuple* tuple);

    virtual const TempTableLimits* getTempTableLimits() const {
        return NULL;
    }

    virtual ~LargeTempTable();

protected:

    LargeTempTable();

private:

    std::vector<int64_t> m_blockIds;

    bool m_insertsFinished;

    TableIterator m_iter;

    LargeTempTableBlock* m_blockForWriting;
};

}

#endif // VOLTDB_LARGETEMPTABLE_H
