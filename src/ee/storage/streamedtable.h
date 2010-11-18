/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef STREAMEDTABLE_H
#define STREAMEDTABLE_H

#include "common/ids.h"
#include "table.h"
#include "storage/StreamedTableStats.h"
#include "storage/TableStats.h"

namespace voltdb {

// forward decl.
class Topend;
class ExecutorContext;
class TupleStreamWrapper;

/**
 * A streamed table does not store data. It may not be read. It may
 * not be updated. Only new appended writes are permitted. All writes
 * are passed through a TupleStreamWrapper to Export. The table exists
 * only to support Export.
 */

class StreamedTable : public Table {
    friend class TableFactory;
    friend class StreamedTableStats;

  public:
    StreamedTable(ExecutorContext *ctx, bool exportEnabled);
    StreamedTable(int tableAllocationTargetSize);
    static StreamedTable* createForTest(size_t, ExecutorContext*);

    virtual ~StreamedTable();

    // virtual Table functions
    // Return a table iterator BY VALUE
    virtual TableIterator& iterator();
    virtual TableIterator* makeIterator();

    virtual void deleteAllTuples(bool freeAllocatedStrings);
    virtual bool insertTuple(TableTuple &source);
    virtual bool updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes);
    virtual bool deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings);
    virtual void loadTuplesFrom(bool allowExport, SerializeInput &serialize_in, Pool *stringPool = NULL);
    virtual void flushOldTuples(int64_t timeInMillis);
    virtual StreamBlock* getCommittedExportBytes();
    virtual bool releaseExportBytes(int64_t releaseOffset);
    virtual void resetPollMarker();

    virtual std::string tableType() const {
        return "StreamedTable";
    }

    // undo interface particular to streamed table.
    void undo(size_t mark);

  protected:
    // Stats
    voltdb::StreamedTableStats stats_;
    voltdb::TableStats *getTableStats();

    TBPtr allocateNextBlock();
    void nextFreeTuple(TableTuple *tuple);

  private:
    ExecutorContext *m_executorContext;
    TupleStreamWrapper *m_wrapper;
    int64_t m_sequenceNo;
};

}
#endif
