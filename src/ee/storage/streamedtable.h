/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef STREAMEDTABLE_H
#define STREAMEDTABLE_H

#include <vector>

#include "common/ids.h"
#include "table.h"
#include "storage/StreamedTableStats.h"
#include "storage/TableStats.h"

namespace voltdb {

// forward decl.
class Topend;
class ExecutorContext;
class ExportTupleStream;

/**
 * A streamed table does not store data. It may not be read. It may
 * not be updated. Only new appended writes are permitted. All writes
 * are passed through a ExportTupleStream to Export. The table exists
 * only to support Export.
 */

class StreamedTable : public Table {
    friend class TableFactory;
    friend class StreamedTableStats;

  public:
    StreamedTable(bool exportEnabled);
    static StreamedTable* createForTest(size_t, ExecutorContext*);

    //This returns true if a stream was created thus caller can setSignatureAndGeneration to push.
    bool enableStream();

    virtual ~StreamedTable();

    // virtual Table functions
    // Return a table iterator BY VALUE
    virtual TableIterator& iterator();
    virtual TableIterator* makeIterator();

    virtual TableIterator& iteratorDeletingAsWeGo() {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "May not iterate a streamed table.");
    }

    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples(bool freeAllocatedStrings);
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    // The bool argument is irrelevent to StreamedTable.
    virtual bool deleteTuple(TableTuple &tuple, bool=true);
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool insertTuple(TableTuple &tuple);
    // Updating streamed tuples is not supported
    // Update is irrelevent to StreamedTable.
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool updateTupleWithSpecificIndexes(TableTuple &targetTupleToUpdate,
                                                TableTuple &sourceTupleWithNewValues,
                                                std::vector<TableIndex*> const &indexesToUpdate,
                                                bool=true);


    virtual void loadTuplesFrom(SerializeInputBE &serialize_in, Pool *stringPool = NULL);
    virtual void flushOldTuples(int64_t timeInMillis);
    virtual void setSignatureAndGeneration(std::string signature, int64_t generation);

    virtual std::string tableType() const {
        return "StreamedTable";
    }

    // undo interface particular to streamed table.
    void undo(size_t mark);

    //Override and say how many bytes are in Java and C++
    int64_t allocatedTupleMemory() const;


    /**
     * Get the current offset in bytes of the export stream for this Table
     * since startup.
     */
    void getExportStreamPositions(int64_t &seqNo, size_t &streamBytesUsed);

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed);

    virtual bool isExport() {
        return true;
    }

    /*
     * For an export table return the sequence number
     */
    virtual int64_t activeTupleCount() const {
        return m_sequenceNo;
    }

private:
    // Stats
    voltdb::TableStats *getTableStats();

    // Just say 0
    size_t allocatedBlockCount() const;

    TBPtr allocateNextBlock();
    virtual void nextFreeTuple(TableTuple *tuple);

    voltdb::StreamedTableStats stats_;
    ExecutorContext *m_executorContext;
    ExportTupleStream *m_wrapper;
    int64_t m_sequenceNo;
};

}
#endif
