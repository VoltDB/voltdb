/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

namespace catalog {
class MaterializedViewInfo;
}

namespace voltdb {
class ExecutorContext;
class ExportTupleStream;
class MaterializedViewTriggerForStreamInsert;

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
    StreamedTable(int partitionColumn = -1);
    //Used for test
    StreamedTable(ExportTupleStream *wrapper, int partitionColumn = -1);
    static StreamedTable* createForTest(size_t, ExecutorContext*, TupleSchema *schema, std::vector<std::string> & columnNames);

    virtual ~StreamedTable();

    // virtual Table functions
    // Return a table iterator BY VALUE
    TableIterator iterator();

    TableIterator iteratorDeletingAsWeGo();

    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples(bool freeAllocatedStrings, bool=true);
    // TODO: change meaningless bool return type to void (starting in class Table) and migrate callers.
    virtual bool insertTuple(TableTuple &tuple);

    virtual void loadTuplesFrom(SerializeInputBE &serialize_in, Pool *stringPool = NULL);
    virtual void flushOldTuples(int64_t timeInMillis);
    void setSignatureAndGeneration(std::string signature, int64_t generation);

    // The MatViewType typedef is required to satisfy initMaterializedViews
    // template code that needs to identify
    // "whatever MaterializedView*Trigger class is used by this *Table class".
    // There's no reason to actually use MatViewType in the class definition.
    // That would just make the code a little harder to analyze.
    typedef MaterializedViewTriggerForStreamInsert MatViewType;

    /** Add/drop/list materialized views to this table */
    void addMaterializedView(MaterializedViewTriggerForStreamInsert* view);
    void dropMaterializedView(MaterializedViewTriggerForStreamInsert* targetView);
    std::vector<MaterializedViewTriggerForStreamInsert*>& views() { return m_views; }
    bool hasViews() { return (m_views.size() > 0); }

    virtual std::string tableType() const { return "StreamedTable"; }

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

    int partitionColumn() const { return m_partitionColumn; }

    /*
     * For an export table return the sequence number
     */
    virtual int64_t activeTupleCount() const {
        return m_sequenceNo;
    }

    // STATS
    TableStats* getTableStats() {  return &m_stats; };

    // No Op
    std::vector<uint64_t> getBlockAddresses() const {
        return std::vector<uint64_t>();
    }

    void setWrapper(ExportTupleStream *wrapper) {
        m_wrapper = wrapper;
    }

    ExportTupleStream* getWrapper() {
        return m_wrapper;
    }

private:
    // Just say 0
    size_t allocatedBlockCount() const;

    TBPtr allocateNextBlock();
    virtual void nextFreeTuple(TableTuple *tuple);

    voltdb::StreamedTableStats m_stats;
    ExecutorContext *m_executorContext;
    ExportTupleStream *m_wrapper;
    int64_t m_sequenceNo;

    // partition key
    const int m_partitionColumn;

    // list of materialized views that are sourced from this table
    std::vector<MaterializedViewTriggerForStreamInsert*> m_views;
};

}
#endif
