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

#ifndef STREAMEDTABLE_H
#define STREAMEDTABLE_H

#include <vector>

#include "common/ids.h"

#include "storage/viewableandreplicabletable.h"
#include "storage/StreamedTableStats.h"
#include "storage/TableStats.h"

namespace catalog {
class MaterializedViewInfo;
}

namespace voltdb {
class ExecutorContext;
class ExportTupleStream;
class MaterializedViewTriggerForStreamInsert;

class MigrateTxnSizeGuard {
public:
    MigrateTxnSizeGuard()
        :undoToken(0L),uso(0L),estimatedDRLogSize(0) {}
    inline void reset() {
        undoToken = 0L;
        uso = 0L;
        estimatedDRLogSize = 0;
    }
    int64_t undoToken;
    int64_t uso;
    int32_t estimatedDRLogSize;

};

/**
 * A streamed table does not store data. It may not be read. It may
 * not be updated. Only new appended writes are permitted. All writes
 * are passed through a ExportTupleStream to Export. The table exists
 * only to support Export.
 */
class StreamedTable : public ViewableAndReplicableTable<MaterializedViewTriggerForStreamInsert>, public UndoQuantumReleaseInterest {
    friend class TableFactory;
    friend class StreamedTableStats;

public:
    StreamedTable(int partitionColumn, bool isReplicated);
    //Used for test
    StreamedTable(ExportTupleStream *wrapper, int partitionColumn = -1, bool isReplicated = true);
    static StreamedTable* createForTest(size_t, ExecutorContext*, TupleSchema *schema,
            std::string tableName, std::vector<std::string> & columnNames);

    virtual ~StreamedTable();

    void notifyQuantumRelease();

    // virtual Table functions
    // Return a table iterator BY VALUE
    TableIterator iterator();

    TableIterator iteratorDeletingAsWeGo();

    // ------------------------------------------------------------------
    // GENERIC TABLE OPERATIONS
    // ------------------------------------------------------------------
    virtual void deleteAllTuples();
    void streamTuple(TableTuple &source, ExportTupleStream::STREAM_ROW_TYPE type, AbstractDRTupleStream *drStream = NULL);
    virtual bool insertTuple(TableTuple &tuple);

    virtual void loadTuplesFrom(SerializeInputBE &serialize_in, Pool *stringPool = NULL);
    virtual void flushOldTuples(int64_t timeInMillis);
    void setGeneration(int64_t generation);

    // The MatViewType typedef is required to satisfy initMaterializedViews
    // template code that needs to identify
    // "whatever MaterializedView*Trigger class is used by this *Table class".
    // There's no reason to actually use MatViewType in the class definition.
    // That would just make the code a little harder to analyze.
    typedef MaterializedViewTriggerForStreamInsert MatViewType;

    virtual std::string tableType() const { return "StreamedTable"; }

    // undo interface particular to streamed table.
    void undo(size_t mark, int64_t seqNo);

    //Override and say how many bytes are in Java and C++
    int64_t allocatedTupleMemory() const;


    /**
     * Get the current offset in bytes of the export stream for this Table
     * since startup.
     */
    void getExportStreamPositions(int64_t &seqNo, size_t &streamBytesUsed, int64_t &genId);

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed, int64_t generationIdCreated);

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
        if (m_wrapper) {
            m_sequenceNo = m_wrapper->getSequenceNumber() - 1;
        }
    }

    /**
     * Move the ExportTupleStream wrapper from this streamed table to other. Setting this wrapper to null
     */
    void moveWrapperTo(StreamedTable *other) {
        other->setWrapper(m_wrapper);
        m_wrapper = nullptr;
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
    ExportTupleStream *m_wrapper;
    int64_t m_sequenceNo;

    // Used to prevent migrate transaction from generating >50MB DR binary log
    MigrateTxnSizeGuard m_migrateTxnSizeGuard;
};

}
#endif
