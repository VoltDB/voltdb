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
#ifndef BINARYLOGSINK_H
#define BINARYLOGSINK_H
#include <boost/unordered_map.hpp>

namespace voltdb {

class PersistentTable;
class Pool;
class VoltDBEngine;
class Table;
class TempTable;
class TableTuple;

/*
 * Responsible for applying binary logs to table data
 */
class BinaryLogSink {
public:
    BinaryLogSink();
    int64_t apply(const char* taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine, bool isActiveActiveDREnabled = false);
    void exportDRConflict(PersistentTable* srcTable, Table* dstTable, const DRRecordType &type, TableTuple &tuple);
private:
    void validateChecksum(uint32_t expected, const char *start, const char *end);

    /**
     * Find all rows in a @table that conflict with the @searchTuple (unique key violation) except the @expectedTuple
     * All conflicting rows are put into @conflictRows.
     */
    void findConflictTuple(Table* table, TableTuple* searchTuple, TableTuple* expectedTuple, std::vector<TableTuple *>& conflictRows);

    /**
     * Report the DR conflict to frontend through conflict resolution API
     */
    DRResolutionType reportDRConflict(PersistentTable* table, int64_t partitionId, int64_t sequenceNumber, DRConflictType conflictType,
            DRRecordType recordType, Table* existingRow, Table* expectedRow, Table* newRow, Table* outputRow);

    /**
     * Handle insert constraint violation
     */
    bool handleConflict(VoltDBEngine* engine, PersistentTable* drTable, Pool *pool, TableTuple* conflictTuple, TableTuple* missingTuple, TableTuple* newTuple, int64_t uniqueId,
            int64_t sequenceNumber, DRRecordType actionType, DRConflictType conflictType);

    /**
     * create conflict export tuple from the conflict tuple
     */
    void createConflictExportTuple(TempTable *outputTable, PersistentTable *drTable, Pool *pool,
            TableTuple *tupleToBeWrote, DRRecordType actionType, DRConflictType conflictType, DRConflictRowType reportType);

    /**
     * Divide update related conflict types into fine singularity
     */
    DRConflictType optimizeUpdateConflictType(PersistentTable* drTable, TableTuple* expectedTuple, TableTuple* newTuple, std::vector<TableTuple*> &existingRows, DRConflictType conflictType);
};


}
#endif

