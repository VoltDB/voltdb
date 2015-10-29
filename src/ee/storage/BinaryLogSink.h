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
#include <boost/shared_ptr.hpp>

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
    int64_t apply(const char* taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine);
private:
    void validateChecksum(uint32_t expected, const char *start, const char *end);

    /**
     * Handle insert constraint violation
     */
    bool handleConflict(VoltDBEngine* engine, PersistentTable* drTable, Pool *pool, TableTuple* existingTuple, const TableTuple* expectedTuple, TableTuple* newTuple, int64_t uniqueId,
            DRRecordType actionType, DRConflictType deleteConflict, DRConflictType insertConflict);

    /**
     * Export the conflict log to downstream
     */
    void exportDRConflict(Table *exportTable, bool diverge, TempTable *existingTable, TempTable *expectedTable, TempTable *newTable, TempTable *outputTable);
};


}
#endif

