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

#ifndef BINARYLOGSINK_H_
#define BINARYLOGSINK_H_

#include <string>
#include "common/Pool.hpp"
#include "common/serializeio.h"
#include "common/tabletuple.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "common/UniqueId.hpp"
#include "storage/BinaryLogSink.h"
#include "storage/persistenttable.h"
#include "storage/ConstraintFailureException.h"
#include "storage/tablefactory.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "indexes/tableindex.h"

#include "catalog/database.h"

#include<boost/unordered_map.hpp>
#include<boost/unordered_set.hpp>
#include<crc/crc32c.h>

namespace voltdb {

const static std::string EXISTING_TABLE = "existing_table";
const static std::string EXPECTED_TABLE = "expected_table";
const static std::string NEW_TABLE = "new_table";
const static std::string OUTPUT_TABLE = "output_table";

class CachedIndexKeyTuple {
public:
    CachedIndexKeyTuple() : m_tuple(), m_cachedIndexCrc(0), m_storageSize(0), m_tupleStorage() {}

    TableTuple &tuple(PersistentTable *table, uint32_t indexCrc) {
        if (m_storageSize > 0 && indexCrc == m_cachedIndexCrc) {
            return m_tuple;
        }
        std::pair<const TableIndex*, uint32_t> index = table->getUniqueIndexForDR();
        if (!index.first || indexCrc != index.second) {
            throwSerializableEEException("Unable to find unique index %u while applying a binary log record",
                                         indexCrc);
        }
        const TupleSchema* schema = index.first->getKeySchema();
        size_t tupleLength = schema->tupleLength() + TUPLE_HEADER_SIZE;
        if (tupleLength > m_storageSize) {
            m_tupleStorage.reset(new char[tupleLength]);
            m_storageSize = tupleLength;
        }
        m_tuple.setSchema(schema);
        m_tuple.move(m_tupleStorage.get());
        m_cachedIndexCrc = index.second;
        return m_tuple;
    }
private:
    TableTuple m_tuple;
    uint32_t m_cachedIndexCrc;
    size_t m_storageSize;
    boost::scoped_array<char> m_tupleStorage;
};

BinaryLogSink::BinaryLogSink() {}

int64_t BinaryLogSink::apply(const char *taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine, bool isActiveActiveDREnabled) {
    ReferenceSerializeInputLE taskInfo(taskParams + 4, ntohl(*reinterpret_cast<const int32_t*>(taskParams)));

    int64_t __attribute__ ((unused)) uniqueId = 0;
    int64_t __attribute__ ((unused)) sequenceNumber = -1;

    size_t rowCount = 0;
    CachedIndexKeyTuple indexKeyTuple;
    while (taskInfo.hasRemaining()) {
        pool->purge();
        const char* recordStart = taskInfo.getRawPointer();
        const uint8_t drVersion = taskInfo.readByte();
        if (drVersion > 2) {
            throwFatalException("Unsupported DR version %d", drVersion);
        }
        const DRRecordType type = static_cast<DRRecordType>(taskInfo.readByte());
        rowCount += rowCostForDRRecord(type);

        switch (type) {
        case DR_RECORD_INSERT: {
            int64_t tableHandle = taskInfo.readLong();
            int32_t rowLength = taskInfo.readInt();
            const char *rowData = reinterpret_cast<const char *>(taskInfo.getRawPointer(rowLength));
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwSerializableEEException("Unable to find table hash %jd while applying a binary log insert record",
                                             (intmax_t)tableHandle);
            }
            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = table->tempTuple();

            ReferenceSerializeInputLE rowInput(rowData, rowLength);
            tempTuple.deserializeFromDR(rowInput, pool);
            try {
                table->insertPersistentTuple(tempTuple, true);
            } catch (ConstraintFailureException &e) {
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    if (handleConflict(engine, table, pool, NULL, NULL, e.getConflictTuple(), uniqueId, sequenceNumber, DR_RECORD_INSERT, NO_CONFLICT, CONFLICT_CONSTRAINT_VIOLATION)) {
                        continue;
                    }
                }
                throw;
            }
            break;
        }
        case DR_RECORD_DELETE: {
            int64_t tableHandle = taskInfo.readLong();
            int32_t rowLength = taskInfo.readInt();
            const char *rowData = reinterpret_cast<const char *>(taskInfo.getRawPointer(rowLength));
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwSerializableEEException("Unable to find table hash %jd while applying a binary log delete record",
                                             (intmax_t)tableHandle);
            }
            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = table->tempTuple();

            ReferenceSerializeInputLE rowInput(rowData, rowLength);
            tempTuple.deserializeFromDR(rowInput, pool);

            TableTuple deleteTuple = table->lookupTupleByValues(tempTuple);
            if (deleteTuple.isNullTuple()) {
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    if (handleConflict(engine, table, pool, NULL, &tempTuple, NULL, uniqueId, sequenceNumber, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISSING, NO_CONFLICT)) {
                        continue;
                    }
                }
                throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                                 type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
            }

            // we still run in risk of having timestamp mismatch, need to check.
            if (isActiveActiveDREnabled && table->isDREnabled()) {
                NValue localHiddenColumn = deleteTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
                int64_t localTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(localHiddenColumn);
                NValue remoteHiddenColumn = tempTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
                int64_t remoteTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(remoteHiddenColumn);
                if (localTimestamp != remoteTimestamp) {
                    // timestamp mismatch conflict
                    if (handleConflict(engine, table, pool, &deleteTuple, &tempTuple, NULL, uniqueId, sequenceNumber, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISMATCH, NO_CONFLICT)) {
                        continue;
                    }
                }
            }

            table->deleteTuple(deleteTuple, true);
            break;
        }
        case DR_RECORD_UPDATE: {
            int64_t tableHandle = taskInfo.readLong();
            int32_t oldRowLength = taskInfo.readInt();
            const char *oldRowData = reinterpret_cast<const char*>(taskInfo.getRawPointer(oldRowLength));
            int32_t newRowLength = taskInfo.readInt();
            const char *newRowData = reinterpret_cast<const char*>(taskInfo.getRawPointer(newRowLength));
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwSerializableEEException("Unable to find table hash %jd while applying a binary log update record",
                                             (intmax_t)tableHandle);
            }
            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = table->tempTuple();

            ReferenceSerializeInputLE oldRowInput(oldRowData, oldRowLength);
            tempTuple.deserializeFromDR(oldRowInput, pool);

            TableTuple oldTuple = table->lookupTupleByValues(tempTuple);
            if (oldTuple.isNullTuple()) {
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    // create tuple to accommodate new row
                    TableTuple oldTuple(table->schema());
                    boost::shared_array<char> data = boost::shared_array<char>(new char[tempTuple.tupleLength() + TUPLE_HEADER_SIZE]);
                    oldTuple.move(data.get());
                    oldTuple.copyForPersistentInsert(tempTuple, pool);
                    ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
                    tempTuple.deserializeFromDR(newRowInput, pool);
                    if (handleConflict(engine, table, pool, NULL, &oldTuple, &tempTuple, uniqueId, sequenceNumber, DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_MISSING, NO_CONFLICT)) {
                        continue;
                    }
                }
                throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                         type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
            }

            // Timestamp mismatch conflict
            if (isActiveActiveDREnabled && table->isDREnabled()) {
                NValue localHiddenColumn = oldTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
                int64_t localTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(localHiddenColumn);
                NValue remoteHiddenColumn = tempTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
                int64_t remoteTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(remoteHiddenColumn);
                if (localTimestamp != remoteTimestamp) {
                    // create tuple to accommodate new row
                    TableTuple expectedTuple(table->schema());
                    boost::shared_array<char> data = boost::shared_array<char>(new char[tempTuple.tupleLength()]);
                    expectedTuple.move(data.get());
                    expectedTuple.copyForPersistentInsert(tempTuple, pool);
                    ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
                    tempTuple.deserializeFromDR(newRowInput, pool);
                    if (handleConflict(engine, table, pool, &oldTuple, &expectedTuple, &tempTuple, uniqueId, sequenceNumber, DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_MISMATCH, NO_CONFLICT)) {
                        continue;
                    }
                }
            }

            ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
            tempTuple.deserializeFromDR(newRowInput, pool);

            try {
                table->updateTupleWithSpecificIndexes(oldTuple, tempTuple, table->allIndexes());
            } catch (ConstraintFailureException &e) {
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    if (handleConflict(engine, table, pool, NULL, e.getOriginalTuple(), e.getConflictTuple(), uniqueId, sequenceNumber, DR_RECORD_UPDATE, NO_CONFLICT, CONFLICT_CONSTRAINT_VIOLATION)) {
                        continue;
                    }
                }
                throw;
            }
            break;
        }
        case DR_RECORD_DELETE_BY_INDEX: {
            int64_t tableHandle = taskInfo.readLong();
            int32_t rowKeyLength = taskInfo.readInt();
            uint32_t indexCrc = taskInfo.readInt();
            const char *rowKeyData = reinterpret_cast<const char *>(taskInfo.getRawPointer(rowKeyLength));
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwSerializableEEException("Unable to find table hash %jd while applying a binary log delete record",
                                             (intmax_t)tableHandle);
            }
            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = indexKeyTuple.tuple(table, indexCrc);

            ReferenceSerializeInputLE rowInput(rowKeyData, rowKeyLength);
            tempTuple.deserializeFromDR(rowInput, pool);

            const TableIndex* index = table->getUniqueIndexForDR().first;
            IndexCursor indexCursor(index->getTupleSchema());
            index->moveToKey(&tempTuple, indexCursor);
            TableTuple deleteTuple = index->nextValueAtKey(indexCursor);
            if (deleteTuple.isNullTuple()) {
                throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
            }

            table->deleteTuple(deleteTuple, true);
            break;
        }
        case DR_RECORD_UPDATE_BY_INDEX: {
            int64_t tableHandle = taskInfo.readLong();
            int32_t oldRowKeyLength = taskInfo.readInt();
            uint32_t oldKeyIndexCrc = taskInfo.readInt();
            const char *oldRowKeyData = reinterpret_cast<const char*>(taskInfo.getRawPointer(oldRowKeyLength));
            int32_t newRowLength = taskInfo.readInt();
            const char *newRowData = reinterpret_cast<const char*>(taskInfo.getRawPointer(newRowLength));
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwSerializableEEException("Unable to find table hash %jd while applying a binary log update record",
                                             (intmax_t)tableHandle);
            }
            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = indexKeyTuple.tuple(table, oldKeyIndexCrc);

            ReferenceSerializeInputLE oldRowInput(oldRowKeyData, oldRowKeyLength);
            tempTuple.deserializeFromDR(oldRowInput, pool);

            const TableIndex* index = table->getUniqueIndexForDR().first;
            IndexCursor indexCursor(index->getTupleSchema());
            index->moveToKey(&tempTuple, indexCursor);
            TableTuple oldTuple = index->nextValueAtKey(indexCursor);
            if (oldTuple.isNullTuple()) {
                throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                            type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
            }

            tempTuple = table->tempTuple();
            ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
            tempTuple.deserializeFromDR(newRowInput, pool);

            table->updateTupleWithSpecificIndexes(oldTuple, tempTuple, table->allIndexes());
            break;
        }
        case DR_RECORD_BEGIN_TXN: {
            uniqueId = taskInfo.readLong();
            int64_t tempSequenceNumber = taskInfo.readLong();
            if (sequenceNumber >= 0) {
                if (tempSequenceNumber < sequenceNumber) {
                    throwFatalException("Found out of order sequencing inside a binary log segment. Expected %jd but found %jd",
                                        (intmax_t)(sequenceNumber + 1), (intmax_t)tempSequenceNumber);
                } else if (tempSequenceNumber == sequenceNumber) {
                    throwFatalException("Found duplicate transaction %jd in a binary log segment",
                                        (intmax_t)tempSequenceNumber);
                } else if (tempSequenceNumber > sequenceNumber + 1) {
                    throwFatalException("Found sequencing gap inside a binary log segment. Expected %jd but found %jd",
                                        (intmax_t)(sequenceNumber + 1), (intmax_t)tempSequenceNumber);
                }
            }
            sequenceNumber = tempSequenceNumber;
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());
            break;
        }
        case DR_RECORD_END_TXN: {
            int64_t tempSequenceNumber = taskInfo.readLong();
            if (tempSequenceNumber != sequenceNumber) {
                throwFatalException("Closing the wrong transaction inside a binary log segment. Expected %jd but found %jd",
                                    (intmax_t)sequenceNumber, (intmax_t)tempSequenceNumber);
            }
            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());
            break;
        }
        case DR_RECORD_TRUNCATE_TABLE: {
            int64_t tableHandle = taskInfo.readLong();
            std::string tableName = taskInfo.readTextString();

            uint32_t checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwSerializableEEException("Unable to find table %s hash %jd while applying binary log for truncate record",
                                             tableName.c_str(), (intmax_t)tableHandle);
            }

            PersistentTable *table = tableIter->second;

            table->truncateTable(engine, true);

            break;
        }
        default:
            throwFatalException("Unrecognized DR record type %d", type);
            break;
        }
    }
    return static_cast<int64_t>(rowCount);
}

/**
   * Find all rows in a @table that conflict with the @searchTuple (unique key violation) except the @expectedTuple
   * All conflicting rows are put into @conflictRows.
   */
static void findConflictTuple(Table *table, TableTuple *searchTuple, TableTuple *expectedTuple, std::vector<boost::shared_ptr<TableTuple> > &conflictRows) {
    boost::unordered_set<char*> redundancyFilter;
    BOOST_FOREACH(TableIndex* index, table->allIndexes()) {
        if (index->isUniqueIndex()) {
            TableTuple conflictTuple;
            if (index->exists(searchTuple, &conflictTuple)) {
                if (expectedTuple && expectedTuple->equals(conflictTuple)) {
                    continue; // skip the expected tuple
                } else if (searchTuple->equals(conflictTuple)) {
                    continue; // skip the search tuple
                } else if (redundancyFilter.find(conflictTuple.address()) != redundancyFilter.end()) {
                    continue; // skip conflict tuples already found
                } else {
                    conflictRows.push_back(boost::shared_ptr<TableTuple>(new TableTuple(conflictTuple)));
                    redundancyFilter.insert(conflictTuple.address());
                }
            }
        }
    }
}

/**
 * create conflict export tuple from the conflict tuple
 */
static void createConflictExportTuple(TempTable *outputTable, PersistentTable *drTable, Pool *pool, TableTuple *tupleToBeWrote, DRRecordType actionType, DRConflictType conflictType, DRConflictRowType rowType) {
    TableTuple tempTuple = outputTable->tempTuple();
    NValue hiddenValue = tupleToBeWrote->getHiddenNValue(drTable->getDRTimestampColumnIndex());
    tempTuple.setNValue(0, ValueFactory::getTinyIntValue(rowType));
    tempTuple.setNValue(1, ValueFactory::getTinyIntValue(actionType));
    tempTuple.setNValue(2, ValueFactory::getTinyIntValue(conflictType));
    switch (rowType) {
    case EXISTING_ROW:
    case EXPECTED_ROW:
        tempTuple.setNValue(3, ValueFactory::getTinyIntValue(KEEP_ROW));   // decision
        break;
    case NEW_ROW:
        tempTuple.setNValue(3, ValueFactory::getTinyIntValue(DELETE_ROW));     // decision
        break;
    default:
        break;
    }
    tempTuple.setNValue(4, ValueFactory::getTinyIntValue(NOT_DIVERGE));
    tempTuple.setNValue(5, ValueFactory::getTinyIntValue((ExecutorContext::getClusterIdFromHiddenNValue(hiddenValue))));    // clusterId
    tempTuple.setNValue(6, ValueFactory::getBigIntValue(ExecutorContext::getDRTimestampFromHiddenNValue(hiddenValue)));     // timestamp
    tempTuple.setNValues(7, *tupleToBeWrote, 0, tupleToBeWrote->sizeInValues());    // rest of columns, excludes the hidden column

    outputTable->insertTupleNonVirtualWithDeepCopy(tempTuple, pool);
}

// iterate all four tables and push them into export table
void BinaryLogSink::exportDRConflict(Table *exportTable, bool diverge, TempTable *existingTableForDelete, TempTable *expectedTableForDelete, TempTable *existingTableForInsert, TempTable *newTableInsert) {
    assert(exportTable != NULL);
    assert(exportTable->isExport());

    TableTuple tempTuple(exportTable->schema());
    if (existingTableForDelete) {
        TableIterator iterator = existingTableForDelete->iterator();
        while (iterator.next(tempTuple)) {
            if (diverge) {
                tempTuple.setNValue(4, ValueFactory::getTinyIntValue(DIVERGE));
            }
            exportTable->insertTuple(tempTuple);
        }
    }

    if (expectedTableForDelete) {
        TableIterator iterator = expectedTableForDelete->iterator();
        while (iterator.next(tempTuple)) {
            if (diverge) {
                tempTuple.setNValue(4, ValueFactory::getTinyIntValue(DIVERGE));
            }
            exportTable->insertTuple(tempTuple);
        }
    }

    if (existingTableForInsert) {
        TableIterator iterator = existingTableForInsert->iterator();
        while (iterator.next(tempTuple)) {
            if (diverge) {
                tempTuple.setNValue(4, ValueFactory::getTinyIntValue(DIVERGE));
            }
            exportTable->insertTuple(tempTuple);
        }
    }

    if (newTableInsert) {
        TableIterator iterator = newTableInsert->iterator();
        while (iterator.next(tempTuple)) {
            if (diverge) {
                tempTuple.setNValue(4, ValueFactory::getTinyIntValue(DIVERGE));
            }
            exportTable->insertTuple(tempTuple);
        }
    }
}

void BinaryLogSink::validateChecksum(uint32_t checksum, const char *start, const char *end) {
    uint32_t recalculatedCRC = vdbcrc::crc32cInit();
    recalculatedCRC = vdbcrc::crc32c( recalculatedCRC, start, (end - 4) - start);
    recalculatedCRC = vdbcrc::crc32cFinish(recalculatedCRC);

    if (recalculatedCRC != checksum) {
        throwFatalException("CRC mismatch of DR log data %d and %d", checksum, recalculatedCRC);
    }
}

bool BinaryLogSink::handleConflict(VoltDBEngine *engine, PersistentTable *drTable, Pool *pool, TableTuple *existingTuple, TableTuple *expectedTuple, TableTuple *newTuple,
        int64_t uniqueId, int64_t sequenceNumber, DRRecordType actionType, DRConflictType deleteConflict, DRConflictType insertConflict) {
    if (!engine) {
        return false;
    }
    Table* conflictExportTable = engine->getDRConflictTable(drTable);
    if (!conflictExportTable) {
        return false;
    }

    // add new row
    std::vector<boost::shared_ptr<TableTuple> > existingRows;
    if (newTuple) {
        // add any rows conflict with the new row
        findConflictTuple(drTable, newTuple, actionType == DR_RECORD_UPDATE ? expectedTuple : NULL, existingRows);
        if (actionType == DR_RECORD_UPDATE) {
            if (existingRows.size() > 0) {
                insertConflict = CONFLICT_CONSTRAINT_VIOLATION;
            }
        }
    }

    // construct delete conflict
    boost::shared_ptr<TempTable> existingTableForDelete;
    boost::shared_ptr<TempTable> expectedTableForDelete;
    if (deleteConflict != NO_CONFLICT) {
        existingTableForDelete.reset(TableFactory::getCopiedTempTable(0, EXISTING_TABLE, conflictExportTable, NULL));
        if (existingTuple) {
            createConflictExportTuple(existingTableForDelete.get(), drTable, pool, existingTuple, actionType, deleteConflict, EXISTING_ROW);
        }

        expectedTableForDelete.reset(TableFactory::getCopiedTempTable(0, EXPECTED_TABLE, conflictExportTable, NULL));
        if (expectedTuple) {
            createConflictExportTuple(expectedTableForDelete.get(), drTable, pool, expectedTuple, actionType, deleteConflict, EXPECTED_ROW);
        }
    }

    // construct insert conflict
    boost::shared_ptr<TempTable> existingTableForInsert;
    boost::shared_ptr<TempTable> newTableForInsert;
    if (insertConflict != NO_CONFLICT) {
        existingTableForInsert.reset(TableFactory::getCopiedTempTable(0, EXISTING_TABLE, conflictExportTable, NULL));
        if (existingRows.size() > 0) {
            BOOST_FOREACH(boost::shared_ptr<TableTuple> tuple, existingRows) {
                createConflictExportTuple(existingTableForInsert.get(), drTable, pool, tuple.get(), actionType, insertConflict, EXISTING_ROW);
            }
        }

        newTableForInsert.reset(TableFactory::getCopiedTempTable(0, NEW_TABLE, conflictExportTable, NULL));
        if (newTuple) {
            createConflictExportTuple(newTableForInsert.get(), drTable, pool, newTuple, actionType, insertConflict, NEW_ROW);
        }
    }

//    //======================== test output ===========================
//    TableTuple tempTuple(conflictExportTable->schema());
//    if (deleteConflict != NO_CONFLICT) {
//        TableIterator iterator = existingTableForDelete->iterator();
//        bool once = true;
//        while (iterator.next(tempTuple)) {
//            if (once) {
//                std::cout << "\n========existingTableForDelete=========" << std::endl;
//                once = false;
//            }
//            std::cout << tempTuple.debugNoHeader() << std::endl;
//        }
//
//        iterator = expectedTableForDelete->iterator();
//        once = true;
//        while (iterator.next(tempTuple)) {
//            if (once) {
//                std::cout << "\n========expectedTableForDelete=========" << std::endl;
//                once = false;
//            }
//            std::cout << tempTuple.debugNoHeader() << std::endl;
//        }
//    } else {
//        std::cout << "\n========existingTableForDelete=========" << std::endl;
//        std::cout << "<null>" << std::endl;
//        std::cout << "\n========expectedTableForDelete=========" << std::endl;
//        std::cout << "<null>" << std::endl;
//    }
//
//    if (insertConflict != NO_CONFLICT) {
//        TableIterator iterator = existingTableForInsert->iterator();
//        bool once = true;
//        while (iterator.next(tempTuple)) {
//            if (once) {
//                std::cout << "\n========existingTableForInsert=========" << std::endl;
//                once = false;
//            }
//            std::cout << tempTuple.debugNoHeader() << std::endl;
//        }
//
//        iterator = newTableForInsert->iterator();
//        once = true;
//        while (iterator.next(tempTuple)) {
//            if (once) {
//                std::cout << "\n========newTableForInsert=========" << std::endl;
//            }
//            std::cout << tempTuple.debugNoHeader() << std::endl;
//        }
//    } else {
//        std::cout << "\n========existingTableForInsert=========" << std::endl;
//        std::cout << "<null>" << std::endl;
//        std::cout << "\n========newTableForInsert=========" << std::endl;
//        std::cout << "<null>" << std::endl;
//    }
//    //=================================================================

    bool diverge = ExecutorContext::getExecutorContext()->getTopend()->reportDRConflict(static_cast<int32_t>(UniqueId::pid(uniqueId)),
                                                                                      sequenceNumber,
                                                                                      drTable->name(),
                                                                                      actionType,
                                                                                      deleteConflict,
                                                                                      existingTableForDelete.get(),
                                                                                      expectedTableForDelete.get(),
                                                                                      insertConflict,
                                                                                      existingTableForInsert.get(),
                                                                                      newTableForInsert.get());

    TableTuple tempTuple(conflictExportTable->schema());
    if (deleteConflict != NO_CONFLICT) {
        if (existingTuple) {
            TableIterator iterator = existingTableForDelete->iterator();
            iterator.next(tempTuple);
            DRRowDecision decision = static_cast<DRRowDecision>(ValuePeeker::peekTinyInt(tempTuple.getNValue(3)));
            if (decision == DELETE_ROW) {
                drTable->deleteTuple(*existingTuple, true);
            }
        }
    }
    if (insertConflict != NO_CONFLICT) {
        TableIterator iterator = existingTableForInsert->iterator();
        for (int i = 0; iterator.next(tempTuple); i++) {
            DRRowDecision decision = static_cast<DRRowDecision>(ValuePeeker::peekTinyInt(tempTuple.getNValue(3)));
            if (decision == DELETE_ROW) {
                drTable->deleteTuple(*existingRows[i].get(), true);
            }
        }
        iterator = newTableForInsert->iterator();
        iterator.next(tempTuple);
        DRRowDecision decision = static_cast<DRRowDecision>(ValuePeeker::peekTinyInt(tempTuple.getNValue(3)));
        if (decision == KEEP_ROW) {
            drTable->insertPersistentTuple(*newTuple, true);
        }

    }
    exportDRConflict(conflictExportTable, diverge, existingTableForDelete.get(), expectedTableForDelete.get(), existingTableForInsert.get(), newTableForInsert.get());

    if (existingTableForDelete.get()) {
        existingTableForDelete.get()->deleteAllTuples(true);
    }
    if (expectedTableForDelete.get()) {
        expectedTableForDelete.get()->deleteAllTuples(true);
    }
    if (existingTableForInsert.get()) {
        existingTableForInsert.get()->deleteAllTuples(true);
    }
    if (newTableForInsert.get()) {
        newTableForInsert.get()->deleteAllTuples(true);
    }

    return true;
}
}

#endif
