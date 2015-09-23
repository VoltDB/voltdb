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

//class DRConflict {
//public:
//    DRConflict();
//    ~DRConflict();
//private:
//    Table* m_existingTable;
//    Table* m_expectedTable;
//    Table* m_newTable;
//    Table* m_outputTable;
//    DRConflictType m_conflictType;
//    DRRecordType m_actionType;
//    int64_t m_uniqueId;
//    int64_t m_sequenceId;
//};

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
                    if (handleConflict(engine, table, pool, NULL, NULL, e.getConflictTuple(), uniqueId, sequenceNumber, DR_RECORD_INSERT, CONFLICT_NEW_ROW_UNIQUE_CONSTRAINT_VIOLATION)) {
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
                    if (handleConflict(engine, table, pool, NULL, &tempTuple, NULL, uniqueId, sequenceNumber, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISSING)) {
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
                    if (handleConflict(engine, table, pool, &deleteTuple, &tempTuple, NULL, uniqueId, sequenceNumber, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_TIMESTAMP_MISMATCH)) {
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
                    char * data = new char[tempTuple.tupleLength()];
                    oldTuple.move(data);
                    oldTuple.copy(tempTuple);
                    ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
                    tempTuple.deserializeFromDR(newRowInput, pool);
                    if (handleConflict(engine, table, pool, NULL, &oldTuple, &tempTuple, uniqueId, sequenceNumber, DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_MISSING)) {
                        delete [] data;
                        continue;
                    }
                    delete [] data;
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
                    char * data = new char[tempTuple.tupleLength()];
                    expectedTuple.move(data);
                    expectedTuple.copy(tempTuple);
                    ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
                    tempTuple.deserializeFromDR(newRowInput, pool);
                    if (handleConflict(engine, table, pool, &oldTuple, &expectedTuple, &tempTuple, uniqueId, sequenceNumber, DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_TIMESTAMP_MISMATCH)) {
                        delete [] data;
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
                    if (handleConflict(engine, table, pool, e.getOriginalTuple(), e.getOriginalTuple(), e.getConflictTuple(), uniqueId, sequenceNumber, DR_RECORD_UPDATE, CONFLICT_NEW_ROW_UNIQUE_CONSTRAINT_VIOLATION)) {
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

void BinaryLogSink::exportDRConflict(Table *exportTable, TempTable *existingTable, TempTable *expectedTable, TempTable *newTable, TempTable *outputTable) {
    assert(exportTable != NULL);
    assert(exportTable->isExport());

    // iterate all four tables and push them into export table
    TableTuple tempTuple(exportTable->schema());
    TableIterator iterator = existingTable->iterator();
    while (iterator.next(tempTuple)) {
        exportTable->insertTuple(tempTuple);
    }

    iterator = expectedTable->iterator();
    while (iterator.next(tempTuple)) {
        exportTable->insertTuple(tempTuple);
    }

    iterator = newTable->iterator();
    while (iterator.next(tempTuple)) {
        exportTable->insertTuple(tempTuple);
    }

    iterator = outputTable->iterator();
    while (iterator.next(tempTuple)) {
        exportTable->insertTuple(tempTuple);
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

// iterate all unique indices to find any conflicting tuples (under assumption that primary key is the first index in the index vector)
// TODO: conflict rows may contains duplicated rows, reduce the redundancy
void BinaryLogSink::findConflictTuple(Table *table, TableTuple *searchTuple, TableTuple *expectedTuple, std::vector<TableTuple *> &conflictRows) {
    BOOST_FOREACH(TableIndex* index, table->allIndexes()) {
        if (index->isUniqueIndex()) {
            TableTuple* conflictTuple = new TableTuple();
            if (index->exists(searchTuple, conflictTuple)) {
                if (expectedTuple && expectedTuple->equals(*conflictTuple)) {
                    continue; // skip the expected tuple
                } else if (searchTuple->equals(*conflictTuple)) {
                    continue; // skip the search tuple
                } else {
                    conflictRows.push_back(conflictTuple);
                }
            }
        }
    }
}

void BinaryLogSink::createConflictExportTuple(TempTable *outputTable, PersistentTable *drTable, Pool *pool, TableTuple *tupleToBeWrote, DRRecordType actionType, DRConflictType conflictType, DRConflictRowType rowType) {
    TableTuple tempTuple = outputTable->tempTuple();
    NValue hiddenValue = tupleToBeWrote->getHiddenNValue(drTable->getDRTimestampColumnIndex());
    tempTuple.setNValue(0, ValueFactory::getTinyIntValue(rowType));
    tempTuple.setNValue(1, ValueFactory::getTinyIntValue(actionType));
    tempTuple.setNValue(2, ValueFactory::getTinyIntValue(conflictType));
    switch (rowType) {
    case CONFLICT_EXISTING_ROW:
    case CONFLICT_EXPECTED_ROW:
        tempTuple.setNValue(3, ValueFactory::getTinyIntValue(CONFLICT_KEEP_ROW));   // decision
        break;
    case CONFLICT_NEW_ROW:
        tempTuple.setNValue(3, ValueFactory::getTinyIntValue(CONFLICT_DELETE_ROW));     // decision
        break;
    case CONFLICT_CUSTOM_ROW:
    default:
        break;
    }
    tempTuple.setNValue(4, ValueFactory::getTinyIntValue((ExecutorContext::getClusterIdFromHiddenNValue(hiddenValue))));    // clusterId
    tempTuple.setNValue(5, ValueFactory::getBigIntValue(ExecutorContext::getDRTimestampFromHiddenNValue(hiddenValue)));     // timestamp
    tempTuple.setNValues(6, *tupleToBeWrote, 0, tupleToBeWrote->sizeInValues());    // rest of columns, excludes the hidden column

    outputTable->insertTupleNonVirtualWithDeepCopy(tempTuple, pool);
}

DRConflictType BinaryLogSink::optimizeUpdateConflictType(PersistentTable* drTable, TableTuple* expectedTuple, TableTuple* newTuple, std::vector<TableTuple*> &existingRows, DRConflictType conflictType) {
    if (conflictType == CONFLICT_EXPECTED_ROW_MISSING || conflictType == CONFLICT_EXPECTED_ROW_TIMESTAMP_MISMATCH) {
        // add any rows conflict with the new row
        findConflictTuple(drTable, newTuple, expectedTuple, existingRows);
        if (existingRows.size() > 0) {
            if (conflictType == CONFLICT_EXPECTED_ROW_MISSING) {
                conflictType = CONFLICT_EXPECTED_ROW_MISSING_AND_NEW_ROW_CONSTRAINT;
            }
            if (conflictType == CONFLICT_EXPECTED_ROW_TIMESTAMP_MISMATCH) {
                conflictType = CONFLICT_EXPECTED_ROW_TIMESTAMP_AND_NEW_ROW_CONSTRAINT;
            }
        }
    }
    if (conflictType == CONFLICT_EXPECTED_ROW_MISSING ||
            conflictType == CONFLICT_NEW_ROW_UNIQUE_CONSTRAINT_VIOLATION ||
        conflictType == CONFLICT_EXPECTED_ROW_MISSING_AND_NEW_ROW_CONSTRAINT) {
        // check if change happens on primary key
        TableIndex * primaryKey = drTable->primaryKeyIndex();
        if (primaryKey && primaryKey->checkForIndexChange(expectedTuple, newTuple) == true) {
            conflictType = static_cast<DRConflictType>(conflictType + 1);
        }
    }
    return conflictType;
}

bool BinaryLogSink::handleConflict(VoltDBEngine *engine, PersistentTable *drTable, Pool *pool, TableTuple *existingTuple, TableTuple *expectedTuple, TableTuple *newTuple,
        int64_t uniqueId, int64_t sequenceNumber, DRRecordType actionType, DRConflictType conflictType) {
    if (engine) {
        Table* conflictExportTable = engine->getDRConflictTable(drTable);
        if (conflictExportTable) {
            // add new row
            std::vector<TableTuple *> existingRows;
            TempTable* newTable = TableFactory::getCopiedTempTable(0, NEW_TABLE, conflictExportTable, NULL);
            if (newTuple) {
                if (actionType == DR_RECORD_UPDATE) {
                    conflictType = optimizeUpdateConflictType(drTable, expectedTuple, newTuple, existingRows, conflictType);
                }
                if (actionType == DR_RECORD_INSERT) {
                    // add any rows conflict with the new row
                    findConflictTuple(drTable, newTuple, NULL, existingRows);
                }
                createConflictExportTuple(newTable, drTable, pool, newTuple, actionType, conflictType, CONFLICT_NEW_ROW);
            }

            // add expected row
            TempTable* expectedTable = TableFactory::getCopiedTempTable(0, EXPECTED_TABLE, conflictExportTable, NULL);
            if (expectedTuple) {
                createConflictExportTuple(expectedTable, drTable, pool, expectedTuple, actionType, conflictType, CONFLICT_EXPECTED_ROW);
            }

            // add existing row
            TempTable* existingTable = TableFactory::getCopiedTempTable(0, EXISTING_TABLE, conflictExportTable, NULL);
            if (existingTuple) {
                createConflictExportTuple(existingTable, drTable, pool, existingTuple, actionType, conflictType, CONFLICT_EXISTING_ROW);
            }
            if (existingRows.size() > 0) {
                BOOST_FOREACH(TableTuple* tuple, existingRows) {
                    createConflictExportTuple(existingTable, drTable, pool, tuple, actionType, conflictType, CONFLICT_EXISTING_ROW);
                }
            }

            // TODO: the size of output table should be same as existingTable
            TempTable* outputTable = TableFactory::getCopiedTempTable(0, OUTPUT_TABLE, conflictExportTable, NULL);

            //======================== test output ===========================
            TableTuple tempTuple(conflictExportTable->schema());
            TableIterator iterator = existingTable->iterator();
            bool first = true;
            while (iterator.next(tempTuple)) {
                if (first) {
                    std::cout << "\n========existingTable=========" << std::endl;
                    first = false;
                }
                std::cout << tempTuple.debugNoHeader() << std::endl;
            }

            iterator = expectedTable->iterator();
            first = true;
            while (iterator.next(tempTuple)) {
                if (first) {
                    std::cout << "\n========expectedTable=========" << std::endl;
                    first = false;
                }
                std::cout << tempTuple.debugNoHeader() << std::endl;
            }

            iterator = newTable->iterator();
            first = true;
            while (iterator.next(tempTuple)) {
                if (first) {
                    std::cout << "\n========newTable=========" << std::endl;
                }
                std::cout << tempTuple.debugNoHeader() << std::endl;
            }
            //=================================================================

            DRResolutionType retval =static_cast<DRResolutionType>(ExecutorContext::getExecutorContext()->getTopend()->reportDRConflict(UniqueId::pid(uniqueId),
                                                                                                                                      sequenceNumber,
                                                                                                                                      conflictType,
                                                                                                                                      actionType,
                                                                                                                                      drTable->name(),
                                                                                                                                      existingTable,
                                                                                                                                      expectedTable,
                                                                                                                                      newTable,
                                                                                                                                      outputTable));
            switch (retval) {
            case CONFLICT_DO_NOTHING: {
                // Literally meaning don't do anything except logging the conflict
                exportDRConflict(conflictExportTable, existingTable, expectedTable, newTable, outputTable);
                break;
            }
            case CONFLICT_APPLY_NEW: {
                // Delete rows in existing table that conflict with other rows and apply the new row
                BOOST_FOREACH(TableTuple* tuple, existingRows) {
                    drTable->deleteTuple(*tuple, true);
                    delete tuple;
                }
                if (actionType == DR_RECORD_INSERT) {
                    drTable->insertPersistentTuple(*newTuple, true);
                } else if (actionType == DR_RECORD_UPDATE) {
                    // TODO: should we set expectedTuple as the old value here?
                    drTable->updateTupleWithSpecificIndexes(*expectedTuple, *newTuple, drTable->allIndexes());
                }
                break;
            }
            case CONFLICT_DELETE_EXISTING: {
                // Delete all rows in existing table that be marked with "DELETE", ignore the new row
                TableTuple tempTuple(conflictExportTable->schema());
                TableIterator iterator = existingTable->iterator();
                while (iterator.next(tempTuple)) {
                    DRRowDecision decision = static_cast<DRRowDecision>(ValuePeeker::peekTinyInt(tempTuple.getNValue(3)));
                    if (decision == CONFLICT_DELETE_ROW) {
                        drTable->deleteTuple(tempTuple, true);
                    }
                }
                break;
            }
            case CONFLICT_APPLY_GENERATED: {
                //TODO: implement custom conflict resolver
                break;
            }
            case BREAK_REPLICATION: {
                // Ignore current record, commit the transaction then throw a break replication exception.
                throwSerializableEEException("Because of a unresolvable conflict in table %s actionType %d conflictType %d sequenceNumber %jd uniqueId %jd, DR replication is broken",
                                                             drTable->name().c_str(), (int8_t)actionType, (int8_t)conflictType, (intmax_t)sequenceNumber, (intmax_t)uniqueId);
                break;
            }
            default:
                return false;
            }

            delete existingTable;
            delete expectedTable;
            delete newTable;
            delete outputTable;
        }
    }

    return true;
}
}

#endif
