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
#include "indexes/tableindex.h"

#include "catalog/Database.h"

#include<boost/unordered_map.hpp>
#include<crc/crc32c.h>

namespace voltdb {

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
        int retval = 0;

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
                // Conflict detection handling
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    retval = reportDRConflict(table, UniqueId::pid(uniqueId), sequenceNumber,
                            DR_CONFLICT_UNIQUE_CONSTRIANT_VIOLATION, DR_RECORD_INSERT,
                            e.getSourceTuple(), NULL, e.getConflictTuple(), NULL);
                }
                if (retval != 0) {
                    throw;
                } else {
                    //delete the tuple and insert a new tuple based on retval
                    break;
                }
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
                    retval = reportDRConflict(table, UniqueId::pid(uniqueId), sequenceNumber,
                            DR_CONFLICT_MISSING_TUPLE, DR_RECORD_DELETE,
                            NULL, NULL, &tempTuple, NULL);
                }
                if (retval != 0) {
                    throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
                } else {
                    // the only choice for delete missing tuple resolution is just do nothing
                    break;
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
                    TableTuple newTuple(table->schema());
                    char * data = new char[tempTuple.tupleLength()];
                    newTuple.move(data);
                    ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
                    newTuple.deserializeFromDR(newRowInput, pool);
                    retval = reportDRConflict(table, UniqueId::pid(uniqueId), sequenceNumber,
                            DR_CONFLICT_MISSING_TUPLE, DR_RECORD_UPDATE,
                            NULL, &tempTuple, &newTuple, NULL);
                    delete [] data;
                }
                if (retval != 0) {
                    throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
                } else {
                    // either do nothing or insert the new tuple.
                    break;
                }
            }

            ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
            tempTuple.deserializeFromDR(newRowInput, pool);

            table->updateTupleWithSpecificIndexes(oldTuple, tempTuple, table->allIndexes());
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
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    // create tuple to accommodate new row
                    TableTuple newTuple(table->schema());
                    char * data = new char[tempTuple.tupleLength()];
                    newTuple.move(data);
                    ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
                    newTuple.deserializeFromDR(newRowInput, pool);
                    retval = reportDRConflict(table, UniqueId::pid(uniqueId), sequenceNumber,
                            DR_CONFLICT_MISSING_TUPLE, DR_RECORD_UPDATE,
                            NULL, &tempTuple, &newTuple, NULL);
                    delete [] data;
                }
                if (retval != 0) {
                    throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                                type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
                } else {
                    // either do nothing or insert the new tuple.
                    break;
                }
            }

            tempTuple = table->tempTuple();
            ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
            tempTuple.deserializeFromDR(newRowInput, pool);

            try {
                table->updateTupleWithSpecificIndexes(oldTuple, tempTuple, table->allIndexes());
            } catch (ConstraintFailureException &e) {
                if (isActiveActiveDREnabled && table->isDREnabled()) {
                    // we only use first three DR record types to represent the action, so here we use DR_RECORD_UPDATE
                    retval = reportDRConflict(table, UniqueId::pid(uniqueId), sequenceNumber,
                            DR_CONFLICT_UNIQUE_CONSTRIANT_VIOLATION, DR_RECORD_UPDATE,
                            e.getSourceTuple(), &oldTuple, e.getConflictTuple(), NULL);
                }
                if (retval != 0) {
                    throw;
                } else {
                    //delete the tuple and insert a new tuple based on retval
                    break;
                }
            }
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

void BinaryLogSink::exportDRConflict(PersistentTable *drTable, Table *exportTable, const DRRecordType &type, TableTuple &exportTuple) {
    assert(exportTable != NULL);
    assert(exportTable->isExport());

    TableTuple tempTuple = exportTable->tempTuple();
    NValue hiddenColumn = exportTuple.getHiddenNValue(drTable->getDRTimestampColumnIndex());

    NValue tableName = ValueFactory::getStringValue(drTable->name());
    tempTuple.setNValue(0, tableName);  // Table Name
    tempTuple.setNValue(1, ValueFactory::getTinyIntValue((ExecutorContext::getClusterIdFromHiddenNValue(hiddenColumn))));       // Cluster Id
    tempTuple.setNValue(2, ValueFactory::getBigIntValue(ExecutorContext::getDRTimestampFromHiddenNValue(hiddenColumn)));   // Timestamp
    tempTuple.setNValue(3, ValueFactory::getTinyIntValue(type));            // Type of Operation
    tempTuple.setNValues(4, exportTuple, 0, exportTuple.sizeInValues());    // rest of columns

    exportTable->insertTuple(tempTuple);
    tableName.free();
}

void BinaryLogSink::validateChecksum(uint32_t checksum, const char *start, const char *end) {
    uint32_t recalculatedCRC = vdbcrc::crc32cInit();
    recalculatedCRC = vdbcrc::crc32c( recalculatedCRC, start, (end - 4) - start);
    recalculatedCRC = vdbcrc::crc32cFinish(recalculatedCRC);

    if (recalculatedCRC != checksum) {
        throwFatalException("CRC mismatch of DR log data %d and %d", checksum, recalculatedCRC);
    }
}

int BinaryLogSink::reportDRConflict(Table* table, int64_t partitionId, int64_t sequenceNumber, DRConflictType conflictType,
        DRRecordType recordType, TableTuple* existingRow, TableTuple* expectedRow, TableTuple* newRow,
        TableTuple* outputRow) {
    char signature[20];
    Table* existingTable = TableFactory::getPersistentTable(0, "existingRow",
            TupleSchema::createTupleSchema(table->schema()), table->getColumnNames(), signature);
    if (existingRow) {
        existingTable->insertTuple(*existingRow);
    }
    Table* expectedTable = TableFactory::getPersistentTable(0, "expectedRow",
            TupleSchema::createTupleSchema(table->schema()), table->getColumnNames(), signature);
    if (expectedRow) {
        expectedTable->insertTuple(*expectedRow);
    }
    Table* newTable = TableFactory::getPersistentTable(0, "newRow",
            TupleSchema::createTupleSchema(table->schema()), table->getColumnNames(), signature);
    if (newRow) {
        newTable->insertTuple(*newRow);
    }
    newTable->insertTuple(*newRow);
    Table* outputTable = TableFactory::getPersistentTable(0, "outputRow",
            TupleSchema::createTupleSchema(table->schema()), table->getColumnNames(), signature);
    int retval = ExecutorContext::getExecutorContext()->getTopend()->reportDRConflict(partitionId,
                                                                      sequenceNumber,
                                                                      conflictType,
                                                                      recordType,
                                                                      table->name(),
                                                                      existingTable,
                                                                      expectedTable,
                                                                      newTable,
                                                                      outputTable);
    delete existingTable;
    delete expectedTable;
    delete newTable;
    delete outputTable;
    return retval;
}

}

#endif
