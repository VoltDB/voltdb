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
#include "storage/BinaryLogSink.h"
#include "storage/persistenttable.h"
#include "indexes/tableindex.h"

#include<boost/unordered_map.hpp>
#include<crc/crc32c.h>

namespace voltdb {

const uint8_t MAX_CLUSTER_ID = static_cast<uint8_t>((1 << 8) - 1);
const int64_t MAX_SEOUENCE_NUMBER = (1L << 55) - 1L;

static uint8_t getClusterIdFromDRId(int64_t drId) {
    return static_cast<uint8_t>((drId >> 55) & MAX_CLUSTER_ID);
}

static int64_t getSequenceNumberFromDRId(int64_t drId) {
    return drId & MAX_SEOUENCE_NUMBER;
}

class CachedIndexKeyTuple {
public:
    CachedIndexKeyTuple() : m_tuple(), m_cachedIndexCrc(0), m_storageSize(0), m_tupleStorage() {}

    TableTuple &tuple(PersistentTable *table, uint32_t indexCrc) {
        if (m_storageSize > 0 && indexCrc == m_cachedIndexCrc) {
            return m_tuple;
        }
        std::pair<const TableIndex*, uint32_t> index = table->getSmallestUniqueIndex();
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


int64_t BinaryLogSink::apply(const char *taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine) {
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
            table->insertPersistentTuple(tempTuple, true);
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
                throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
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
                throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
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

            const TableIndex* index = table->getSmallestUniqueIndex().first;
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

            const TableIndex* index = table->getSmallestUniqueIndex().first;
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

void BinaryLogSink::exportDRConflict(PersistentTable *drTable, Table *exportTable, const DRRecordType &type, TableTuple &exportTuple) {
    if (exportTable != NULL && exportTable->isExport()) {
        TableTuple tempTuple = exportTable->tempTuple();
        NValue hiddenColumn = exportTuple.getHiddenNValue(drTable->getDRTimestampColumnIndex());
        int64_t drId = ValuePeeker::peekAsBigInt(hiddenColumn);

        tempTuple.setNValue(0, ValueFactory::getStringValue(drTable->name()));  // Table Name
        tempTuple.setNValue(1, ValueFactory::getTinyIntValue(getClusterIdFromDRId(drId)));       // Cluster Id
        tempTuple.setNValue(2, ValueFactory::getBigIntValue(getSequenceNumberFromDRId(drId)));   // Timestamp
        tempTuple.setNValue(3, ValueFactory::getTinyIntValue(type));            // Type of Operation
        tempTuple.setNValues(4, exportTuple, 0, exportTuple.sizeInValues());    // rest of columns

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
}

#endif
