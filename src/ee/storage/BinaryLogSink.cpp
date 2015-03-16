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
#include "storage/BinaryLogSink.h"
#include "storage/persistenttable.h"

#include<boost/unordered_map.hpp>
#include<crc/crc32c.h>

namespace voltdb {

BinaryLogSink::BinaryLogSink() {}

void BinaryLogSink::apply(const char *taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine) {
    ReferenceSerializeInputLE taskInfo(taskParams + 4, ntohl(*reinterpret_cast<const int32_t*>(taskParams)));

    int64_t __attribute__ ((unused)) uniqueId = 0;
    int64_t __attribute__ ((unused)) sequenceNumber = -1;
    bool hasData = false;
    while (taskInfo.hasRemaining()) {
        pool->purge();
        const char* recordStart = taskInfo.getRawPointer();
        const uint8_t drVersion = taskInfo.readByte();
        if (drVersion != 0) {
            throwFatalException("Unsupported DR version %d", drVersion);
        }
        const DRRecordType type = static_cast<DRRecordType>(taskInfo.readByte());

        int64_t tableHandle = 0;

        uint32_t checksum = 0;
        const char * rowData = NULL;
        switch (type) {
        case DR_RECORD_DELETE:
        case DR_RECORD_INSERT: {
            hasData = true;
            tableHandle = taskInfo.readLong();
            int32_t rowLength = taskInfo.readInt();
            rowData = reinterpret_cast<const char *>(taskInfo.getRawPointer(rowLength));
            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwFatalException("Unable to find table hash %jd while applying a binary log insert/delete record",
                                    (intmax_t)tableHandle);
            }

            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = table->tempTuple();

            ReferenceSerializeInputLE rowInput(rowData,  rowLength);
            tempTuple.deserializeFromDR(rowInput, pool);

            if (type == DR_RECORD_DELETE) {
                TableTuple deleteTuple = table->lookupTupleByValues(tempTuple);
                if (deleteTuple.isNullTuple()) {
                    char msg[1024 * 100];
                    snprintf(msg, 1024 * 100,
                             "Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
                    VOLT_ERROR("%s", msg);
                    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
                }
                table->deleteTuple(deleteTuple, true);
            } else {
                table->insertPersistentTuple(tempTuple, true);
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
                } else if (tempSequenceNumber == sequenceNumber && hasData) {
                    throwFatalException("Found duplicate transactions inside a binary log segment. Expected %jd but found %jd",
                                        (intmax_t)(sequenceNumber + 1), (intmax_t)tempSequenceNumber);
                } else if (tempSequenceNumber > sequenceNumber + 1) {
                    throwFatalException("Found sequencing gap inside a binary log segment. Expected %jd but found %jd",
                                        (intmax_t)(sequenceNumber + 1), (intmax_t)tempSequenceNumber);
                }
            }
            hasData = false;
            sequenceNumber = tempSequenceNumber;
            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());
            break;
        }
        case DR_RECORD_END_TXN: {
            int64_t tempSequenceNumber = taskInfo.readLong();
            if (tempSequenceNumber != sequenceNumber) {
                throwFatalException("Closing the wrong transaction inside a binary log segment. Expected %jd but found %jd",
                                    (intmax_t)sequenceNumber, (intmax_t)tempSequenceNumber);
            }

            // Not setting hasData to false here so that we can detect duplicate transactions.
            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());
            break;
        }
        case DR_RECORD_TRUNCATE_TABLE: {
            hasData = true;
            tableHandle = taskInfo.readLong();
            std::string tableName = taskInfo.readTextString();

            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwFatalException("Unable to find table %s hash %jd while applying binary log for truncate record",
                                    tableName.c_str(), (intmax_t)tableHandle);
            }

            PersistentTable *table = tableIter->second;

            table->truncateTable(engine, true);
            break;
        }
        case DR_RECORD_UPDATE:
        default:
            throwFatalException("Unrecognized DR record type %d", type);
            break;
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
}

#endif
