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

void BinaryLogSink::apply(const char *taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool) {
    ReferenceSerializeInputLE taskInfo(taskParams + 4, ntohl(*reinterpret_cast<const int32_t*>(taskParams)));

    while (taskInfo.hasRemaining()) {
        pool->purge();
        const char* recordStart = taskInfo.getRawPointer();
        const uint8_t drVersion = taskInfo.readByte();
        if (drVersion != 0) {
            throwFatalException("Unsupported DR version %d", drVersion);
        }
        const DRRecordType type = static_cast<DRRecordType>(taskInfo.readByte());

        int64_t tableHandle = 0;
        int64_t  __attribute__ ((unused)) txnId = 0;
        int64_t  __attribute__ ((unused)) spHandle = 0;
        uint32_t checksum = 0;
        const char * rowData = NULL;
        switch (type) {
        case DR_RECORD_DELETE:
        case DR_RECORD_INSERT: {
            tableHandle = taskInfo.readLong();
            int32_t rowLength = taskInfo.readInt();
            rowData = reinterpret_cast<const char *>(taskInfo.getRawPointer(rowLength));
            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());

            boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
            if (tableIter == tables.end()) {
                throwFatalException("Where is my table at yo? %jd", (intmax_t)tableHandle);
            }

            PersistentTable *table = tableIter->second;

            TableTuple tempTuple = table->tempTuple();

            ReferenceSerializeInputLE rowInput(rowData,  rowLength);
            tempTuple.deserializeFromDR(rowInput, pool);

            if (type == DR_RECORD_DELETE) {
                TableTuple deleteTuple = table->lookupTuple(tempTuple);
                table->deleteTuple(deleteTuple, false);
            } else {
                table->insertTuple(tempTuple);
            }
            break;
        }
        case DR_RECORD_BEGIN_TXN: {
            txnId = taskInfo.readLong();
            spHandle = taskInfo.readLong();
            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());
            break;
        }
        case DR_RECORD_END_TXN: {
            spHandle = taskInfo.readLong();
            checksum = taskInfo.readInt();
            validateChecksum(checksum, recordStart, taskInfo.getRawPointer());
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
