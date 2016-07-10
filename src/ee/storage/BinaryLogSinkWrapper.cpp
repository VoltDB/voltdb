/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "BinaryLogSinkWrapper.h"

#include "storage/DRTupleStream.h"
#include "storage/CompatibleDRTupleStream.h"
#include "common/serializeio.h"

using namespace std;
using namespace voltdb;

int64_t BinaryLogSinkWrapper::apply(const char* taskParams, boost::unordered_map<int64_t, PersistentTable*> &tables,
                                    Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId)
{
    ReferenceSerializeInputLE taskInfo(taskParams + 4, ntohl(*reinterpret_cast<const int32_t*>(taskParams)));

    int64_t __attribute__ ((unused)) uniqueId = 0;
    int64_t __attribute__ ((unused)) sequenceNumber = -1;

    int64_t rowCount = 0;
    while (taskInfo.hasRemaining()) {
        pool->purge();
        const char* recordStart = taskInfo.getRawPointer();
        const uint8_t drVersion = taskInfo.readByte();
        if (drVersion == DRTupleStream::PROTOCOL_VERSION) {
            rowCount += m_sink.applyTxn(&taskInfo, tables, pool, engine, remoteClusterId,
                                        recordStart);
        } else if (drVersion == CompatibleDRTupleStream::COMPATIBLE_PROTOCOL_VERSION) {
            rowCount += m_compatibleSink.apply(&taskInfo, tables, pool, engine, remoteClusterId,
                                               recordStart, &uniqueId, &sequenceNumber);
        } else {
            throwFatalException("Unsupported DR version %d", drVersion);
        }
    }
    return rowCount;
}
