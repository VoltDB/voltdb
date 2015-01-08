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

#ifndef DRTUPLESTREAM_H_
#define DRTUPLESTREAM_H_

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/TupleStreamBase.h"
#include <deque>
#include <cassert>

namespace voltdb {
class StreamBlock;

class DRTupleStream : public voltdb::TupleStreamBase {
public:
    //Version(1), type(1), txnid(8), sphandle(8), checksum(4)
    static const size_t BEGIN_RECORD_SIZE = 1 + 1 + 8 + 8 + 4;
    //Version(1), type(1), sphandle(8), checksum(4)
    static const size_t END_RECORD_SIZE = 1 + 1 + 8 + 4;
    //Version(1), type(1), table signature(8), checksum(4)
    static const size_t TXN_RECORD_HEADER_SIZE = 1 + 1 + 4 + 8;
    static const uint8_t DR_VERSION = 0;

    DRTupleStream();

    virtual ~DRTupleStream() {
    }

    void configure(CatalogId partitionId) {
        m_partitionId = partitionId;
    }

    virtual void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream);

    /** write a tuple to the stream */
    virtual size_t appendTuple(int64_t lastCommittedSpHandle,
                       char *tableHandle,
                       int64_t txnId,
                       int64_t spHandle,
                       TableTuple &tuple,
                       DRRecordType type);

    size_t computeOffsets(TableTuple &tuple,size_t *rowHeaderSz);

    void beginTransaction(int64_t txnId, int64_t spHandle);
    void endTransaction(int64_t spHandle);

    bool m_enabled;
private:
    CatalogId m_partitionId;
};

class MockDRTupleStream : public DRTupleStream {
public:
    MockDRTupleStream() : DRTupleStream() {}
    size_t appendTuple(int64_t lastCommittedSpHandle,
                           char *tableHandle,
                           int64_t txnId,
                           int64_t spHandle,
                           TableTuple &tuple,
                           DRRecordType type) {
        return 0;
    }

    void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {}

    void rollbackTo(size_t mark) {}
};

}

#endif
