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

#ifndef EXPORTTUPLESTREAM_H_
#define EXPORTTUPLESTREAM_H_
#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/TupleStreamBase.h"
#include <deque>
#include <cassert>
namespace voltdb {

class StreamBlock;

class ExportTupleStream : public voltdb::TupleStreamBase {
public:
    enum Type { INSERT, DELETE };

    ExportTupleStream(CatalogId partitionId, int64_t siteId);

    virtual ~ExportTupleStream() {
    }

    void setSignatureAndGeneration(std::string signature, int64_t generation);

    /** Read the total bytes used over the life of the stream */
    size_t bytesUsed() {
        return m_uso;
    }

    /** Set the total number of bytes used (for rejoin/recover) */
    void setBytesUsed(size_t count) {
        assert(m_uso == 0);
        StreamBlock *sb = new StreamBlock(new char[1], 0, count);
        ExecutorContext::getExecutorContext()->getTopend()->pushExportBuffer(
                                m_generation, m_partitionId, m_signature, sb, false, false);
        delete sb;
        m_uso = count;
        //Extend the buffer chain to replace any existing stream blocks with a new one
        //with the correct USO
        extendBufferChain(0);
    }

    int64_t allocatedByteCount() const {
        return (m_pendingBlocks.size() * (m_defaultCapacity- MAGIC_HEADER_SPACE_FOR_JAVA)) +
                ExecutorContext::getExecutorContext()->getTopend()->getQueuedExportBytes( m_partitionId, m_signature);
    }

    void pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream);

    /** write a tuple to the stream */
    size_t appendTuple(int64_t lastCommittedSpHandle,
                       int64_t spHandle,
                       int64_t seqNo,
                       int64_t uniqueId,
                       int64_t timestamp,
                       TableTuple &tuple,
                       ExportTupleStream::Type type);

    size_t computeOffsets(TableTuple &tuple,size_t *rowHeaderSz);

    virtual int partitionId() { return m_partitionId; }

    // cached catalog values
    const CatalogId m_partitionId;
    const int64_t m_siteId;

    std::string m_signature;
    int64_t m_generation;
};

}

#endif
