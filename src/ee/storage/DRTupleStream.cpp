/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "storage/DRTupleStream.h"

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/executorcontext.hpp"
#include "crc/crc32c.h"

#include <cstdio>
#include <limits>
#include <iostream>
#include <cassert>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

DRTupleStream::DRTupleStream()
    : TupleStreamBase(),
      m_enabled(true),
      m_partitionId(0),m_secondaryCapacity(SECONDAERY_BUFFER_SIZE)
{}

void DRTupleStream::setSecondaryCapacity(size_t capacity) {
    assert (capacity > 0);
    if (m_uso != 0 || m_openSpHandle != 0 ||
        m_openTransactionUso != 0 || m_committedSpHandle != 0)
    {
        throwFatalException("setSecondaryCapacity only callable before "
                            "TupleStreamBase is used");
    }
    m_secondaryCapacity = capacity;
}

/*
 * If SpHandle represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t DRTupleStream::appendTuple(int64_t lastCommittedSpHandle,
                                  char *tableHandle,
                                  int64_t txnId,
                                  int64_t spHandle,
                                  TableTuple &tuple,
                                  DRRecordType type)
{
    //Drop the row, don't move the USO
    if (!m_enabled) return m_uso;

    size_t rowHeaderSz = 0;
    size_t tupleMaxLength = 0;

    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openSpHandle)
    {
        throwFatalException(
                "Active transactions moving backwards: openSpHandle is %jd, while the append spHandle is %jd",
                (intmax_t)m_openSpHandle, (intmax_t)spHandle
                );
    }

    commit(lastCommittedSpHandle, spHandle, txnId, false, false);

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(tuple, &rowHeaderSz) + TXN_RECORD_HEADER_SIZE;

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if (m_currBlock->remaining() < tupleMaxLength) {
        extendBufferChain(tupleMaxLength);
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());
    io.writeByte(DR_VERSION);
    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr() + io.position(), 0, rowHeaderSz);

    const size_t lengthPrefixPosition = io.reserveBytes(rowHeaderSz);

    // the nullarray lives in rowheader after the 4 byte header length prefix
    uint8_t *nullArray =
      reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + io.position());

    // write the tuple's data
    tuple.serializeToExport(io, 0, nullArray);

    // write the row size in to the row header
    // rowlength does not include the 4 byte length prefix or record header
    // but does include the null array.
    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr() + lengthPrefixPosition, 4);
    //The TXN_RECORD_HEADER_SIZE is 4 bytes longer because it includes the checksum at the end
    //so there is no need to subtract and additional 4 bytes to make the length prefix not inclusive
    hdr.writeInt((int32_t)(io.position() - TXN_RECORD_HEADER_SIZE));

    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), io.position());
    crc = vdbcrc::crc32cFinish(crc);
    io.writeInt(crc);

    // update m_offset
    m_currBlock->consumed(io.position());
//
//    // update uso.
    const size_t startingUso = m_uso;
    m_uso += io.position();

//    std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t
DRTupleStream::computeOffsets(TableTuple &tuple,
                                   size_t *rowHeaderSz)
{
    // round-up columncount to next multiple of 8 and divide by 8
    const int columnCount = tuple.sizeInValues();
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;

    // row header is 32-bit length of row plus null mask
    *rowHeaderSz = sizeof(int32_t) + nullMaskLength;

    //Can return 0 for a single column varchar with null
    size_t dataSz = tuple.maxExportSerializationSize();

    return *rowHeaderSz + dataSz;
}

void DRTupleStream::pushExportBuffer(StreamBlock *block, bool sync, bool endOfStream) {
    if (sync) return;
    ExecutorContext::getExecutorContext()->getTopend()->pushDRBuffer(m_partitionId, block);
}

void DRTupleStream::beginTransaction(int64_t txnId, int64_t spHandle) {
//    std::cout << "Beginning txn " << txnId << " spHandle " << std::endl;
    if (!m_currBlock) {
         extendBufferChain(m_defaultCapacity);
     }

     m_currBlock->recordLastBeginTxnOffset();

     if (m_currBlock->remaining() < BEGIN_RECORD_SIZE) {
         extendBufferChain(BEGIN_RECORD_SIZE);
     }
     ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                              m_currBlock->remaining());
     io.writeByte(DR_VERSION);
     io.writeByte(static_cast<int8_t>(DR_RECORD_BEGIN_TXN));
     io.writeLong(txnId);
     io.writeLong(spHandle);
     uint32_t crc = vdbcrc::crc32cInit();
     crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), BEGIN_RECORD_SIZE - 4);
     crc = vdbcrc::crc32cFinish(crc);
     io.writeInt(crc);
     m_currBlock->consumed(io.position());
     m_uso += io.position();
}

void DRTupleStream::endTransaction(int64_t spHandle) {
//    std::cout << "Ending txn spHandle " << spHandle << std::endl;
    if (!m_currBlock) {
         extendBufferChain(m_defaultCapacity);
     }

     if (m_currBlock->remaining() < END_RECORD_SIZE) {
         extendBufferChain(END_RECORD_SIZE);
     }
     ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                              m_currBlock->remaining());
     io.writeByte(DR_VERSION);
     io.writeByte(static_cast<int8_t>(DR_RECORD_END_TXN));
     io.writeLong(spHandle);
     uint32_t crc = vdbcrc::crc32cInit();
     crc = vdbcrc::crc32c( crc, m_currBlock->mutableDataPtr(), END_RECORD_SIZE - 4);
     crc = vdbcrc::crc32cFinish(crc);
     io.writeInt(crc);
     m_currBlock->consumed(io.position());
     m_uso += io.position();
}

void DRTupleStream::extendBufferChain(size_t minLength) {
    if (m_defaultCapacity < minLength) {
        // exportxxx: rollback instead?
        throwFatalException("Default capacity is less than required buffer size.");
    }

    StreamBlock* oldBlock = NULL;
    size_t partialTxnLength = 0;
    size_t blockSize = m_defaultCapacity;
    bool spanBuffer = false;
    bool throwException = false;
    size_t uso = m_uso;

    if (m_currBlock) {
        if (m_currBlock->offset() > 0) {
            m_pendingBlocks.push_back(m_currBlock);
            oldBlock = m_currBlock;
            m_currBlock = NULL;
        }
        // fully discard empty blocks. makes valgrind/testcase
        // conclusion easier.
        else {
            discardBlock(m_currBlock);
            m_currBlock = NULL;
        }
    }

    // If partial transaction is going to span multiple buffer, first time move it to
    // the next buffer, the next time move it to a 45 megabytes buffer, then after throw
    // an exception and rollback.
    if (oldBlock && oldBlock->remaining() < minLength   /* remain space is not big enough */
            && oldBlock->hasDRBeginTxn()   /* this block contains a DR begin txn */
            && oldBlock->lastDRBeginTxnOffset() != oldBlock->offset() /* current txn is not a DR begin txn */) {
        spanBuffer = true;
        partialTxnLength = oldBlock->offset() - oldBlock->lastDRBeginTxnOffset();
        if (partialTxnLength + minLength >= (m_defaultCapacity - MAGIC_HEADER_SPACE_FOR_JAVA)) {
            switch (oldBlock->type()) {
                case voltdb::NORMAL_STREAM_BLOCK:
                {
                    blockSize = m_secondaryCapacity;
                    break;
                }
                case voltdb::LARGE_STREAM_BLOCK:
                {
                    throwException = true;
                    break;
                }
            }
        }
        if (!throwException) {
            uso -= partialTxnLength;
        }
    }
    char * buffer = new char[blockSize];
    if (!buffer) {
        throwFatalException("Failed to claim managed buffer for DR");
    }
    m_currBlock = new StreamBlock(buffer, blockSize, uso);
    if (blockSize == m_secondaryCapacity) {
        m_currBlock->setType(LARGE_STREAM_BLOCK);
    }

    if (throwException) {
        rollbackTo(uso);
        throw SQLException(SQLException::volt_output_buffer_overflow, "Transaction is bigger than DR Buffer size");
    }

    if (spanBuffer) {
        ::memcpy(m_currBlock->mutableDataPtr(), oldBlock->mutableLastBeginTxnDataPtr(), partialTxnLength);
        m_currBlock->recordLastBeginTxnOffset();
        m_currBlock->consumed(partialTxnLength);
        ::memset(oldBlock->mutableLastBeginTxnDataPtr(), 0, partialTxnLength);
        oldBlock->truncateTo(uso);
        oldBlock->clearLastBeginTxnOffset();
    }

    pushPendingBlocks();
}
