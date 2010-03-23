/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TUPLESTREAMWRAPPER_H_
#define TUPLESTREAMWRAPPER_H_

#include "common/ids.h"
#include "common/tabletuple.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"

#include <deque>

namespace voltdb {

class Topend;
const int EL_BUFFER_SIZE = /* 1024; */ 2 * 1024 * 1024;

class TupleStreamWrapper {
public:
    enum Type { INSERT, DELETE };

    TupleStreamWrapper(CatalogId partitionId, CatalogId siteId, CatalogId tableId,
                       Topend *topend, int64_t createTime);

    ~TupleStreamWrapper() {
        delete[] m_cachedBlockHeader;
    }

    /**
     * Drop and release all claimed buffers. Intended for use at
     * shutdown to achieve full memory deallocation for valgrind.
     */
    void cleanupManagedBuffers(Topend *);

    /**
     * Configure the buffer size requested from JNI pool.
     * This allows testcases to use significantly smaller buffers
     * to test buffer rollover.
     */
    void setDefaultCapacity(size_t capacity) {
        assert (capacity > 0);
        m_defaultCapacity = capacity;
    }

    /** Read the total bytes used over the life of the stream */
    size_t bytesUsed() {
        return m_uso;
    }

    /** Send committed data to the top end */
    void commit(int64_t txnId);

    /** truncate stream back to mark */
    void rollbackTo(size_t mark);

    /** commit() as much data as possible */
    void flushOldTuples(int64_t lastCommittedTxn,
                         int64_t currentTime);

    /** write a tuple to the stream */
    size_t appendTuple(int64_t txnId,
                       int64_t seqNo,
                       int64_t timestamp,
                       TableTuple &tuple,
                       TupleStreamWrapper::Type type);

    /** Oldest timestamp possible for data in this stream */
    int64_t lastFlushTime() {
        return m_lastFlush;
    }

    /** Create and store the VBINARY block header */
    void cacheBlockHeader(TupleSchema &schema);

private:
    /**
     * A single data block with some buffer semantics.
     */
    class StreamBlock {
      public:
        StreamBlock(char* data, size_t capacity, size_t uso)
            : m_data(data), m_capacity(capacity), m_offset(0), m_uso(uso)
        {
        }

        ~StreamBlock()
        {
        }

        char * dataPtr() {
            return m_data;
        }

        size_t remaining() {
            return m_offset;
        }

        void consumed(size_t consumed) {
            m_offset += consumed;
            assert (m_offset < m_capacity);
        }

        size_t uso() {
            return m_uso;
        }

        void truncateTo(size_t mark) {
            // just move offset. pretty easy.
            if (((m_uso + m_offset) >= mark ) && (m_uso <= mark)) {
                m_offset = mark - m_uso;
            }
            else {
                throwFatalException( "Attempted ELT block truncation past start of block."
                        "\n m_uso(%jd), m_offset(%jd), mark(%jd)\n",
                        (intmax_t)m_uso, (intmax_t)m_offset, (intmax_t)mark);
            }
        }

      private:
        char *m_data;
        const size_t m_capacity;
        size_t m_offset;  // position for next write.
        size_t m_uso;     // universal stream offset of m_offset 0.
    };

    void writeBlockHeader(StreamBlock &block);
    size_t computeOffsets(TableTuple &tuple,size_t *rowHeaderSz);
    void extendBufferChain(size_t minLength);
    void discardBlock(StreamBlock *sb);

    // cached catalog values
    const CatalogId m_partitionId;
    const CatalogId m_siteId;
    const CatalogId m_tableId;

    /** Reference to Topend (the JNI/IPC interface abstraction) */
    Topend *m_topend;

    /** timestamp of most recent flush() */
    int64_t m_lastFlush;

    /** size of buffer requested from the top-end */
    size_t m_defaultCapacity;

    /** Universal stream offset. Total bytes appended to this stream. */
    size_t m_uso;

    /** transaction id of the current (possibly uncommitted) transaction */
    int64_t m_openTransactionId;

    /** Universal stream offset when current transaction was opened */
    size_t m_openTransactionUso;

    /** Current block */
    StreamBlock *m_currBlock;

    /** Blocks not yet issued to the top-end. Does not contain m_currBlock */
    std::deque<StreamBlock*> m_pendingBlocks;

    /** Cached block header.
        12 byte signature
        4 byte header length
        2 byte version
        1 byte sync marker length (0)
        2 byte column count
        4 byte column width (per column) */
    char *m_cachedBlockHeader;

    /** number of bytes in the cached header */
    size_t m_cachedBlockHeaderSize;

    /** total tuples in current block */
    int32_t m_currBlockTupleCount;
};

}

#endif
