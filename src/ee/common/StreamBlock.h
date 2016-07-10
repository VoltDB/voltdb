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
#ifndef STREAMBLOCK_H_
#define STREAMBLOCK_H_

#include "common/FatalException.hpp"

#include <cassert>
#include <cstring>
#include <stdint.h>
#include <limits>

#define MAGIC_HEADER_SPACE_FOR_JAVA 8
namespace voltdb
{
    enum StreamBlockType {
        NORMAL_STREAM_BLOCK = 1,
        LARGE_STREAM_BLOCK = 2,
    };
    /**
     * A single data block with some buffer semantics.
     */
    class StreamBlock {
    public:
        StreamBlock(char* data, size_t headerSize, size_t capacity, size_t uso)
            : m_data(data + headerSize), m_capacity(capacity - headerSize),
              m_headerSize(headerSize), m_offset(0),
              m_uso(uso),
              m_startSpHandle(std::numeric_limits<int64_t>::max()),
              m_lastSpHandle(std::numeric_limits<int64_t>::max()),
              m_lastCommittedSpHandle(std::numeric_limits<int64_t>::max()),
              m_lastDRBeginTxnOffset(0),
              m_hasDRBeginTxn(false),
              m_rowCountForDR(0),
              m_startDRSequenceNumber(std::numeric_limits<int64_t>::max()),
              m_lastDRSequenceNumber(std::numeric_limits<int64_t>::max()),
              m_lastSpUniqueId(0),
              m_lastMpUniqueId(0),
              m_type(NORMAL_STREAM_BLOCK),
              m_drEventType(voltdb::NOT_A_EVENT)
        {
        }

        StreamBlock(StreamBlock *other)
            : m_data(other->m_data), m_capacity(other->m_capacity),
              m_headerSize(other->m_headerSize), m_offset(other->m_offset),
              m_uso(other->m_uso),
              m_startSpHandle(std::numeric_limits<int64_t>::max()),
              m_lastSpHandle(std::numeric_limits<int64_t>::max()),
              m_lastCommittedSpHandle(std::numeric_limits<int64_t>::max()),
              m_lastDRBeginTxnOffset(other->m_lastDRBeginTxnOffset),
              m_hasDRBeginTxn(other->m_hasDRBeginTxn),
              m_rowCountForDR(other->m_rowCountForDR),
              m_startDRSequenceNumber(other->m_startDRSequenceNumber),
              m_lastDRSequenceNumber(other->m_lastDRSequenceNumber),
              m_lastSpUniqueId(other->m_lastSpUniqueId),
              m_lastMpUniqueId(other->m_lastMpUniqueId),
              m_type(other->m_type),
              m_drEventType(other->m_drEventType)
        {
        }

        ~StreamBlock()
        {
        }

        /**
         * Returns a pointer to the underlying raw memory allocation
         */
        char* rawPtr() {
            return m_data - m_headerSize;
        }

        int32_t rawLength() const {
            return static_cast<int32_t>(m_offset + m_headerSize);
        }

        /**
         * Returns the universal stream offset of the block not
         * including any of the octets in this block.
         */
        size_t uso() const {
            return m_uso;
        }

        /**
         * Returns the additional offset from uso() to count all the
         * octets in this block.  uso() + offset() will compute the
         * universal stream offset for the entire block. This excludes
         * the length prefix.
         */
        size_t offset() const {
            return m_offset;
        }

        /**
         * Number of bytes left in the buffer
         */
        size_t remaining() const {
            return m_capacity - m_offset;
        }

        size_t headerSize() const {
            return m_headerSize;
        }

        /**
         * Number of maximum bytes stored in the buffer
         */
        size_t capacity() const {
            return m_capacity;
        }

        size_t lastDRBeginTxnOffset() const {
            return m_lastDRBeginTxnOffset;
        }

        int64_t startDRSequenceNumber() const {
            return m_startDRSequenceNumber;
        }

        void startDRSequenceNumber(int64_t startDRSequenceNumber) {
            m_startDRSequenceNumber = std::min(startDRSequenceNumber, m_startDRSequenceNumber);
        }

        int64_t lastDRSequenceNumber() const {
            return m_lastDRSequenceNumber;
        }

        int64_t lastSpUniqueId() const {
            return m_lastSpUniqueId;
        }

        int64_t lastMpUniqueId() const {
            return m_lastMpUniqueId;
        }

        void recordCompletedSequenceNumForDR(int64_t lastDRSequenceNumber) {
            m_lastDRSequenceNumber = lastDRSequenceNumber;
        }

        void recordCompletedSpTxnForDR(int64_t lastSpUniqueId) {
            m_lastSpUniqueId = lastSpUniqueId;
        }

        void recordCompletedMpTxnForDR(int64_t lastMpUniqueId) {
            m_lastMpUniqueId = lastMpUniqueId;
        }

        void markAsEventBuffer(DREventType type) {
            m_drEventType = type;
        }

        DREventType drEventType() {
            return m_drEventType;
        }

        size_t updateRowCountForDR(size_t rowsToCommit) {
            m_rowCountForDR += rowsToCommit;
            return m_rowCountForDR;
        }

        StreamBlockType type() const {
            return m_type;
        }

    private:
        char* mutableDataPtr() {
            return m_data + m_offset;
        }

        void consumed(size_t consumed) {
            assert ((m_offset + consumed) <= m_capacity);
            m_offset += consumed;
        }

        void truncateTo(size_t mark) {
            // just move offset. pretty easy.
            if (((m_uso + offset()) >= mark ) && (m_uso <= mark)) {
                m_offset = mark - m_uso;
            }
            else {
                throwFatalException("Attempted Export block truncation past start of block."
                                    "\n m_uso(%jd), m_offset(%jd), mark(%jd)\n",
                                    (intmax_t)m_uso, (intmax_t)m_offset, (intmax_t)mark);
            }

            recordLastBeginTxnOffset();
        }

        void recordLastBeginTxnOffset() {
            m_lastDRBeginTxnOffset = m_offset;
            m_hasDRBeginTxn = true;
        }

        void clearLastBeginTxnOffset() {
            m_lastDRBeginTxnOffset = 0;
            m_hasDRBeginTxn =false;
        }

        bool hasDRBeginTxn() {
            return m_hasDRBeginTxn;
        }

        char* mutableLastBeginTxnDataPtr() {
            return m_data + m_lastDRBeginTxnOffset;
        }

        void setType(StreamBlockType type) { m_type = type; }

        char *m_data;
        const size_t m_capacity;
        const size_t m_headerSize;
        size_t m_offset;         // position for next write.
        size_t m_uso;            // universal stream offset of m_offset 0.
        int64_t m_startSpHandle;
        int64_t m_lastSpHandle;
        int64_t m_lastCommittedSpHandle;
        size_t m_lastDRBeginTxnOffset;  // keep record of DR begin txn to avoid txn span multiple buffers
        bool m_hasDRBeginTxn;    // only used for DR Buffer
        size_t m_rowCountForDR;
        int64_t m_startDRSequenceNumber;
        int64_t m_lastDRSequenceNumber;
        int64_t m_lastSpUniqueId;
        int64_t m_lastMpUniqueId;
        StreamBlockType m_type;
        DREventType m_drEventType;

        friend class TupleStreamBase;
        friend class ExportTupleStream;
        friend class AbstractDRTupleStream;
        friend class DRTupleStream;
        friend class CompatibleDRTupleStream;
    };
}

#endif
