/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
#include "common/types.h"

#include <common/debuglog.h>
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
              m_type(NORMAL_STREAM_BLOCK),
              m_lastCommittedSpHandle(std::numeric_limits<int64_t>::min()),
              m_lastSpUniqueId(0)
        {}

        StreamBlock(StreamBlock *other)
            : m_data(other->m_data), m_capacity(other->m_capacity),
              m_headerSize(other->m_headerSize), m_offset(other->m_offset),
              m_uso(other->m_uso),
              m_type(other->m_type),
              m_lastCommittedSpHandle(std::numeric_limits<int64_t>::min()),
              m_lastSpUniqueId(other->m_lastSpUniqueId)
        {}

        ~StreamBlock()
        {}

        /**
         * Returns a pointer to the underlying raw memory allocation
         */
        inline char* rawPtr() {
            return m_data - m_headerSize;
        }

        inline int32_t rawLength() const {
            return static_cast<int32_t>(m_offset + m_headerSize);
        }

        /**
         * Returns the universal stream offset of the block not
         * including any of the octets in this block.
         */
        inline size_t uso() const {
            return m_uso;
        }

        /**
         * Returns the additional offset from uso() to count all the
         * octets in this block.  uso() + offset() will compute the
         * universal stream offset for the entire block. This excludes
         * the length prefix.
         */
        inline size_t offset() const {
            return m_offset;
        }

        /**
         * Number of bytes left in the buffer
         */
        inline size_t remaining() const {
            return m_capacity - m_offset;
        }

        inline size_t headerSize() const {
            return m_headerSize;
        }

        /**
         * Number of maximum bytes stored in the buffer
         */
        inline size_t capacity() const {
            return m_capacity;
        }

        inline int64_t lastCommittedSpHandle() {
            return m_lastCommittedSpHandle;
        }

        inline void recordLastCommittedSpHandle(int64_t spHandle) {
            m_lastCommittedSpHandle = spHandle;
        }

        inline void recordCompletedSpTxn(int64_t lastSpUniqueId) {
            m_lastSpUniqueId = lastSpUniqueId;
        }

        inline int64_t lastSpUniqueId() const {
            return m_lastSpUniqueId;
        }

        inline StreamBlockType type() const {
            return m_type;
        }

        inline char* mutableDataPtr() {
            return m_data + m_offset;
        }

        inline void commonConsumed(size_t consumed) {
            vassert((m_offset + consumed) <= m_capacity);
            m_offset += consumed;
        }

        inline void commonTruncateTo(size_t mark) {
            // just move offset. pretty easy.
            if (((m_uso + offset()) >= mark ) && (m_uso <= mark)) {
                m_offset = mark - m_uso;
            }
            else {
                throwFatalException("Attempted Export block truncation past start of block."
                                    "\n m_uso(%jd), m_offset(%jd), mark(%jd)\n",
                                    (intmax_t)m_uso, (intmax_t)m_offset, (intmax_t)mark);
            }
        }

        inline void setType(StreamBlockType type) { m_type = type; }

    protected:
        char *m_data;
        const size_t m_capacity;
        const size_t m_headerSize;
        size_t m_offset;         // position for next write.
        const size_t m_uso;            // universal stream offset of m_offset 0.

        StreamBlockType m_type;

        int64_t m_lastCommittedSpHandle; // for record last CommittedSpHandle for Sp Txn in this block
        int64_t m_lastSpUniqueId;
    };


    class ExportStreamBlock : public StreamBlock {
    public:
        ExportStreamBlock(char* data, size_t headerSize, size_t capacity, size_t uso) :
            StreamBlock(data, headerSize, capacity, uso),
            m_rowCount(0),
            m_startSequenceNumber(0),
            m_committedSequenceNumber(-1L)
        {}

        ExportStreamBlock(ExportStreamBlock* other) :
            StreamBlock(other),
            m_rowCount(other->m_rowCount),
            m_startSequenceNumber(other->m_startSequenceNumber),
            m_committedSequenceNumber(other->m_committedSequenceNumber)
        {}

        ~ExportStreamBlock()
        {}

        inline void recordStartSequenceNumber(int64_t startSequenceNumber) {
            m_startSequenceNumber = startSequenceNumber;
        }

        inline const size_t getRowCount() const {
            return m_rowCount;
        }

        inline int64_t startSequenceNumber() const {
            return m_startSequenceNumber;
        }

        inline int64_t lastSequenceNumber() const {
            return m_startSequenceNumber + (int64_t)getRowCount() - 1;
        }

        inline int64_t getCommittedSequenceNumber() const {
            return m_committedSequenceNumber;
        }

        inline void setCommittedSequenceNumber(int64_t committedSequenceNumber) {
            m_committedSequenceNumber = committedSequenceNumber;
        }

        inline char* headerDataPtr() {
            return m_data - (m_headerSize - MAGIC_HEADER_SPACE_FOR_JAVA);
        }

        inline void consumed(size_t consumed) {
            commonConsumed(consumed);
            m_rowCount++;
        }

        inline void truncateExportTo(size_t mark, int64_t seqNo) {
            commonTruncateTo(mark);
            m_rowCount = seqNo - m_startSequenceNumber;
        }

    private:
        size_t m_rowCount;
        int64_t m_startSequenceNumber;
        int64_t m_committedSequenceNumber;
    };


    class DrStreamBlock : public StreamBlock {
    public:
        DrStreamBlock(char* data, size_t headerSize, size_t capacity, size_t uso) :
            StreamBlock(data, headerSize, capacity, uso),
            m_lastDRBeginTxnOffset(0),
            m_rowCountForDR(0),
            m_drEventType(voltdb::NOT_A_EVENT),
            m_hasDRBeginTxn(false),
            m_startDRSequenceNumber(std::numeric_limits<int64_t>::max()),
            m_lastDRSequenceNumber(std::numeric_limits<int64_t>::max()),
            m_lastMpUniqueId(0)
        {}

        DrStreamBlock(DrStreamBlock *other) :
            StreamBlock(other),
            m_lastDRBeginTxnOffset(other->m_lastDRBeginTxnOffset),
            m_rowCountForDR(other->m_rowCountForDR),
            m_drEventType(other->m_drEventType),
            m_hasDRBeginTxn(other->m_hasDRBeginTxn),
            m_startDRSequenceNumber(other->m_startDRSequenceNumber),
            m_lastDRSequenceNumber(other->m_lastDRSequenceNumber),
            m_lastMpUniqueId(other->m_lastMpUniqueId)
        {}

        ~DrStreamBlock()
        {}

        inline void markAsEventBuffer(DREventType type) {
            m_drEventType = type;
        }

        inline DREventType drEventType() {
            return m_drEventType;
        }

        inline size_t updateRowCountForDR(size_t rowsToCommit) {
            m_rowCountForDR += rowsToCommit;
            return m_rowCountForDR;
        }

        inline void recordCompletedMpTxnForDR(int64_t lastMpUniqueId) {
            m_lastMpUniqueId = lastMpUniqueId;
        }

        inline int64_t lastMpUniqueId() const {
            return m_lastMpUniqueId;
        }

        inline void recordCompletedSequenceNumForDR(int64_t lastDRSequenceNumber) {
            m_lastDRSequenceNumber = lastDRSequenceNumber;
        }

        inline int64_t lastDRSequenceNumber() const {
            return m_lastDRSequenceNumber;
        }

        inline void startDRSequenceNumber(int64_t startDRSequenceNumber) {
            m_startDRSequenceNumber = std::min(startDRSequenceNumber, m_startDRSequenceNumber);
        }

        inline int64_t startDRSequenceNumber() const {
            return m_startDRSequenceNumber;
        }

        inline size_t lastDRBeginTxnOffset() const {
            return m_lastDRBeginTxnOffset;
        }

        inline void recordLastBeginTxnOffset() {
            m_lastDRBeginTxnOffset = m_offset;
            m_hasDRBeginTxn = true;
        }

        inline void clearLastBeginTxnOffset() {
            m_lastDRBeginTxnOffset = 0;
            m_hasDRBeginTxn =false;
        }

        inline bool hasDRBeginTxn() {
            return m_hasDRBeginTxn;
        }

        inline char* mutableLastBeginTxnDataPtr() {
            return m_data + m_lastDRBeginTxnOffset;
        }

        inline void consumed(size_t consumed) {
            commonConsumed(consumed);
        }

        inline void truncateTo(size_t mark) {
            commonTruncateTo(mark);
            recordLastBeginTxnOffset();
        }

    private:
        size_t m_lastDRBeginTxnOffset;  // keep record of DR begin txn to avoid txn span multiple buffers
        size_t m_rowCountForDR;
        DREventType m_drEventType;
        bool m_hasDRBeginTxn;    // only used for DR Buffer
        int64_t m_startDRSequenceNumber;
        int64_t m_lastDRSequenceNumber;
        int64_t m_lastMpUniqueId;
    };
}

#endif
