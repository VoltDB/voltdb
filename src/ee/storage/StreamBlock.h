/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#ifndef STREAMBLOCK_H_
#define STREAMBLOCK_H_

#include "common/FatalException.hpp"

#include <cassert>
#include <cstring>
#include <limits>
#include <stdint.h>

namespace voltdb
{
    /**
     * A single data block with some buffer semantics.
     */
    class StreamBlock
    {
    public:
        StreamBlock(char* data, size_t capacity, size_t uso)
            : m_data(data), m_capacity(capacity), m_offset(0),
            m_uso(uso), m_generationId(std::numeric_limits<int64_t>::min()),
            m_signature(""), m_endOfStream(false)
        {
        }

        StreamBlock(StreamBlock *other)
            : m_data(other->m_data), m_capacity(other->m_capacity),
              m_offset(other->m_offset), m_uso(other->m_uso),
            m_generationId(other->m_generationId),
            m_signature(other->m_signature),
            m_endOfStream(other->m_endOfStream)
        {
        }

        ~StreamBlock()
        {
        }

        /**
         * Returns a pointer to the underlying raw memory allocation
         */
        char* rawPtr() {
            return m_data;
        }

        int32_t rawLength() const {
            return  static_cast<int32_t>(m_offset);
        }

        /**
         * Returns the universal stream offset of the block not
         * including any of the octets in this block.
         */
        const size_t uso() const {
            return m_uso;
        }

        /**
         * Returns the additional offset from uso() to count all the
         * octets in this block.  uso() + offset() will compute the
         * universal stream offset for the entire block. This excludes
         * the length prefix.
         */
        const size_t offset() const {
            return m_offset;
        }

        /**
         * Number of bytes left in the buffer
         */
        const size_t remaining() const {
            return m_capacity - m_offset;
        }

        /**
         * Generation ID of the buffer
         */
        const int64_t generationId() const
        {
            return m_generationId;
        }

        void setGenerationId(int64_t generationId)
        {
            m_generationId = generationId;
        }

        const std::string signature() const
        {
            return m_signature;
        }

        void setSignature(std::string signature)
        {
            m_signature = signature;
        }

        const bool endOfStream() const
        {
            return m_endOfStream;
        }

        void setEndOfStream(bool endOfStream)
        {
            m_endOfStream = endOfStream;
        }

    private:
        char* mutableDataPtr() {
            return m_data + m_offset;
        }

        void consumed(size_t consumed) {
            m_offset += consumed;
            assert (m_offset < m_capacity);
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
        }

        char *m_data;
        const size_t m_capacity;
        size_t m_offset;         // position for next write.
        size_t m_uso;            // universal stream offset of m_offset 0.
        int64_t m_generationId;  // The generation ID to which this
                                 // block belongs
        std::string m_signature; // the signature of the export stream
                                 // when this block was created
        bool m_endOfStream;      // Does this block mark the end of a
                                 // generation?

        friend class TupleStreamWrapper;
    };
}

#endif
