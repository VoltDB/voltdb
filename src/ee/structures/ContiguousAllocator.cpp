/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "ContiguousAllocator.h"
#include "common/debuglog.h"
#include "common/ThreadLocalPool.h"

using namespace voltdb;

ContiguousAllocator::ContiguousAllocator(int32_t allocSize, int32_t chunkSize)
    : m_count(0),
      m_allocationSize(allocSize),
      m_numberAllocationsPerBlock(chunkSize),
      m_tail(NULL),
      m_blockCount(0),
      m_cachedBuffer(0) {}

ContiguousAllocator::~ContiguousAllocator() {
    while (m_tail) {
        Buffer *buf = m_tail->prev;
        free(m_tail);
        m_tail = buf;
    }
    if (m_cachedBuffer != NULL) {
        free(m_cachedBuffer);
    }
}

void *ContiguousAllocator::alloc() {
    m_count++;

    // determine where in the current block the new alloc will go
    int64_t blockOffset = (m_count - 1) % m_numberAllocationsPerBlock;

    // if a new block is needed...
    if (blockOffset == 0) {
        void *memory;
        if (m_cachedBuffer != NULL) {
            memory = static_cast<void *>(m_cachedBuffer);
            m_cachedBuffer = NULL;
        } else {
            memory = static_cast<void *>(malloc(sizeof(Buffer) + m_allocationSize * m_numberAllocationsPerBlock));
        }

        Buffer *buf = reinterpret_cast<Buffer*>(memory);

        // for debugging
        //memset(buf, 0, sizeof(sizeof(Buffer) + m_allocSize * m_chunkSize));

        buf->prev = m_tail;
        m_tail = buf;
        m_blockCount++;
    }

    // get a pointer to where the new alloc will live
    void *retval = m_tail->data + (m_allocationSize * blockOffset);
    vassert(retval == last());
    return retval;
}

void *ContiguousAllocator::last() const {
    vassert(m_count > 0);
    vassert(m_tail != NULL);

    // determine where in the current block the last alloc is
    int64_t blockOffset = (m_count - 1) % m_numberAllocationsPerBlock;
    return m_tail->data + (m_allocationSize * blockOffset);
}

void ContiguousAllocator::trim() {
    // for debugging
    //memset(last(), 0, allocSize);
    vassert(m_count > 0);
    vassert(m_tail != NULL);

    m_count--;

    // determine where in the current block the last alloc is
    int64_t blockOffset = m_count % m_numberAllocationsPerBlock;

    // yay! kill a block
    if (blockOffset == 0) {
        Buffer *buf = m_tail->prev;
        m_blockCount--;
        if (m_blockCount == 0) {
            m_cachedBuffer = m_tail;
        } else {
            free(m_tail);
        }
        m_tail = buf;
    }
}

size_t ContiguousAllocator::bytesAllocated() const {
    size_t total = static_cast<size_t>(m_blockCount) *
        static_cast<size_t>(m_allocationSize) *
        static_cast<size_t>(m_numberAllocationsPerBlock);
    return total;
}
