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

#ifndef CONTIGUOUSALLOCATOR_H_
#define CONTIGUOUSALLOCATOR_H_

#include <cstdlib>

namespace voltdb {

/**
 * Dead simple buffer chain that allocates fixed size buffers and
 * fixed size indivual allocations to consumers within those
 * buffers.
 *
 * Note, there are few checks here when running in release mode.
 */
class ContiguousAllocator {
    struct Buffer {
        Buffer *prev;
        char data[0];
    };

    int64_t m_count;
    int32_t m_allocSize;
    int32_t m_chunkSize;
    Buffer *m_tail;
    int32_t m_blockCount;

public:
    /**
     * @param allocSize is the size in bytes of individual allocations.
     * @param chunkSize is the number of allocations per buffer (not bytes).
     */
    ContiguousAllocator(int32_t allocSize, int32_t chunkSize);
    ~ContiguousAllocator();

    void *alloc();
    void *last() const;
    void trim();
    int64_t count() const { return m_count; }

    size_t bytesAllocated() const;
};

} // namespace voltdb

#endif // CONTIGUOUSALLOCATOR_H_
