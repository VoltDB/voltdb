/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#ifndef VOLTDB_LARGETEMPTABLEBLOCK_HPP
#define VOLTDB_LARGETEMPTABLEBLOCK_HPP

#include <utility>

#include "storage/TupleBlock.h"

namespace voltdb {

class LargeTempTable;

/**
 */
class LargeTempTableBlock {
 public:

    static const size_t BLOCK_SIZE_IN_BYTES = 8 * 1024 * 1024; // 8 MB

    /** constructor for a new block. */
    LargeTempTableBlock(int64_t id, LargeTempTable* ltt);

    int64_t id() const {
        return m_id;
    }

    bool insertTuple(const TableTuple& source);

    void* allocate(std::size_t size);

    uint32_t unusedTupleBoundary() {
        return m_activeTupleCount;
    }

    char* address() {
        return m_storage.get();
    }

    int64_t getAllocatedMemory() const;
    int64_t getAllocatedTupleMemory() const;
    int64_t getAllocatedPoolMemory() const;

    std::unique_ptr<char[]> releaseData();

    void setData(std::unique_ptr<char[]> storage);

    virtual ~LargeTempTableBlock();

    bool isPinned() const {
        return m_isPinned;
    }

    void pin() {
        assert(!m_isPinned);
        m_isPinned = true;
    }

    void unpin() {
        assert(m_isPinned);
        m_isPinned = false;
    }

    bool isResident() const {
        return m_storage.get() != NULL;
    }

    int64_t activeTupleCount() const {
        return m_activeTupleCount;
    }

 private:

    int64_t m_id;
    std::unique_ptr<char[]> m_storage;
    char* m_tupleInsertionPoint;
    char* m_stringInsertionPoint;
    bool m_isPinned;
    int64_t m_activeTupleCount;
};

} // end namespace voltdb

#endif // VOLTDB_LARGETEMPTABLEBLOCK_HPP
