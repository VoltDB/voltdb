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

#include "common/Pool.hpp"

#include "storage/TupleBlock.h"

namespace voltdb {

class LargeTempTable;

/**
 * A LargeTempTableBlock contains a normal tuple block and also a
 * separate pool which contains all the variable-length data in the
 * block.  If we need to store this block to make room for other data,
 * the pool gets stored with the fixed-size data.
 *
 * Most methods just forward to the underylying tuple block.
 *
 * Block size is 128k
 * Pool chunk size is 32k
 */
class LargeTempTableBlock {
 public:
    /** constructor for a new block. */
    LargeTempTableBlock(int64_t id, LargeTempTable* ltt);

    int64_t id() const {
        return m_id;
    }


    bool hasFreeTuples() const;

    void insertTuple(const TableTuple& source);

    TBPtr getTupleBlockPointer() {
        return m_tupleBlockPointer;
    }

    uint32_t unusedTupleBoundary() {
        return m_tupleBlockPointer->unusedTupleBoundary();
    }

    char* address() {
        return m_tupleBlockPointer->address();
    }

    int64_t getAllocatedMemory() const;
    int64_t getAllocatedTupleMemory() const;
    int64_t getAllocatedPoolMemory() const;

    std::pair<TBPtr, std::unique_ptr<Pool>> releaseData();

    void setData(TBPtr block, std::unique_ptr<Pool> pool);

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
        if (m_tupleBlockPointer.get() == NULL) {
            assert(m_pool.get() == NULL);
            return false;
        }
        else {
            assert(m_pool.get() != NULL);
            return true;
        }
    }

    int64_t activeTupleCount() const {
        return m_tupleBlockPointer->activeTuples();
    }

 private:

    int64_t m_id;
    std::unique_ptr<Pool> m_pool;
    TBPtr m_tupleBlockPointer;
    bool m_isPinned;
};

} // end namespace voltdb

#endif // VOLTDB_LARGETEMPTABLEBLOCK_HPP
