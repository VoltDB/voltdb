/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#include "common/LargeTempTableBlockCache.h"
#include "storage/LargeTableIterator.h"
#include "storage/LargeTempTable.h"
#include "storage/LargeTempTableBlock.h"

namespace voltdb {

    // Copied from temptable.h
    static const int BLOCKSIZE = 131072;

    LargeTempTable::LargeTempTable()
        : Table(BLOCKSIZE)
        , m_iter(this)
        , m_blockForWriting(nullptr)
        , m_blockIds()
    {
    }

bool LargeTempTable::insertTuple(TableTuple& source) {
    TableTuple target(m_schema);

    if (m_blockForWriting == nullptr || !m_blockForWriting->hasFreeTuples()) {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

        if (m_blockForWriting != nullptr) {
            int64_t lastBlockId = m_blockIds.back();
            lttBlockCache->unpinBlock(lastBlockId);
        }

        int64_t nextBlockId;
        std::tie(nextBlockId, m_blockForWriting) = lttBlockCache->getEmptyBlock(this);
        m_blockIds.push_back(nextBlockId);
    }

    char* data;
    std::tie(data, std::ignore) = m_blockForWriting->nextFreeTuple();
    target.move(data);
    target.copyForPersistentInsert(source, m_blockForWriting->getPool()); // tuple in freelist must be already cleared
    target.setActiveTrue();

    return true;
}

LargeTableIterator LargeTempTable::largeIterator() const {
    return LargeTableIterator(schema(), &m_blockIds);
}

} // namespace voltdb
